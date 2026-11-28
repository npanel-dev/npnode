package limiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// 实时准入（admission_v1，A1～A7，对接文档 §7、标准 §6）。
//
// 会话表按 (uid, ip) 放在进程级、跨所有入站：同一服务器的不同入站在面板上共用一个计数成员（按 IP 口径和严格模式都是），
// 所以同一 (uid, ip) 在本进程只做一次 check、一路保活，只有跨所有入站的最后一条连接关闭时才 release。
// 链路计数与 ConnTable 用同一个开、关钩子（Limiter.OpenConn 返回的释放函数）。
//
// 面板把名额的 IPv6 成员归一到 /64（devseat.NormalizeIP），check 响应里不返回归一后的成员，所以节点对 IPv6 按 /64 归并：
// 同一 uid 同一 /64 内仍有活跃会话时不发 release（否则会删掉兄弟地址仍在用的成员）。IPv4 按完整 IP；
// 面板 seat_conflict_ipv4_prefix 配成 <32 时节点无从得知，存在同样的残余风险。
//
// 只对 device_limit > 0 的条目准入。会话表、释放队列、拒绝缓存、并发检查数都有上限；后台循环随 Start / Stop 启停。

// SessionChecker 面板准入接口（panel.SessionClient 实现）
type SessionChecker interface {
	Check(ctx context.Context, req panel.SessionRequest) (*panel.SessionResult, error)
	Release(ctx context.Context, req panel.SessionRequest) error
}

// AdmissionConfig 准入的外部依赖，重载时可以整体替换
type AdmissionConfig struct {
	Checker SessionChecker
	// ServerID 本服务器 ID，用于生成 connection_id
	ServerID int64
	// Closer 可选：保活被拒时额外调用（关闭当前内核上该 (uid, ip) 的连接）。
	// 链路登记时提供的关闭函数总会被调用，重载前建立、仍在旧 dispatcher 上的链路也靠它关闭
	Closer func(uid int, ip string)
}

const (
	// DefaultAdmissionMaxSessions 会话表上限：超过时新的 (uid, ip) 直接拒绝并告警
	DefaultAdmissionMaxSessions = 1 << 18
	// DefaultReleaseQueueSize 释放队列上限：满了丢弃最旧的（§7.6）
	DefaultReleaseQueueSize = 4096
	// DefaultDenyCacheSize 拒绝负缓存上限：满了不再缓存
	DefaultDenyCacheSize = 1 << 16
	// internalUserIDBase 内部账号（中转、测试端）的起点，与面板 noderelay.RelayUserIDBase 相同；面板对它们直接放行
	internalUserIDBase int64 = 9_000_000_000_000
)

// isInternalUser 内部账号：面板对 check 直接放行（compat_session.go IsInternalUserID），节点跳过准入
func isInternalUser(uid int) bool {
	return int64(uid) >= internalUserIDBase
}

type admKey struct {
	uid int
	ip  string
}

// Admission 进程级准入会话表
type Admission struct {
	mu       sync.Mutex
	cfg      AdmissionConfig
	users    map[int]map[string]*admEntry
	sessions int
	degraded int
	linkSeq  uint64
	// denied 面板拒绝（allowed=false）的负缓存，到期时间
	denied  map[admKey]time.Time
	sf      singleflight.Group
	queue   *releaseQueue
	recover chan struct{}

	runMu   sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	logMu   sync.Mutex
	lastLog map[string]time.Time

	// 以下参数在 Start 前可调整（测试用），之后只读
	Timeout          time.Duration   // 单次请求超时（A5：≤2 秒）
	KeepaliveMin     time.Duration   // 保活间隔下限
	KeepaliveMax     time.Duration   // 保活间隔上限（≤60 秒，§7.3）
	ScanInterval     time.Duration   // 保活扫描周期
	ProbeInterval    time.Duration   // 降级期间用一个 degraded 会话试探面板的周期
	OrphanAfter      time.Duration   // 放行后迟迟没有链路登记的会话，超过这么久按已关闭处理
	ReleaseGrace     time.Duration   // 链路归零后等这么久再发 release，期间有新连接即撤销（§7.6）
	ReleaseBackoff   []time.Duration // release 失败后的重试退避（重试次数 = 长度）
	StaleReleasing   time.Duration   // 等待释放超过这么久仍未处理的会话直接移出（兜底，面板 120 秒 TTL 过期）
	DenyCacheTTL     time.Duration   // 面板拒绝结果的负缓存时长，0 表示不缓存
	CheckConcurrency int             // 后台保活 / 补发的并发请求数
	ReleaseWorkers   int
	MaxSessions      int
	now              func() time.Time
}

type admEntry struct {
	// links 本进程所有入站上这个 (uid, ip) 的活跃链路数
	links int
	// closers 链路登记时提供的关闭函数（按登记序号），保活被拒、或用户由不限速改为限速时调用
	closers map[uint64]linkCloser
	// sess 准入会话；nil 表示没有（device_limit 为 0 的用户、被拒后等待链路关闭）
	sess *admSession
}

type linkCloser struct {
	taguuid string
	fn      func()
}

type admSession struct {
	connID   string
	protocol string
	port     int
	// degraded 面板不可用时按本机规则放行，面板恢复后要补发 check（A6）
	degraded bool
	// next 下一次保活时间
	next time.Time
	// touched 最近一次放行时间：放行后一直没有链路登记的会话靠它判断为孤儿
	touched time.Time
	// releasing 链路已归零、release 已入队；在发出前又有新连接时清掉，即撤销这次 release
	releasing      bool
	releasingSince time.Time
	// checking 后台保活 / 补发进行中，避免同一会话并发检查
	checking bool
	// releaseAfterCheck release 到期时保活还在途：等保活结果回来后再发（M1）
	releaseAfterCheck bool
}

// NewAdmission 建立准入会话表（后台循环未启动，见 Start）
func NewAdmission(cfg AdmissionConfig) *Admission {
	return &Admission{
		cfg:              cfg,
		users:            make(map[int]map[string]*admEntry),
		denied:           make(map[admKey]time.Time),
		queue:            newReleaseQueue(DefaultReleaseQueueSize),
		recover:          make(chan struct{}, 1),
		lastLog:          make(map[string]time.Time),
		Timeout:          panel.SessionTimeout,
		KeepaliveMin:     50 * time.Second,
		KeepaliveMax:     60 * time.Second,
		ScanInterval:     time.Second,
		ProbeInterval:    10 * time.Second,
		OrphanAfter:      30 * time.Second,
		ReleaseGrace:     3 * time.Second,
		ReleaseBackoff:   []time.Duration{time.Second, 2 * time.Second, 4 * time.Second},
		StaleReleasing:   2 * time.Minute,
		DenyCacheTTL:     5 * time.Second,
		CheckConcurrency: 64,
		ReleaseWorkers:   4,
		MaxSessions:      DefaultAdmissionMaxSessions,
		now:              time.Now,
	}
}

// Configure 替换外部依赖（重载时换成新的面板客户端）；会话表保留
func (a *Admission) Configure(cfg AdmissionConfig) {
	a.mu.Lock()
	a.cfg = cfg
	a.mu.Unlock()
}

func (a *Admission) config() AdmissionConfig {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}

// Start 启动保活、补发、释放循环；重复调用无效果
func (a *Admission) Start() {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if a.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.running = true
	a.wg.Add(1 + a.ReleaseWorkers)
	go a.loop(ctx)
	for i := 0; i < a.ReleaseWorkers; i++ {
		go a.releaseWorker(ctx)
	}
}

// Stop 停止后台循环并等待退出；会话表和释放队列保留，重载后 Start 继续。重复调用无效果
func (a *Admission) Stop() {
	a.runMu.Lock()
	if !a.running {
		a.runMu.Unlock()
		return
	}
	a.running = false
	a.cancel()
	a.runMu.Unlock()
	a.wg.Wait()
}

func admNormalizeIP(ip string) (string, bool) {
	ip = strings.TrimPrefix(ip, "::ffff:")
	return ip, net.ParseIP(ip) != nil
}

// admGroup 面板名额成员的归并口径：IPv4 按完整地址，IPv6 按 /64
func admGroup(ip string) string {
	p := net.ParseIP(ip)
	if p == nil || p.To4() != nil {
		return ip
	}
	return p.Mask(net.CIDRMask(64, 128)).String() + "/64"
}

// managed 是否受准入管理：device_limit > 0、uid 有效且不是内部账号、IP 可解析
func managed(uid int, ip string, deviceLimit int) (string, bool) {
	if uid <= 0 || deviceLimit <= 0 || isInternalUser(uid) {
		return "", false
	}
	return admNormalizeIP(ip)
}

func (a *Admission) entryLocked(uid int, ip string) *admEntry {
	if m := a.users[uid]; m != nil {
		return m[ip]
	}
	return nil
}

func (a *Admission) ensureEntryLocked(uid int, ip string) *admEntry {
	m := a.users[uid]
	if m == nil {
		m = make(map[string]*admEntry)
		a.users[uid] = m
	}
	e := m[ip]
	if e == nil {
		e = &admEntry{}
		m[ip] = e
	}
	return e
}

func (a *Admission) deleteEntryLocked(uid int, ip string) {
	m := a.users[uid]
	if m == nil {
		return
	}
	delete(m, ip)
	if len(m) == 0 {
		delete(a.users, uid)
	}
}

// dropSessionLocked 去掉 (uid, ip) 的会话；没有链路时连条目一起删除
func (a *Admission) dropSessionLocked(uid int, ip string, e *admEntry) {
	if e.sess != nil {
		if e.sess.degraded {
			a.degraded--
		}
		e.sess = nil
		a.sessions--
	}
	if e.links <= 0 {
		a.deleteEntryLocked(uid, ip)
	}
}

func (a *Admission) jitterLocked(now time.Time) time.Time {
	d := a.KeepaliveMin
	if span := a.KeepaliveMax - a.KeepaliveMin; span > 0 {
		d += rand.N(span)
	}
	return now.Add(d)
}

func (a *Admission) newSessionLocked(uid int, ip, protocol string, port int, degraded bool, now time.Time) *admSession {
	e := a.ensureEntryLocked(uid, ip)
	s := &admSession{
		connID:   a.connID(ip, now),
		protocol: protocol,
		port:     port,
		degraded: degraded,
		next:     a.jitterLocked(now),
		touched:  now,
	}
	e.sess = s
	a.sessions++
	if degraded {
		a.degraded++
	}
	return s
}

// connID §7.2：{server_id}:{ip}:{首次连接的毫秒时间戳}，同一 (uid, ip) 的 check、保活、release 共用
func (a *Admission) connID(ip string, now time.Time) string {
	return strconv.FormatInt(a.cfg.ServerID, 10) + ":" + ip + ":" + strconv.FormatInt(now.UnixMilli(), 10)
}

// otherActiveGroupsLocked 这个 uid 在本进程所有入站上、与 ip 不同归并组的活跃设备数（降级计数，§7.5；IPv6 同一 /64 算一台）
func (a *Admission) otherActiveGroupsLocked(uid int, ip string) int {
	mine := admGroup(ip)
	groups := make(map[string]struct{})
	for other, e := range a.users[uid] {
		if !(e.links > 0 || (e.sess != nil && !e.sess.releasing)) {
			continue
		}
		if g := admGroup(other); g != mine {
			groups[g] = struct{}{}
		}
	}
	return len(groups)
}

// groupBusyLocked 同一 uid 同一归并组内还有除 except 之外的有效会话（含同一 IP 上新建的会话）：面板成员仍在使用，不能 release
func (a *Admission) groupBusyLocked(uid int, ip string, except *admSession) bool {
	g := admGroup(ip)
	for other, e := range a.users[uid] {
		if e.sess != nil && e.sess != except && !e.sess.releasing && admGroup(other) == g {
			return true
		}
	}
	return false
}

// deniedLocked 是否在拒绝负缓存期内
func (a *Admission) deniedLocked(k admKey, now time.Time) bool {
	until, ok := a.denied[k]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(a.denied, k)
	return false
}

// cacheDenyLocked 记下面板的拒绝结果（只缓存 allowed=false，不缓存降级 / 出错）
func (a *Admission) cacheDenyLocked(k admKey, now time.Time) {
	if a.DenyCacheTTL <= 0 {
		return
	}
	if _, ok := a.denied[k]; !ok && len(a.denied) >= DefaultDenyCacheSize {
		return
	}
	a.denied[k] = now.Add(a.DenyCacheTTL)
}

// Admit 判定 (uid, ip) 的一条新连接是否放行（A1、A5）。在 dispatch 的同步路径上，首连接最多阻塞约 Timeout。
//   - deviceLimit <= 0、uid 无效、内部账号、IP 无法解析：不准入，直接放行；
//   - 这个 (uid, ip) 已有会话：复用，不再 check（含撤销已入队的 release）；
//   - 面板几秒内刚拒绝过：直接拒绝（负缓存）；
//   - 否则 check（同一 (uid, ip) 并发的首连接用 singleflight 合并成一次）：放行则建会话；拒绝则不建；
//     面板不可用时降级：本机该 uid 其他活跃设备（IPv6 按 /64）已达 deviceLimit 则拒绝，否则放行并标记 degraded。
func (a *Admission) Admit(uid int, ip string, deviceLimit int, protocol string, port int) bool {
	ip, ok := managed(uid, ip, deviceLimit)
	if !ok {
		return true
	}
	if allowed, decided := a.fastPath(uid, ip); decided {
		return allowed
	}
	v, _, _ := a.sf.Do(strconv.Itoa(uid)+"|"+ip, func() (interface{}, error) {
		return a.firstCheck(uid, ip, deviceLimit, protocol, port), nil
	})
	return v.(bool)
}

// fastPath 已有会话时复用（刷新放行时间，撤销尚未发出的 release）；在拒绝负缓存期内直接拒绝
func (a *Admission) fastPath(uid int, ip string) (allowed, decided bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.entryLocked(uid, ip); e != nil && e.sess != nil {
		e.sess.releasing = false
		e.sess.touched = a.now()
		return true, true
	}
	if a.deniedLocked(admKey{uid, ip}, a.now()) {
		return false, true
	}
	return false, false
}

func (a *Admission) firstCheck(uid int, ip string, deviceLimit int, protocol string, port int) bool {
	if allowed, decided := a.fastPath(uid, ip); decided {
		return allowed // singleflight 上一轮刚有结果
	}
	a.mu.Lock()
	if a.sessions >= a.MaxSessions {
		a.mu.Unlock()
		a.logLimited("full", log.WarnLevel, "准入会话表已满，拒绝新的 (id, IP)", log.Fields{"上限": a.MaxSessions, "uid": uid})
		return false
	}
	cfg := a.cfg
	now := a.now()
	connID := a.connID(ip, now)
	a.mu.Unlock()

	req := panel.SessionRequest{Protocol: protocol, Port: port, UserID: int64(uid), ClientIP: ip, ConnectionID: connID}
	res, err := a.check(context.Background(), cfg.Checker, req)

	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.entryLocked(uid, ip); e != nil && e.sess != nil {
		return true
	}
	if err != nil {
		if a.otherActiveGroupsLocked(uid, ip) >= deviceLimit {
			log.WithFields(log.Fields{"uid": uid, "ip": ip, "device_limit": deviceLimit}).Info("面板准入不可用，本机降级：该用户其他 IP 已达设备数，拒绝")
			return false
		}
		s := a.newSessionLocked(uid, ip, protocol, port, true, now)
		s.connID = connID
		log.WithFields(log.Fields{"uid": uid, "ip": ip}).Info("面板准入不可用，本机降级放行，面板恢复后补发 check")
		return true
	}
	a.panelOKLocked()
	if !res.Allowed {
		a.cacheDenyLocked(admKey{uid, ip}, a.now())
		log.WithFields(log.Fields{"uid": uid, "ip": ip, "reason": res.Reason, "current": res.Current, "limit": res.Limit}).Info("准入被拒，关闭连接")
		return false
	}
	s := a.newSessionLocked(uid, ip, protocol, port, false, now)
	s.connID = connID
	return true
}

// Revoked 链路登记（Open）之后调用：这条受准入管理的链路所属 (uid, ip) 已经没有会话（Admit 放行后、登记前保活被拒并关闭了连接），
// 调用方应立即关闭它（M4）。不受准入管理的链路（与 Admit 的跳过条件相同）永远返回 false
func (a *Admission) Revoked(uid int, ip string, deviceLimit int) bool {
	ip, ok := managed(uid, ip, deviceLimit)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.entryLocked(uid, ip)
	return e == nil || e.sess == nil
}

// check 发一次 check；失败（面板不可用）时限频记日志：400 / 403 记 Error，其余记 Warn
func (a *Admission) check(ctx context.Context, checker SessionChecker, req panel.SessionRequest) (*panel.SessionResult, error) {
	if checker == nil {
		return nil, errors.New("准入客户端未配置")
	}
	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()
	res, err := checker.Check(ctx, req)
	if err != nil {
		a.logUnavailable("check", err)
	}
	return res, err
}

func (a *Admission) logUnavailable(op string, err error) {
	var se *panel.SessionError
	if errors.As(err, &se) && se.Misconfigured() {
		a.logLimited(op+"-misconfig", log.ErrorLevel, "准入请求被面板拒绝（参数或密钥错误），按面板不可用降级，请检查配置", log.Fields{"op": op, "err": err})
		return
	}
	a.logLimited(op, log.WarnLevel, "面板准入接口不可用，按本机规则降级", log.Fields{"op": op, "err": err})
}

// logLimited 同一类日志每 10 秒最多一条，避免面板不可用时刷屏
func (a *Admission) logLimited(kind string, level log.Level, msg string, fields log.Fields) {
	a.logMu.Lock()
	now := time.Now()
	if last, ok := a.lastLog[kind]; ok && now.Sub(last) < 10*time.Second {
		a.logMu.Unlock()
		return
	}
	a.lastLog[kind] = now
	a.logMu.Unlock()
	log.WithFields(fields).Log(level, msg)
}

// panelOKLocked 任何一次 check 或 release 成功即视为面板恢复：有 degraded 会话时触发补发（A6）
func (a *Admission) panelOKLocked() {
	if a.degraded > 0 {
		select {
		case a.recover <- struct{}{}:
		default:
		}
	}
}

func (a *Admission) panelOK() {
	a.mu.Lock()
	a.panelOKLocked()
	a.mu.Unlock()
}

// Open 登记 (uid, ip) 的一条链路，返回释放函数（可重复、并发调用，只生效一次）。
// closeLink 可选：关闭这条链路的函数，保活被拒时调用（不依赖链路所在的 dispatcher，重载前的链路也能关掉）。
// 已入队、尚未发出的 release 随之撤销（§7.6）。
func (a *Admission) Open(uid int, ip string, closeLink func()) (release func()) {
	return a.OpenLink(uid, ip, "", closeLink)
}

// OpenLink 同 Open，并记下链路所属的 taguuid（CloseUserLinks 按它挑选）
func (a *Admission) OpenLink(uid int, ip, taguuid string, closeLink func()) (release func()) {
	ip, ok := admNormalizeIP(ip)
	if uid <= 0 || !ok {
		return func() {}
	}
	a.mu.Lock()
	e := a.ensureEntryLocked(uid, ip)
	e.links++
	a.linkSeq++
	id := a.linkSeq
	if closeLink != nil {
		if e.closers == nil {
			e.closers = make(map[uint64]linkCloser)
		}
		e.closers[id] = linkCloser{taguuid: taguuid, fn: closeLink}
	}
	if e.sess != nil {
		e.sess.releasing = false
	}
	a.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { a.close(uid, ip, id) }) }
}

// close 链路关闭：跨所有入站的最后一条关闭时，有会话就（宽限后）release（A4、A7），没有就删除条目
func (a *Admission) close(uid int, ip string, id uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.entryLocked(uid, ip)
	if e == nil || e.links <= 0 {
		return
	}
	delete(e.closers, id)
	e.links--
	if e.links > 0 {
		return
	}
	if e.sess == nil {
		a.deleteEntryLocked(uid, ip)
		return
	}
	a.enqueueReleaseLocked(uid, ip, e.sess, a.ReleaseGrace)
}

func (a *Admission) enqueueReleaseLocked(uid int, ip string, s *admSession, delay time.Duration) {
	now := a.now()
	s.releasing = true
	s.releasingSince = now
	a.pushLocked(releaseItem{uid: uid, ip: ip, sess: s, notBefore: now.Add(delay)})
}

// pushLocked 入队；队列满时丢弃最旧的条目，并把它对应的会话移出会话表（I1：不留下永远等待释放的会话）
func (a *Admission) pushLocked(it releaseItem) {
	dropped, ok := a.queue.push(it)
	if !ok {
		return
	}
	a.logLimited("queue-full", log.WarnLevel, "准入释放队列已满，丢弃最旧的 release（面板 120 秒后自然过期）", log.Fields{"上限": a.queue.cap})
	if dropped.forced {
		return // 会话已不在表里
	}
	if e := a.entryLocked(dropped.uid, dropped.ip); e != nil && e.sess == dropped.sess && dropped.sess.releasing && e.links <= 0 {
		a.dropSessionLocked(dropped.uid, dropped.ip, e)
	}
}

// CloseUserLinks 关闭 uid 在 taguuid（某个入站上的这个用户）下登记过的全部链路，不动会话（U5：原不限速改为限速，
// 这些链路没有限速写端，断开后客户端重连按新限速）。按链路登记的关闭函数执行，重载前旧 dispatcher 上的链路也能关掉
func (a *Admission) CloseUserLinks(uid int, taguuid string) int {
	a.mu.Lock()
	var fns []func()
	for _, e := range a.users[uid] {
		for _, c := range e.closers {
			if c.taguuid == taguuid {
				fns = append(fns, c.fn)
			}
		}
	}
	a.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
	return len(fns)
}

// Forget 用户的 device_limit 改为 0（U5，例如面板关闭了实时准入）：不再保活，已登记的会话立即 release；链路照常保留
func (a *Admission) Forget(uid int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for ip, e := range a.users[uid] {
		s := e.sess
		if s == nil || s.releasing {
			continue
		}
		a.dropSessionLocked(uid, ip, e)
		a.pushLocked(releaseItem{uid: uid, ip: ip, sess: s, forced: true})
	}
}

// Adopt 用户的 device_limit 由 0 改为正数（U5）：给仍有活跃链路、但还没有会话的 (uid, ip) 建会话并立即 check，
// 被拒的关闭对应连接
func (a *Admission) Adopt(uid int, protocol string, port int) {
	if isInternalUser(uid) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for ip, e := range a.users[uid] {
		if e.links <= 0 || e.sess != nil || a.sessions >= a.MaxSessions {
			continue
		}
		s := a.newSessionLocked(uid, ip, protocol, port, false, now)
		s.next = now // 下一次扫描即 check
	}
}

// ===== 后台：保活、补发、试探 =====

type checkJob struct {
	uid  int
	ip   string
	sess *admSession
	req  panel.SessionRequest
}

func (a *Admission) loop(ctx context.Context) {
	defer a.wg.Done()
	scan := time.NewTicker(a.ScanInterval)
	defer scan.Stop()
	probe := time.NewTicker(a.ProbeInterval)
	defer probe.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-scan.C:
			a.runChecks(ctx, a.collectKeepalive())
		case <-probe.C:
			a.runChecks(ctx, a.collectDegraded(1))
		case <-a.recover:
			a.runChecks(ctx, a.collectDegraded(0))
		}
	}
}

func jobFor(uid int, ip string, s *admSession) checkJob {
	s.checking = true
	return checkJob{uid: uid, ip: ip, sess: s, req: panel.SessionRequest{
		Protocol: s.protocol, Port: s.port, UserID: int64(uid), ClientIP: ip, ConnectionID: s.connID,
	}}
}

// collectKeepalive 到期的会话（A3）；顺带处理孤儿会话、长期等待释放的会话，清理过期的拒绝缓存
func (a *Admission) collectKeepalive() []checkJob {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for k, until := range a.denied {
		if !now.Before(until) {
			delete(a.denied, k)
		}
	}
	var jobs []checkJob
	for uid, m := range a.users {
		for ip, e := range m {
			s := e.sess
			if s == nil || s.checking {
				continue
			}
			if s.releasing {
				if e.links <= 0 && now.Sub(s.releasingSince) >= a.StaleReleasing {
					a.logLimited("stale-release", log.WarnLevel, "准入会话等待释放过久，直接移出（面板 120 秒后自然过期）", log.Fields{"uid": uid})
					a.dropSessionLocked(uid, ip, e)
				}
				continue
			}
			if e.links <= 0 && now.Sub(s.touched) >= a.OrphanAfter {
				a.enqueueReleaseLocked(uid, ip, s, 0)
				continue
			}
			if now.Before(s.next) {
				continue
			}
			s.next = a.jitterLocked(now)
			jobs = append(jobs, jobFor(uid, ip, s))
		}
	}
	return jobs
}

// collectDegraded degraded 会话（A6）；limit > 0 时最多取这么多个（试探用）
func (a *Admission) collectDegraded(limit int) []checkJob {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.degraded == 0 {
		return nil
	}
	var jobs []checkJob
	for uid, m := range a.users {
		for ip, e := range m {
			s := e.sess
			if s == nil || !s.degraded || s.releasing || s.checking {
				continue
			}
			jobs = append(jobs, jobFor(uid, ip, s))
			if limit > 0 && len(jobs) >= limit {
				return jobs
			}
		}
	}
	return jobs
}

// runChecks 并发（有上限）发出保活 / 补发 check 并处理结果，全部完成后返回
func (a *Admission) runChecks(ctx context.Context, jobs []checkJob) {
	if len(jobs) == 0 {
		return
	}
	checker := a.config().Checker
	sem := make(chan struct{}, max(1, a.CheckConcurrency))
	var wg sync.WaitGroup
	for _, j := range jobs {
		if ctx.Err() != nil {
			a.abandon(j)
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(j checkJob) {
			defer func() { <-sem; wg.Done() }()
			res, err := a.check(ctx, checker, j.req)
			if ctx.Err() != nil {
				a.abandon(j) // 停止（重载）打断的请求不算面板不可用
				return
			}
			a.applyResult(j, res, err)
		}(j)
	}
	wg.Wait()
}

func (a *Admission) abandon(j checkJob) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.entryLocked(j.uid, j.ip); e != nil && e.sess == j.sess {
		a.finishCheckLocked(j.uid, j.ip, e, j.sess)
		return
	}
	j.sess.checking = false
}

// finishCheckLocked 一次后台检查结束；期间 release 已到期（M1）就现在发
func (a *Admission) finishCheckLocked(uid int, ip string, e *admEntry, s *admSession) {
	s.checking = false
	if !s.releaseAfterCheck {
		return
	}
	s.releaseAfterCheck = false
	if s.releasing && e.links <= 0 {
		a.pushLocked(releaseItem{uid: uid, ip: ip, sess: s, notBefore: a.now()})
	}
}

// applyResult 保活 / 补发的结果：放行清除 degraded；失败标记 degraded、不关闭连接；
// 拒绝时删除会话并关闭这个 (uid, ip) 跨所有入站的全部连接（A2、A6），短暂负缓存后同一 (uid, ip) 的新连接重新 check
func (a *Admission) applyResult(j checkJob, res *panel.SessionResult, err error) {
	a.mu.Lock()
	e := a.entryLocked(j.uid, j.ip)
	if e == nil || e.sess != j.sess {
		j.sess.checking = false
		a.mu.Unlock()
		return // 会话已经不在了（释放、被拒、Forget）
	}
	s := e.sess
	if err != nil {
		if !s.degraded {
			s.degraded = true
			a.degraded++
		}
		a.finishCheckLocked(j.uid, j.ip, e, s)
		a.mu.Unlock()
		return
	}
	if res.Allowed {
		if s.degraded {
			s.degraded = false
			a.degraded--
		}
		a.finishCheckLocked(j.uid, j.ip, e, s)
		a.panelOKLocked()
		a.mu.Unlock()
		return
	}
	// 被拒：面板上没有这个成员，不需要 release
	s.checking = false
	s.releaseAfterCheck = false
	closers := make([]func(), 0, len(e.closers))
	for _, c := range e.closers {
		closers = append(closers, c.fn)
	}
	a.dropSessionLocked(j.uid, j.ip, e)
	a.cacheDenyLocked(admKey{j.uid, j.ip}, a.now())
	a.panelOKLocked()
	closer := a.cfg.Closer
	a.mu.Unlock()
	log.WithFields(log.Fields{"uid": j.uid, "ip": j.ip, "reason": res.Reason, "current": res.Current, "limit": res.Limit}).Info("准入保活被拒，关闭该 IP 的全部连接")
	for _, c := range closers {
		c()
	}
	if closer != nil {
		closer(j.uid, j.ip)
	}
}

// ===== 释放队列（A7）=====

type releaseItem struct {
	uid  int
	ip   string
	sess *admSession
	// forced 会话已从表中去掉（Forget、停止时放回），不再要求“仍无链路”；发送前仍要检查是否已被新会话取代
	forced bool
	// notBefore 宽限：这个时间之前不发（§7.6，期间有新连接即撤销）
	notBefore time.Time
}

func (a *Admission) releaseWorker(ctx context.Context) {
	defer a.wg.Done()
	for {
		for {
			if ctx.Err() != nil {
				return
			}
			it, ok := a.queue.pop()
			if !ok {
				break
			}
			if a.queue.len() > 0 {
				a.queue.signal() // 让其他 worker 一起处理
			}
			a.processRelease(ctx, it)
		}
		select {
		case <-ctx.Done():
			return
		case <-a.queue.notify:
		}
	}
}

// claimRelease 发出前的检查（锁内）：
//   - 普通条目：同一 (uid, ip) 在此期间又有新连接就撤销（复用原会话，不重新 check）；保活在途就等它结束再发（M1）；否则把会话移出表；
//   - 同一归并组（同 IP 的新会话，或 IPv6 同一 /64 的其他地址）仍有有效会话：面板成员仍在使用，不发（I2、I3）
func (a *Admission) claimRelease(it releaseItem) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !it.forced {
		e := a.entryLocked(it.uid, it.ip)
		if e == nil || e.sess != it.sess || !it.sess.releasing || e.links > 0 {
			return false
		}
		if it.sess.checking {
			it.sess.releaseAfterCheck = true
			return false
		}
		a.dropSessionLocked(it.uid, it.ip, e)
	}
	return !a.groupBusyLocked(it.uid, it.ip, it.sess)
}

// recheckGroup release 发出后同一归并组又建了会话（release 在途时的新连接）：立即补发 check，让面板重新登记成员（I3）
func (a *Admission) recheckGroup(ctx context.Context, it releaseItem) {
	a.mu.Lock()
	g := admGroup(it.ip)
	var jobs []checkJob
	for ip, e := range a.users[it.uid] {
		s := e.sess
		if s == nil || s == it.sess || s.releasing || s.checking || admGroup(ip) != g {
			continue
		}
		s.next = a.jitterLocked(a.now())
		jobs = append(jobs, jobFor(it.uid, ip, s))
	}
	a.mu.Unlock()
	a.runChecks(ctx, jobs)
}

func (a *Admission) groupBusy(it releaseItem) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.groupBusyLocked(it.uid, it.ip, it.sess)
}

func (a *Admission) processRelease(ctx context.Context, it releaseItem) {
	if d := it.notBefore.Sub(a.now()); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			a.queue.pushFront(it) // 尚未处理，原样放回
			return
		case <-t.C:
		}
	}
	if !a.claimRelease(it) {
		return
	}
	checker := a.config().Checker
	if checker == nil {
		return
	}
	req := panel.SessionRequest{Port: it.sess.port, UserID: int64(it.uid), ClientIP: it.ip, ConnectionID: it.sess.connID}
	var err error
	for attempt := 0; ; attempt++ {
		if attempt > 0 && a.groupBusy(it) {
			// 重试期间建了新会话：之前的尝试可能已在面板生效，补发 check 重新登记后放弃
			a.recheckGroup(ctx, it)
			return
		}
		rctx, cancel := context.WithTimeout(ctx, a.Timeout)
		err = checker.Release(rctx, req)
		cancel()
		if err == nil {
			a.panelOK()
			a.recheckGroup(ctx, it)
			return
		}
		if attempt >= len(a.ReleaseBackoff) || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(a.ReleaseBackoff[attempt]):
		}
	}
	if ctx.Err() != nil {
		// 停止（重载）时未发出的 release 放回队首，重新启动后继续；会话已移出表，按强制发送处理（发送前仍检查新会话）
		it.forced = true
		it.notBefore = time.Time{}
		a.queue.pushFront(it)
		return
	}
	var se *panel.SessionError
	level := log.WarnLevel
	if errors.As(err, &se) && se.Misconfigured() {
		level = log.ErrorLevel
	}
	log.WithFields(log.Fields{"uid": it.uid, "ip": it.ip, "err": err}).Log(level, fmt.Sprintf("准入 release 重试 %d 次仍失败，丢弃（面板 120 秒后自然过期）", len(a.ReleaseBackoff)))
}

type releaseQueue struct {
	mu     sync.Mutex
	items  []releaseItem
	cap    int
	notify chan struct{}
}

func newReleaseQueue(capacity int) *releaseQueue {
	return &releaseQueue{cap: capacity, notify: make(chan struct{}, 1)}
}

func (q *releaseQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// push 入队；满了丢弃最旧的一条并返回它
func (q *releaseQueue) push(it releaseItem) (dropped releaseItem, ok bool) {
	q.mu.Lock()
	if len(q.items) >= q.cap {
		dropped, ok = q.items[0], true
		q.items[0] = releaseItem{}
		q.items = q.items[1:]
	}
	q.items = append(q.items, it)
	q.mu.Unlock()
	q.signal()
	return dropped, ok
}

// pushFront 放回队首（停止时未发出的）；满了就丢弃（这些条目的会话已移出表或仍按 releasing 由扫描兜底）
func (q *releaseQueue) pushFront(it releaseItem) {
	q.mu.Lock()
	if len(q.items) < q.cap {
		q.items = append([]releaseItem{it}, q.items...)
	}
	q.mu.Unlock()
}

func (q *releaseQueue) pop() (releaseItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		q.items = nil
		return releaseItem{}, false
	}
	it := q.items[0]
	q.items[0] = releaseItem{}
	q.items = q.items[1:]
	return it, true
}

func (q *releaseQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// ===== 排查与测试 =====

// AdmissionStats 会话表概况
type AdmissionStats struct {
	Sessions, Degraded, Entries, Queued int
}

// Stats 会话数、degraded 会话数、(uid, ip) 条目数、释放队列长度
func (a *Admission) Stats() AdmissionStats {
	a.mu.Lock()
	entries := 0
	for _, m := range a.users {
		entries += len(m)
	}
	st := AdmissionStats{Sessions: a.sessions, Degraded: a.degraded, Entries: entries}
	a.mu.Unlock()
	st.Queued = a.queue.len()
	return st
}

// HasSession (uid, ip) 是否有准入会话
func (a *Admission) HasSession(uid int, ip string) bool {
	ip, _ = admNormalizeIP(ip)
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.entryLocked(uid, ip)
	return e != nil && e.sess != nil
}

// ===== 进程级实例 =====

var (
	admissionMu sync.Mutex
	admission   *Admission
)

// EnsureAdmission 取得进程级准入实例：没有就新建，有就替换外部依赖（会话表跨重载保留，已有连接关闭时仍能对上）
func EnsureAdmission(cfg AdmissionConfig) *Admission {
	admissionMu.Lock()
	defer admissionMu.Unlock()
	if admission == nil {
		admission = NewAdmission(cfg)
	} else {
		admission.Configure(cfg)
	}
	return admission
}

// CurrentAdmission 进程级准入实例；未启用 admission_v1 时为 nil
func CurrentAdmission() *Admission {
	admissionMu.Lock()
	defer admissionMu.Unlock()
	return admission
}

// DisableAdmission 关闭 admission_v1 后（重载）停止并撤掉进程级实例；之后建立的限速器不再准入。
// 已有链路的释放函数仍指向旧实例，只做计数，不再发请求
func DisableAdmission() {
	admissionMu.Lock()
	a := admission
	admission = nil
	admissionMu.Unlock()
	if a != nil {
		a.Stop()
	}
}
