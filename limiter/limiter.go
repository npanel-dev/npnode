package limiter

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/common/rate"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limitLock.Lock()
	limiter = map[string]*Limiter{}
	limitLock.Unlock()
	connTablesMu.Lock()
	connTables = map[string]*ConnTable{}
	connTablesMu.Unlock()
}

type Limiter struct {
	SpeedLimit    int
	UserOnlineIP  *sync.Map      // Key: TagUUID, value: {Key: Ip, value: Uid}
	OldUserOnline *sync.Map      // Key: Ip, value: Uid
	UUIDtoUID     map[string]int // Key: UUID, value: Uid
	UserLimitInfo *sync.Map      // Key: TagUUID, value: UserLimitInfo
	SpeedLimiter  *sync.Map      // key: TagUUID, value: *rate.Bucket（间接层，U5 原地替换）
	AliveList     map[int]int    // Key: Uid, value: alive_ip
	// BitPerSecond 声明了 seat_v1：speed_limit 按 bit/s 解释；否则按旧口径 Mbps 解释
	BitPerSecond bool
	// Conns 活跃连接表：声明了 online_v1 或 admission_v1 时建立（准入复用它的开、关钩子），都未声明时为 nil
	Conns *ConnTable
	// OnlineReport 声明了 online_v1：在线上报按活跃连接表输出；否则在线口径与旧版一致
	OnlineReport bool
	// Admission 声明了 admission_v1：进程级准入会话表，对 device_limit > 0 的条目准入；未声明时为 nil，CheckLimit 保持原逻辑
	Admission *Admission
	// Protocol、Port 本入站的协议和端口，准入请求带上
	Protocol string
	Port     int

	tag string
	// uidTags uid → 本入站的 taguuid（只在挂了准入时维护），关闭某 (uid, ip) 的全部连接时反查
	idxMu   sync.RWMutex
	uidTags map[int]map[string]struct{}
}

type UserLimitInfo struct {
	UID int
	// SpeedLimit 面板下发的限速：声明 seat_v1 时单位为 bit/s，否则为旧口径 Mbps；0 表示不限
	SpeedLimit        int
	DeviceLimit       int
	DynamicSpeedLimit int
	ExpireTime        int64
	OverLimit         bool
}

func AddLimiter(tag string, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	return AddInboundLimiter(tag, "", 0, users, aliveList)
}

// AddInboundLimiter 建立入站的限速器；protocol、port 用于准入请求（admission_v1）。
// 声明了 admission_v1 且进程级准入实例已建立（EnsureAdmission）时挂上准入。
func AddInboundLimiter(tag, protocol string, port int, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	info := &Limiter{
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		OldUserOnline: new(sync.Map),
		BitPerSecond:  panel.SeatV1Enabled(),
		OnlineReport:  panel.OnlineV1Enabled(),
		Protocol:      protocol,
		Port:          port,
		tag:           tag,
	}
	if panel.AdmissionV1Enabled() {
		info.Admission = CurrentAdmission()
	}
	if info.OnlineReport || info.Admission != nil {
		info.Conns = connTableFor(tag, info.OnlineReport)
	}
	if info.Admission != nil {
		info.uidTags = make(map[int]map[string]struct{})
	}
	uuidmap := make(map[string]int)
	for i := range users {
		info.indexUser(users[i].Id, format.UserTag(tag, users[i].Uuid))
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		userLimit.OverLimit = false
		info.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	info.UUIDtoUID = uuidmap
	limitLock.Lock()
	limiter[tag] = info
	limitLock.Unlock()
	return info
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

// indexUser / unindexUser 维护 uid → taguuid 反查（只在挂了准入时）
func (l *Limiter) indexUser(uid int, taguuid string) {
	if l.uidTags == nil {
		return
	}
	l.idxMu.Lock()
	defer l.idxMu.Unlock()
	m := l.uidTags[uid]
	if m == nil {
		m = make(map[string]struct{})
		l.uidTags[uid] = m
	}
	m[taguuid] = struct{}{}
}

func (l *Limiter) unindexUser(uid int, taguuid string) {
	if l.uidTags == nil {
		return
	}
	l.idxMu.Lock()
	defer l.idxMu.Unlock()
	if m := l.uidTags[uid]; m != nil {
		delete(m, taguuid)
		if len(m) == 0 {
			delete(l.uidTags, uid)
		}
	}
}

// TagUUIDsForUID 本进程所有挂了准入的入站上 uid 对应的 taguuid（LinkManagers 的键）
func TagUUIDsForUID(uid int) []string {
	limitLock.RLock()
	defer limitLock.RUnlock()
	var out []string
	for _, l := range limiter {
		if l.uidTags == nil {
			continue
		}
		l.idxMu.RLock()
		for k := range l.uidTags[uid] {
			out = append(out, k)
		}
		l.idxMu.RUnlock()
	}
	return out
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo) {
	for i := range deleted {
		l.unindexUser(deleted[i].Id, format.UserTag(tag, deleted[i].Uuid))
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.UserOnlineIP.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		delete(l.UUIDtoUID, deleted[i].Uuid)
		delete(l.AliveList, deleted[i].Id)
	}
	for i := range added {
		userLimit := &UserLimitInfo{
			UID: added[i].Id,
		}
		if added[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = added[i].SpeedLimit
			userLimit.ExpireTime = 0
		}
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
		l.indexUser(added[i].Id, format.UserTag(tag, added[i].Uuid))
	}
}

// UpdateUserLimit 原地更新用户的限速和设备数（U5）：替换 UserLimitInfo，并替换已有 bucket 间接层里的 bucket，
// 已有连接下一次写入即按新速率；不删用户、不断连接。用户不在限速器里时什么都不做。
// 动态限速（DynamicSpeedLimit / ExpireTime）原样保留。
func (l *Limiter) UpdateUserLimit(taguuid string, speedLimit, deviceLimit int) {
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return
	}
	nu := *v.(*UserLimitInfo)
	oldDevice := nu.DeviceLimit
	nu.SpeedLimit = speedLimit
	nu.DeviceLimit = deviceLimit
	l.UserLimitInfo.Store(taguuid, &nu)
	if l.Admission != nil {
		// 面板切换实时准入时名额条目的 device_limit 在 0/1 之间变化：关掉的立即 release，打开的对已有连接补 check
		switch {
		case oldDevice > 0 && deviceLimit <= 0:
			l.Admission.Forget(nu.UID)
		case oldDevice <= 0 && deviceLimit > 0:
			l.Admission.Adopt(nu.UID, l.Protocol, l.Port)
		}
	}
	ref, ok := l.SpeedLimiter.Load(taguuid)
	if !ok {
		return // 还没有连接建立过 bucket，下一次 CheckLimit 按新值建立
	}
	userLimit := nu.SpeedLimit
	if nu.ExpireTime == 0 || nu.ExpireTime >= time.Now().Unix() {
		userLimit = determineSpeedLimit(nu.SpeedLimit, nu.DynamicSpeedLimit)
	}
	ref.(*rate.Bucket).Store(newUserBucket(speedLimitBytesPerSecond(determineSpeedLimit(l.SpeedLimit, userLimit), l.BitPerSecond)))
}

// newUserBucket 字节每秒为 limit 的 bucket；limit <= 0 表示不限速，返回 nil
func newUserBucket(limit int64) *ratelimit.Bucket {
	if limit <= 0 {
		return nil
	}
	return ratelimit.NewBucketWithQuantum(time.Second, limit, limit) // Byte/s
}

func (l *Limiter) CheckLimit(taguuid string, ip string, isTcp bool, noSSUDP bool) (Bucket *rate.Bucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		if u.ExpireTime < time.Now().Unix() && u.ExpireTime != 0 {
			if u.SpeedLimit != 0 {
				userLimit = u.SpeedLimit
				u.DynamicSpeedLimit = 0
				u.ExpireTime = 0
			} else {
				l.UserLimitInfo.Delete(taguuid)
			}
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, u.DynamicSpeedLimit)
		}
	} else {
		return nil, true
	}
	// 实时准入（admission_v1）：device_limit > 0 的条目由面板判定，本机设备判断不再生效（原逻辑也从未生效，aliveIp 恒为 0）
	admitted := l.Admission != nil && deviceLimit > 0
	if admitted && !l.Admission.Admit(uid, ip, deviceLimit, l.Protocol, l.Port) {
		return nil, true
	}
	if noSSUDP {
		// Store online user for device limit
		ipMap := new(sync.Map)
		ipMap.Store(ip, uid)
		aliveIp := l.AliveList[uid]
		// If any device is online
		if v, ok := l.UserOnlineIP.LoadOrStore(taguuid, ipMap); ok {
			ipMap := v.(*sync.Map)
			// If this is a new ip
			if _, ok := ipMap.LoadOrStore(ip, uid); !ok {
				if deviceLimit > 0 && !admitted {
					if deviceLimit <= aliveIp {
						ipMap.Delete(ip)
						return nil, true
					}
				}
			}
		} else if v, ok := l.OldUserOnline.Load(ip); ok {
			if v.(int) == uid {
				l.OldUserOnline.Delete(ip)
			}
		} else {
			if deviceLimit > 0 && !admitted {
				if deviceLimit <= aliveIp {
					l.UserOnlineIP.Delete(taguuid)
					return nil, true
				}
			}
		}
	}

	limit := speedLimitBytesPerSecond(determineSpeedLimit(nodeLimit, userLimit), l.BitPerSecond)
	if limit > 0 {
		// 同一用户的连接共用一个间接层：首次建立后沿用（与原先缓存 bucket 的行为一致），
		// 间接层里为空（U5 改成过不限速）时按本次计算的速率补上
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			ref := v.(*rate.Bucket)
			if ref.Load() == nil {
				ref.Store(newUserBucket(limit))
			}
			return ref, false
		}
		v, _ := l.SpeedLimiter.LoadOrStore(taguuid, rate.NewBucket(newUserBucket(limit)))
		return v.(*rate.Bucket), false
	} else {
		return nil, false
	}
}

// speedLimitBytesPerSecond 把面板限速换算成字节每秒，0 表示不限。
// bitPerSecond（seat_v1）：字节每秒 = speed_limit / 8，不做任何启发式换算；不足 1 字节每秒按 1 处理，不能当成不限。
// 否则保持旧口径：按 Mbps 解释（× 1000000 / 8）。
func speedLimitBytesPerSecond(speedLimit int, bitPerSecond bool) int64 {
	if !bitPerSecond {
		return int64(speedLimit) * 1000000 / 8
	}
	if speedLimit <= 0 {
		return 0
	}
	limit := int64(speedLimit) / 8
	if limit == 0 {
		limit = 1
	}
	return limit
}

// ActiveOnline 在线上报是否按活跃连接口径（online_v1）
func (l *Limiter) ActiveOnline() bool {
	return l.Conns != nil && l.OnlineReport
}

// OpenConn 把一条已通过 CheckLimit 的链路登记到活跃连接表和准入会话表，返回释放函数（可重复、并发调用，只生效一次）。
// 未声明 online_v1 和 admission_v1、用户不在限速器里、uid 无效或 IP 无法解析时返回 nil，不登记。
// ip 与 CheckLimit 用同一个来源（已按可信 XFF / PROXY protocol 解析后的地址），去掉 IPv4 映射前缀。
func (l *Limiter) OpenConn(taguuid string, ip string) (release func()) {
	if l.Conns == nil {
		return nil
	}
	ip = strings.TrimPrefix(ip, "::ffff:")
	if net.ParseIP(ip) == nil {
		return nil
	}
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return nil
	}
	uid := v.(*UserLimitInfo).UID
	if uid <= 0 {
		return nil
	}
	connRelease := l.Conns.Open(uid, ip)
	if l.Admission == nil {
		return connRelease
	}
	// 准入与连接表共用这一个钩子：同一条链路在两张表里同时登记、同时释放
	admRelease := l.Admission.Open(uid, ip)
	var once sync.Once
	return func() {
		once.Do(func() {
			connRelease()
			admRelease()
		})
	}
}

// GetOnlineDevice 本周期的在线 (uid, ip)。
// 声明 online_v1 时返回活跃连接表的快照（含长连接、空闲连接和本周期的短连接，每个 uid 最多 64 个 IP）；
// 否则保持旧口径：本周期新建连接时登记的 IP，读出后清空。
// 两种口径都会执行旧的读出清空，保证 CheckLimit 的设备数判断（OldUserOnline）与旧版一致，表也不会无限增长。
func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	legacy := l.drainLegacyOnline()
	if l.ActiveOnline() {
		snapshot := l.Conns.Snapshot(MaxOnlineIPsPerUID)
		return &snapshot, nil
	}
	return legacy, nil
}

// RestoreOnline 在线上报失败：online_v1 口径下把这次读出的结果放回活跃连接表，下一周期补报；旧口径不做任何事（与旧版一致）
func (l *Limiter) RestoreOnline(users []panel.OnlineUser) {
	if l.ActiveOnline() {
		l.Conns.Restore(users)
	}
}

func (l *Limiter) drainLegacyOnline() *[]panel.OnlineUser {
	var onlineUser []panel.OnlineUser
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			uid := value.(int)
			ip := key.(string)
			l.OldUserOnline.Store(ip, uid)
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device
		return true
	})

	return &onlineUser
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}
