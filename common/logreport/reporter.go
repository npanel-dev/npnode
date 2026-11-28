package logreport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

const (
	// throttleWindow 为同一合并键两次进入批次之间的最小间隔。
	throttleWindow = 60 * time.Second
	// flushInterval 为 Run 的定时发送间隔。
	flushInterval = 10 * time.Second
	// batchTrigger 为可发送事件达到该数量时立即发送。
	batchTrigger = 20
	// maxQueued 为内存中（暂扣 + 待发 + 未发批次）事件总数上限，超过丢最旧。
	maxQueued = 500
)

var (
	errNoSender = errors.New("logreport: 尚未设置 Sender")
	errUnsent   = errors.New("logreport: 退出前仍有未发送的上报")
)

// Sender 负责把一批日志上报发送给面板（由 gRPC/HTTP 客户端实现）。
type Sender interface {
	SendLogs(ctx context.Context, r *nodev1.LogReport) error
}

// Options 为 Reporter 配置。Now/NewID 为空时使用默认实现。
type Options struct {
	Sender    Sender
	Version   string
	SpoolPath string
	Now       func() time.Time
	NewID     func() string
}

// item 为一条尚未进入批次的事件；holdUntil 非零表示在该时刻前处于暂扣状态。
type item struct {
	key       string
	ev        *nodev1.LogEvent
	holdUntil time.Time
}

// Reporter 负责日志事件的合并节流、分批发送、失败重发与退出前落盘。
// 注意：Add 在 logrus hook 内被调用，必须廉价、不阻塞，且绝不能通过 logrus 打 Info 及以上日志。
type Reporter struct {
	version   string
	spoolPath string
	now       func() time.Time
	newID     func() string

	notify  chan struct{} // 可发送事件满 batchTrigger 时通知 Run
	sem     chan struct{} // 串行化发送，容量 1
	spoolMu sync.Mutex    // 串行化 spool 文件读写

	mu         sync.Mutex
	sender     Sender
	pending    []*item                       // 按到达顺序
	pendingKey map[string]*item              // 合并键 -> 未进入批次的事件
	lastSent   map[string]time.Time          // 合并键 -> 最近一次进入批次的时间
	resolves   []*nodev1.LogResolve          // 按首次出现顺序
	resolveKey map[string]*nodev1.LogResolve // scope|protocol|port -> resolve
	batches    []*nodev1.LogReport           // 已分配 ReportId、尚未发送成功的批次
	batchEvs   int                           // batches 中的事件总数
}

// NewReporter 创建 Reporter。
func NewReporter(opts Options) *Reporter {
	r := &Reporter{
		version:    opts.Version,
		spoolPath:  opts.SpoolPath,
		now:        opts.Now,
		newID:      opts.NewID,
		sender:     opts.Sender,
		notify:     make(chan struct{}, 1),
		sem:        make(chan struct{}, 1),
		pendingKey: map[string]*item{},
		lastSent:   map[string]time.Time{},
		resolveKey: map[string]*nodev1.LogResolve{},
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.newID == nil {
		r.newID = defaultIDGen()
	}
	return r
}

// defaultIDGen 生成 "bootID:seq" 形式的 ID，bootID 每个 Reporter 随机生成。
func defaultIDGen() func() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		b = []byte(strconv.FormatInt(time.Now().UnixNano(), 16))
	}
	boot := hex.EncodeToString(b)
	var seq atomic.Uint64
	return func() string {
		return boot + ":" + strconv.FormatUint(seq.Add(1), 10)
	}
}

// SetSender 设置或替换 Sender（启动中途拿到客户端、热重载时调用）。
func (r *Reporter) SetSender(s Sender) {
	r.mu.Lock()
	r.sender = s
	r.mu.Unlock()
}

// Add 加入一条事件（logrus Hook 的 sink）。
func (r *Reporter) Add(e *nodev1.LogEvent) {
	if e == nil {
		return
	}
	if e.Version == "" {
		e.Version = r.version
	}
	if e.Count == 0 {
		e.Count = 1
	}
	key := MergeKey(e.Code, e.Protocol, int(e.Port), e.Message)

	r.mu.Lock()
	now := r.now()
	if it, ok := r.pendingKey[key]; ok {
		// 同键已有未进入批次的事件：合并
		p := it.ev
		p.Count += e.Count
		p.Message = e.Message
		p.Context = e.Context
		p.OccurredAt = e.OccurredAt
		p.Level = e.Level
		p.Exiting = e.Exiting
	} else {
		it := &item{key: key, ev: e}
		if last, ok := r.lastSent[key]; ok && now.Before(last.Add(throttleWindow)) {
			it.holdUntil = last.Add(throttleWindow)
		}
		r.pending = append(r.pending, it)
		r.pendingKey[key] = it
		r.enforceCapLocked()
	}
	trigger := r.eligibleCountLocked(now) >= batchTrigger
	r.mu.Unlock()

	if trigger {
		select {
		case r.notify <- struct{}{}:
		default:
		}
	}
}

// Resolve 记录一条恢复通知；同一 scope/protocol/port 只保留最新一条。
func (r *Reporter) Resolve(res *nodev1.LogResolve) {
	if res == nil {
		return
	}
	key := res.Scope + "|" + res.Protocol + "|" + strconv.FormatUint(uint64(res.Port), 10)
	r.mu.Lock()
	defer r.mu.Unlock()
	if res.ResolvedAt == 0 {
		res.ResolvedAt = r.now().UnixMilli()
	}
	if old, ok := r.resolveKey[key]; ok {
		for i, x := range r.resolves {
			if x == old {
				r.resolves[i] = res
				break
			}
		}
	} else {
		r.resolves = append(r.resolves, res)
	}
	r.resolveKey[key] = res
}

// Run 每 flushInterval 或可发送事件满 batchTrigger 条时发送，直到 ctx 结束。
func (r *Reporter) Run(ctx context.Context) {
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.notify:
		}
		// 发送失败保留数据待下次重发；这里不能打 Warn 及以上日志，否则会经 hook 回流到自身
		_ = r.flush(ctx, false)
	}
}

// FlushFatal 退出前在 timeout 内发送全部数据（忽略节流暂扣），
// 未能发出的追加写入 spool 文件。仅当全部发送成功时返回 nil。
func (r *Reporter) FlushFatal(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var err error
	// 发送期间可能有新事件到达，最多补发几轮
	for i := 0; i < 3; i++ {
		if err = r.flush(ctx, true); err != nil || r.empty() {
			break
		}
	}

	r.mu.Lock()
	r.buildBatchLocked(true)
	rem := append([]*nodev1.LogReport(nil), r.batches...)
	r.mu.Unlock()
	if len(rem) == 0 {
		return nil
	}
	if err == nil {
		err = errUnsent
	}
	if r.spoolPath != "" {
		if werr := r.appendSpool(rem); werr != nil {
			err = errors.Join(err, werr)
		}
	}
	return err
}

// flush 把可发送事件与 resolve 组成新批次，并按顺序发送全部未发批次。
// all 为真时忽略节流暂扣，且即使没有 Sender 也会组批（供落盘）。
func (r *Reporter) flush(ctx context.Context, all bool) error {
	r.mu.Lock()
	if r.sender == nil && !all {
		r.mu.Unlock()
		return errNoSender
	}
	r.buildBatchLocked(all)
	r.mu.Unlock()

	// 同一时刻只允许一个发送；等待时仍受 ctx 约束
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.sem }()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		if len(r.batches) == 0 {
			r.mu.Unlock()
			return nil
		}
		b, s := r.batches[0], r.sender
		r.mu.Unlock()
		if s == nil {
			return errNoSender
		}
		if err := s.SendLogs(ctx, b); err != nil {
			return err
		}
		r.mu.Lock()
		r.removeBatchLocked(b.ReportId)
		r.mu.Unlock()
	}
}

// buildBatchLocked 把可发送（或 all 时全部）待发事件与 resolve 组成一个新批次。
func (r *Reporter) buildBatchLocked(all bool) {
	now := r.now()
	var evs []*nodev1.LogEvent
	kept := r.pending[:0]
	for _, it := range r.pending {
		if all || it.holdUntil.IsZero() || !now.Before(it.holdUntil) {
			evs = append(evs, it.ev)
			delete(r.pendingKey, it.key)
			r.lastSent[it.key] = now
		} else {
			kept = append(kept, it)
		}
	}
	for i := len(kept); i < len(r.pending); i++ {
		r.pending[i] = nil
	}
	r.pending = kept

	// 清理已过窗口且无暂扣事件的节流记录
	for k, t := range r.lastSent {
		if _, ok := r.pendingKey[k]; !ok && !now.Before(t.Add(throttleWindow)) {
			delete(r.lastSent, k)
		}
	}

	if len(evs) == 0 && len(r.resolves) == 0 {
		return
	}
	r.batches = append(r.batches, &nodev1.LogReport{
		ReportId: r.newID(),
		Events:   evs,
		Resolves: r.resolves,
	})
	r.batchEvs += len(evs)
	r.resolves = nil
	r.resolveKey = map[string]*nodev1.LogResolve{}
}

// enforceCapLocked 保证事件总数不超过 maxQueued，优先丢弃最旧批次中的事件。
// 正在发送的批次可能被并发读取，因此不原地修改，而是替换为同 ReportId 的新对象。
func (r *Reporter) enforceCapLocked() {
	for r.batchEvs+len(r.pending) > maxQueued {
		if r.batchEvs > 0 {
			for i, b := range r.batches {
				if len(b.Events) == 0 {
					continue
				}
				r.batchEvs--
				if len(b.Events) == 1 && len(b.Resolves) == 0 {
					r.batches = append(r.batches[:i:i], r.batches[i+1:]...)
				} else {
					r.batches[i] = &nodev1.LogReport{ReportId: b.ReportId, Events: b.Events[1:], Resolves: b.Resolves}
				}
				break
			}
			continue
		}
		it := r.pending[0]
		r.pending[0] = nil
		r.pending = r.pending[1:]
		delete(r.pendingKey, it.key)
	}
}

// eligibleCountLocked 统计当前可进入批次的待发事件数。
func (r *Reporter) eligibleCountLocked(now time.Time) int {
	if len(r.pending) < batchTrigger {
		return len(r.pending)
	}
	n := 0
	for _, it := range r.pending {
		if it.holdUntil.IsZero() || !now.Before(it.holdUntil) {
			n++
		}
	}
	return n
}

func (r *Reporter) removeBatchLocked(id string) {
	for i, b := range r.batches {
		if b.ReportId == id {
			r.batchEvs -= len(b.Events)
			r.batches = append(r.batches[:i:i], r.batches[i+1:]...)
			return
		}
	}
}

func (r *Reporter) empty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) == 0 && len(r.batches) == 0 && len(r.resolves) == 0
}
