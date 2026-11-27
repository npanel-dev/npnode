package panel

import (
	"fmt"
	"sync"
	"time"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
)

// chunkAssembleTimeout 一个分片序列从第 1 片起多久内必须收齐，否则丢弃并重新订阅
const chunkAssembleTimeout = 30 * time.Second

// chunkAssembler 重组面板 Sync 推送的用户分片（只有声明了 seat_v1 的通道才分片）。
//
// 面板把一次 FULL / DIFF 拆成 chunk_no = 1..chunk_total 的多条 UserListUpdate，各片 version、action 相同，
// 条目先排 removed 后排 users；同一入站的各片连续到达，不同入站的分片可能交错。所以：
//   - 按 (protocol, port) 分别缓冲，收齐后把各片的 users、removed 依次拼接成一条再分发（DIFF 仍按先删后加应用）；
//   - chunk_total <= 1 或没有分片字段按单片直接分发；
//   - 缺片、从中间开始、被不同序列打断或超时未收齐：丢弃整个序列并通知 onLost（本地版本作废、重新订阅取全量）；
//     已作废序列的剩余分片直接忽略，不重复通知。
type chunkAssembler struct {
	deliver func(UserUpdate)
	onLost  func(protocol string, port int)
	timeout time.Duration

	mu   sync.Mutex
	bufs map[string]*chunkBuffer
}

type chunkBuffer struct {
	protocol string
	port     int
	version  uint64
	action   nodev1.UserUpdateAction
	total    uint32
	next     uint32 // 期望的下一片序号
	users    []UserInfo
	removed  []UserInfo
	timer    *time.Timer
	// discarding 序列已作废：剩余分片直接忽略，收到最后一片时移除
	discarding bool
}

func newChunkAssembler(deliver func(UserUpdate), onLost func(protocol string, port int), timeout time.Duration) *chunkAssembler {
	return &chunkAssembler{deliver: deliver, onLost: onLost, timeout: timeout, bufs: map[string]*chunkBuffer{}}
}

func chunkKey(protocol string, port int) string {
	return fmt.Sprintf("%s:%d", protocol, port)
}

func updateFromProto(u *nodev1.UserListUpdate) UserUpdate {
	return UserUpdate{
		Protocol: u.GetProtocol(),
		Port:     int(u.GetPort()),
		Version:  u.GetVersion(),
		Action:   u.GetAction(),
		Users:    userEntriesToInfo(u.GetUsers()),
		Removed:  userEntriesToInfo(u.GetRemoved()),
	}
}

// discardLocked 作废缓冲中的序列；返回 true 表示需要通知 onLost（序列此前尚未作废）
func (b *chunkBuffer) discardLocked() bool {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.users, b.removed = nil, nil
	if b.discarding {
		return false
	}
	b.discarding = true
	return true
}

// handle 处理一条推送；回调在释放锁之后调用
func (a *chunkAssembler) handle(u *nodev1.UserListUpdate) {
	protocol, port := u.GetProtocol(), int(u.GetPort())
	key := chunkKey(protocol, port)
	no, total := u.GetChunkNo(), u.GetChunkTotal()

	var lost bool
	var ready *UserUpdate
	reason := ""

	a.mu.Lock()
	b := a.bufs[key]
	if total <= 1 {
		// 单片：若有未收齐的序列，说明它被打断了
		if b != nil {
			if b.discardLocked() {
				lost, reason = true, "未收齐的分片被新的推送打断"
			}
			delete(a.bufs, key)
		}
		single := updateFromProto(u)
		ready = &single
	} else {
		if b != nil && (b.version != u.GetVersion() || b.action != u.GetAction() || b.total != total) {
			// 不同序列：旧序列未收齐
			if b.discardLocked() {
				lost, reason = true, "未收齐的分片被不同版本的序列打断"
			}
			delete(a.bufs, key)
			b = nil
		}
		switch {
		case b == nil && no != 1:
			// 没有第 1 片（从中间开始）：整个序列作废
			b = &chunkBuffer{protocol: protocol, port: port, version: u.GetVersion(), action: u.GetAction(), total: total, discarding: true}
			if no < total {
				a.bufs[key] = b
			}
			lost, reason = true, "分片序列缺少第 1 片"
		case b == nil:
			b = &chunkBuffer{protocol: protocol, port: port, version: u.GetVersion(), action: u.GetAction(), total: total, next: 2}
			b.users = append(b.users, userEntriesToInfo(u.GetUsers())...)
			b.removed = append(b.removed, userEntriesToInfo(u.GetRemoved())...)
			a.bufs[key] = b
			buf := b
			b.timer = time.AfterFunc(a.timeout, func() { a.expire(key, buf) })
		case b.discarding:
			if no >= total {
				delete(a.bufs, key)
			}
		case no != b.next:
			if b.discardLocked() {
				lost, reason = true, fmt.Sprintf("分片不连续（期望第 %d 片，收到第 %d 片）", b.next, no)
			}
			if no >= total {
				delete(a.bufs, key)
			}
		default:
			b.users = append(b.users, userEntriesToInfo(u.GetUsers())...)
			b.removed = append(b.removed, userEntriesToInfo(u.GetRemoved())...)
			b.next++
			if no == total {
				if b.timer != nil {
					b.timer.Stop()
				}
				delete(a.bufs, key)
				ready = &UserUpdate{Protocol: protocol, Port: port, Version: b.version, Action: b.action, Users: b.users, Removed: b.removed}
			}
		}
	}
	a.mu.Unlock()

	if lost {
		log.WithFields(log.Fields{"protocol": protocol, "port": port, "version": u.GetVersion()}).Warnf("[grpc.Sync] %s，丢弃并重新订阅取全量", reason)
		if a.onLost != nil {
			a.onLost(protocol, port)
		}
	}
	if ready != nil && a.deliver != nil {
		a.deliver(*ready)
	}
}

// expire 超时未收齐：作废并通知重新订阅（缓冲保留为作废状态，迟到的剩余分片直接忽略）
func (a *chunkAssembler) expire(key string, b *chunkBuffer) {
	a.mu.Lock()
	if a.bufs[key] != b || !b.discardLocked() {
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	log.WithFields(log.Fields{"protocol": b.protocol, "port": b.port, "version": b.version}).Warnf("[grpc.Sync] 分片 %s 内未收齐，丢弃并重新订阅取全量", a.timeout)
	if a.onLost != nil {
		a.onLost(b.protocol, b.port)
	}
}

// reset 流断开：清空全部缓冲（重连后面板会重新推送全量）
func (a *chunkAssembler) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, b := range a.bufs {
		if b.timer != nil {
			b.timer.Stop()
		}
		delete(a.bufs, key)
	}
}
