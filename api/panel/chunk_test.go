package panel

import (
	"sync"
	"testing"
	"time"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

type assemblerProbe struct {
	mu        sync.Mutex
	delivered []UserUpdate
	lost      []Inbound
}

func (p *assemblerProbe) deliver(u UserUpdate) {
	p.mu.Lock()
	p.delivered = append(p.delivered, u)
	p.mu.Unlock()
}

func (p *assemblerProbe) onLost(protocol string, port int) {
	p.mu.Lock()
	p.lost = append(p.lost, Inbound{Protocol: protocol, Port: port})
	p.mu.Unlock()
}

func (p *assemblerProbe) snapshot() ([]UserUpdate, []Inbound) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]UserUpdate(nil), p.delivered...), append([]Inbound(nil), p.lost...)
}

func newProbeAssembler(timeout time.Duration) (*chunkAssembler, *assemblerProbe) {
	p := &assemblerProbe{}
	return newChunkAssembler(p.deliver, p.onLost, timeout), p
}

func chunk(protocol string, port uint32, action nodev1.UserUpdateAction, version uint64, no, total uint32, users, removed []*nodev1.UserEntry) *nodev1.UserListUpdate {
	return &nodev1.UserListUpdate{Protocol: protocol, Port: port, Action: action, Version: version, ChunkNo: no, ChunkTotal: total, Users: users, Removed: removed}
}

func ue(id int64) *nodev1.UserEntry {
	return &nodev1.UserEntry{Id: id, Uuid: "u", SpeedLimit: 10485760}
}

func ids(users []UserInfo) []int {
	out := make([]int, 0, len(users))
	for _, u := range users {
		out = append(out, u.Id)
	}
	return out
}

func sameInts(got []int, want ...int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

const (
	full = nodev1.UserUpdateAction_USER_UPDATE_FULL
	diff = nodev1.UserUpdateAction_USER_UPDATE_DIFF
)

// 没有分片字段或 chunk_total<=1：按单片直接分发（未声明 seat_v1 的面板就是这样）
func TestChunkAssemblerPassesSingleMessagesThrough(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, full, 3, 0, 0, []*nodev1.UserEntry{ue(1), ue(2)}, nil))
	a.handle(chunk("vless", 443, diff, 4, 1, 1, []*nodev1.UserEntry{ue(3)}, []*nodev1.UserEntry{ue(1)}))
	got, lost := p.snapshot()
	if len(got) != 2 || len(lost) != 0 {
		t.Fatalf("delivered=%d lost=%v", len(got), lost)
	}
	if got[0].Version != 3 || got[0].Action != full || !sameInts(ids(got[0].Users), 1, 2) {
		t.Fatalf("first = %+v", got[0])
	}
	if got[1].Version != 4 || !sameInts(ids(got[1].Users), 3) || !sameInts(ids(got[1].Removed), 1) {
		t.Fatalf("second = %+v", got[1])
	}
}

// FULL 分 3 片：收齐后一次性分发，用户为各片之和
func TestChunkAssemblerReassemblesFull(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, full, 7, 1, 3, []*nodev1.UserEntry{ue(1), ue(2)}, nil))
	a.handle(chunk("vless", 443, full, 7, 2, 3, []*nodev1.UserEntry{ue(3)}, nil))
	if got, _ := p.snapshot(); len(got) != 0 {
		t.Fatalf("partial FULL must not be applied: %+v", got)
	}
	a.handle(chunk("vless", 443, full, 7, 3, 3, []*nodev1.UserEntry{ue(4)}, nil))
	got, lost := p.snapshot()
	if len(got) != 1 || len(lost) != 0 {
		t.Fatalf("delivered=%d lost=%v", len(got), lost)
	}
	u := got[0]
	if u.Protocol != "vless" || u.Port != 443 || u.Version != 7 || u.Action != full || !sameInts(ids(u.Users), 1, 2, 3, 4) {
		t.Fatalf("merged = %+v", u)
	}
}

// DIFF 分片：面板先排 removed 后排 users，合并后两边都完整
func TestChunkAssemblerReassemblesDiffKeepingRemovedAndUsers(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, diff, 9, 1, 2, nil, []*nodev1.UserEntry{ue(1), ue(2)}))
	a.handle(chunk("vless", 443, diff, 9, 2, 2, []*nodev1.UserEntry{ue(2), ue(5)}, []*nodev1.UserEntry{ue(3)}))
	got, _ := p.snapshot()
	if len(got) != 1 {
		t.Fatalf("delivered = %d", len(got))
	}
	if !sameInts(ids(got[0].Removed), 1, 2, 3) || !sameInts(ids(got[0].Users), 2, 5) || got[0].Version != 9 {
		t.Fatalf("merged = %+v", got[0])
	}
}

// 不同入站的分片可能交错到达，缓冲按 (protocol, port) 分开
func TestChunkAssemblerKeepsInboundsSeparate(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, full, 1, 1, 2, []*nodev1.UserEntry{ue(1)}, nil))
	a.handle(chunk("vless", 8443, full, 5, 1, 2, []*nodev1.UserEntry{ue(10)}, nil))
	a.handle(chunk("trojan", 443, full, 2, 1, 1, []*nodev1.UserEntry{ue(20)}, nil))
	a.handle(chunk("vless", 443, full, 1, 2, 2, []*nodev1.UserEntry{ue(2)}, nil))
	a.handle(chunk("vless", 8443, full, 5, 2, 2, []*nodev1.UserEntry{ue(11)}, nil))
	got, lost := p.snapshot()
	if len(got) != 3 || len(lost) != 0 {
		t.Fatalf("delivered=%d lost=%v", len(got), lost)
	}
	if got[0].Protocol != "trojan" || !sameInts(ids(got[1].Users), 1, 2) || got[1].Port != 443 || !sameInts(ids(got[2].Users), 10, 11) || got[2].Port != 8443 {
		t.Fatalf("delivered = %+v", got)
	}
}

// 未收齐时来了不同 version 的新序列：丢弃旧序列（通知重新订阅），新序列照常重组
func TestChunkAssemblerDropsIncompleteSequenceOnNewVersion(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, full, 1, 1, 3, []*nodev1.UserEntry{ue(1)}, nil))
	a.handle(chunk("vless", 443, full, 2, 1, 2, []*nodev1.UserEntry{ue(5)}, nil))
	a.handle(chunk("vless", 443, full, 2, 2, 2, []*nodev1.UserEntry{ue(6)}, nil))
	got, lost := p.snapshot()
	if len(got) != 1 || got[0].Version != 2 || !sameInts(ids(got[0].Users), 5, 6) {
		t.Fatalf("delivered = %+v", got)
	}
	if len(lost) != 1 || lost[0] != (Inbound{Protocol: "vless", Port: 443}) {
		t.Fatalf("lost = %v, want one vless:443", lost)
	}
}

// 分片不连续（缺片）：整个序列作废、通知一次重新订阅，剩余分片不再重复通知，也不分发
func TestChunkAssemblerDiscardsSequenceWithGap(t *testing.T) {
	a, p := newProbeAssembler(time.Minute)
	a.handle(chunk("vless", 443, diff, 4, 1, 3, nil, []*nodev1.UserEntry{ue(1)}))
	a.handle(chunk("vless", 443, diff, 4, 3, 3, []*nodev1.UserEntry{ue(2)}, nil))
	got, lost := p.snapshot()
	if len(got) != 0 || len(lost) != 1 {
		t.Fatalf("delivered=%+v lost=%v", got, lost)
	}
	// 从中间开始的序列（没有第 1 片）同样作废
	a.handle(chunk("vless", 443, diff, 5, 2, 3, nil, nil))
	a.handle(chunk("vless", 443, diff, 5, 3, 3, nil, nil))
	got, lost = p.snapshot()
	if len(got) != 0 || len(lost) != 2 {
		t.Fatalf("orphan chunks: delivered=%+v lost=%v", got, lost)
	}
	// 之后完整的单片照常分发
	a.handle(chunk("vless", 443, full, 6, 1, 1, []*nodev1.UserEntry{ue(9)}, nil))
	if got, _ := p.snapshot(); len(got) != 1 || got[0].Version != 6 {
		t.Fatalf("delivered = %+v", got)
	}
}

// 超时未收齐：丢弃并通知重新订阅
func TestChunkAssemblerTimesOutIncompleteSequence(t *testing.T) {
	a, p := newProbeAssembler(50 * time.Millisecond)
	a.handle(chunk("vless", 443, full, 1, 1, 2, []*nodev1.UserEntry{ue(1)}, nil))
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, lost := p.snapshot(); len(lost) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("incomplete sequence did not time out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 超时后迟到的剩余分片不能拼成半个列表
	a.handle(chunk("vless", 443, full, 1, 2, 2, []*nodev1.UserEntry{ue(2)}, nil))
	got, lost := p.snapshot()
	if len(got) != 0 || len(lost) != 1 {
		t.Fatalf("late chunk: delivered=%+v lost=%v", got, lost)
	}
}

// 流断开：清空缓冲，不再分发也不再超时通知
func TestChunkAssemblerResetClearsBuffers(t *testing.T) {
	a, p := newProbeAssembler(50 * time.Millisecond)
	a.handle(chunk("vless", 443, full, 1, 1, 2, []*nodev1.UserEntry{ue(1)}, nil))
	a.reset()
	time.Sleep(150 * time.Millisecond)
	a.handle(chunk("vless", 443, full, 3, 1, 2, []*nodev1.UserEntry{ue(7)}, nil))
	a.handle(chunk("vless", 443, full, 3, 2, 2, []*nodev1.UserEntry{ue(8)}, nil))
	got, lost := p.snapshot()
	if len(lost) != 0 {
		t.Fatalf("reset buffers must not report loss: %v", lost)
	}
	if len(got) != 1 || !sameInts(ids(got[0].Users), 7, 8) {
		t.Fatalf("delivered = %+v", got)
	}
}
