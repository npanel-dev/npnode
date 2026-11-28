package limiter

import (
	"sort"
	"sync"

	"github.com/npanel-dev/npnode/api/panel"
)

// MaxOnlineIPsPerUID 同一个 uid 每次最多上报的 IP 数（O4）
const MaxOnlineIPsPerUID = 64

// ConnTable 活跃连接表（online_v1，O1）：按 (uid, ip) 记录此刻仍活跃的链路数，外加“本周期出现过”标记。
// 每条链路建立时 Open 加一，关闭时调用返回的 release 减一；每个上报周期 Snapshot 一次：
// 上报计数 >0（长连接、空闲连接）或本周期出现过（建立又关闭的短连接）的全部 (uid, ip)，然后只清标记、不清计数。
// 链路是 xray 的每次 dispatch，一条 mux / XUDP 连接会产生多条，计数不等于 TCP 连接数，这不影响在线判断。
type ConnTable struct {
	mu sync.Mutex
	m  map[connKey]*connEntry
}

type connKey struct {
	uid int
	ip  string
}

type connEntry struct {
	active int
	seen   bool
}

func NewConnTable() *ConnTable {
	return &ConnTable{m: make(map[connKey]*connEntry)}
}

// Open 登记一条链路，返回它的释放函数。释放函数可以被多个关闭钩子（连接关闭、上下文结束、CloseAll）
// 并发、重复调用，只生效一次。
func (t *ConnTable) Open(uid int, ip string) (release func()) {
	k := connKey{uid: uid, ip: ip}
	t.mu.Lock()
	e := t.m[k]
	if e == nil {
		e = &connEntry{}
		t.m[k] = e
	}
	e.active++
	e.seen = true
	t.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { t.close(k) })
	}
}

func (t *ConnTable) close(k connKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.m[k]
	if e == nil || e.active <= 0 {
		return
	}
	e.active--
	// 计数归零不立即删除：这条链路本周期内在线过，下一次 Snapshot 还要上报一次
	e.seen = true
}

// Snapshot 返回此刻在线的 (uid, ip)：计数 >0 或本周期出现过；同一 uid 最多 maxPerUID 个，活跃的优先。
// 结果按 uid、ip 排序，没有在线时为非 nil 的空切片（上报空列表，O5）。
// 之后清掉“本周期出现过”标记，删除已无活跃链路的条目。
func (t *ConnTable) Snapshot(maxPerUID int) []panel.OnlineUser {
	type item struct {
		key    connKey
		active bool
	}
	t.mu.Lock()
	items := make([]item, 0, len(t.m))
	for k, e := range t.m {
		if e.active > 0 || e.seen {
			items = append(items, item{key: k, active: e.active > 0})
		}
		if e.active > 0 {
			e.seen = false
		} else {
			delete(t.m, k)
		}
	}
	t.mu.Unlock()

	// 同一 uid 内活跃的排在前面，便于截断时保留活跃连接
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.key.uid != b.key.uid {
			return a.key.uid < b.key.uid
		}
		if a.active != b.active {
			return a.active
		}
		return a.key.ip < b.key.ip
	})
	out := make([]panel.OnlineUser, 0, len(items))
	for i := 0; i < len(items); {
		j := i
		for j < len(items) && items[j].key.uid == items[i].key.uid {
			j++
		}
		group := items[i:j]
		if maxPerUID > 0 && len(group) > maxPerUID {
			group = group[:maxPerUID]
		}
		start := len(out)
		for _, it := range group {
			out = append(out, panel.OnlineUser{UID: it.key.uid, IP: it.key.ip})
		}
		sorted := out[start:]
		sort.Slice(sorted, func(a, b int) bool { return sorted[a].IP < sorted[b].IP })
		i = j
	}
	return out
}

// Restore 上报失败时把这次 Snapshot 的结果放回：重新标记为本周期出现过，下一次 Snapshot 再报（审查 M3）。
// 已无活跃链路的条目重新建出来（计数为 0），报出后照常删除
func (t *ConnTable) Restore(users []panel.OnlineUser) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, u := range users {
		k := connKey{uid: u.UID, ip: u.IP}
		e := t.m[k]
		if e == nil {
			e = &connEntry{}
			t.m[k] = e
		}
		e.seen = true
	}
}

// Len 表内条目数（含计数归零、等待下一次上报的条目），供测试和排查
func (t *ConnTable) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

// Active 此刻活跃链路总数，供测试和排查
func (t *ConnTable) Active() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, e := range t.m {
		n += e.active
	}
	return n
}

// 连接表按入站标签放在进程级：重建限速器（重载、入站重启）时沿用，已有连接关闭时仍能对上
var (
	connTablesMu sync.Mutex
	connTables   = map[string]*ConnTable{}
)

func connTableFor(tag string) *ConnTable {
	connTablesMu.Lock()
	defer connTablesMu.Unlock()
	t := connTables[tag]
	if t == nil {
		t = NewConnTable()
		connTables[tag] = t
	}
	return t
}
