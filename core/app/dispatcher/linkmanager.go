package dispatcher

import (
	"strings"
	sync "sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	// release 活跃连接表（online_v1）与准入会话表（admission_v1）的释放函数，未登记时为空。可重复调用，只生效一次。
	// 准入保活被拒时会从其他 goroutine 关闭这条链路，所以用原子指针
	release atomic.Pointer[func()]
	// ip 链路的来源 IP（与 CheckLimit 同一来源，已去掉 IPv4 映射前缀），CloseByIP 按它匹配
	ip string
}

// normalizeLinkIP 与限速器、连接表的 IP 口径一致：去掉 IPv4 映射前缀
func normalizeLinkIP(ip string) string {
	return strings.TrimPrefix(ip, "::ffff:")
}

// releaseOnline 从活跃连接表注销这条链路
func (w *ManagedWriter) releaseOnline() {
	if p := w.release.Load(); p != nil && *p != nil {
		(*p)()
	}
}

func (w *ManagedWriter) setRelease(release func()) {
	if release != nil {
		w.release.Store(&release)
	}
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	w.releaseOnline()
	return common.Close(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.links[writer] = reader
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

// CloseAll 断开该用户的全部连接。先在锁内取出并清空连接表，再在锁外逐个关闭，
// 避免关闭时回调 RemoveWriter 重入加锁，以及和新连接并发修改 map。
func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	links := m.links
	m.links = make(map[*ManagedWriter]buf.Reader)
	m.mu.Unlock()

	for w, r := range links {
		common.Close(w.writer)
		common.Interrupt(r)
		w.releaseOnline()
	}
}

// CloseByIP 断开该用户来自 ip 的全部链路（准入保活被拒，A2）。与 CloseAll 不同，不置 closed：
// 用户仍在、仍可从其他 IP 连接，之后的新链路照常纳入管理
func (m *LinkManager) CloseByIP(ip string) {
	ip = normalizeLinkIP(ip)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	victims := make(map[*ManagedWriter]buf.Reader)
	for w, r := range m.links {
		if w.ip == ip {
			victims[w] = r
			delete(m.links, w)
		}
	}
	m.mu.Unlock()

	for w, r := range victims {
		common.Close(w.writer)
		common.Interrupt(r)
		w.releaseOnline()
	}
}

// CloseWriter 断开一条链路（准入保活被拒时由准入会话表调用，A2）：从管理表移除、关闭写端、打断读端、释放登记。
// 链路还没加入（或管理器已 CloseAll）时用 fallback 作为读端
func (m *LinkManager) CloseWriter(w *ManagedWriter, fallback buf.Reader) {
	m.mu.Lock()
	r, ok := m.links[w]
	if ok {
		delete(m.links, w)
	}
	m.mu.Unlock()
	if !ok {
		r = fallback
	}
	common.Close(w.writer)
	if r != nil {
		common.Interrupt(r)
	}
	w.releaseOnline()
}

// Len 管理中的链路数（排查、测试）
func (m *LinkManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.links)
}
