package logreport

import "sync"

// Ring 是并发安全的定长环形缓冲，保存最近的日志行。
type Ring struct {
	mu    sync.Mutex
	buf   []string
	start int // 最旧一行的下标
	size  int
}

// NewRing 创建容量为 n 的环形缓冲（n<=0 时按 1 处理）。
func NewRing(n int) *Ring {
	if n <= 0 {
		n = 1
	}
	return &Ring{buf: make([]string, n)}
}

// Add 追加一行，满时覆盖最旧的一行。
func (r *Ring) Add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size < len(r.buf) {
		r.buf[(r.start+r.size)%len(r.buf)] = line
		r.size++
		return
	}
	r.buf[r.start] = line
	r.start = (r.start + 1) % len(r.buf)
}

// Last 返回最近 n 行（按时间从旧到新），返回的是副本。
func (r *Ring) Last(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.size {
		n = r.size
	}
	if n < 0 {
		n = 0
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = r.buf[(r.start+r.size-n+i)%len(r.buf)]
	}
	return out
}
