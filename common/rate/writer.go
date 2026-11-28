package rate

import (
	"sync/atomic"

	"github.com/juju/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// Bucket 用户限速 bucket 的间接层（U5）：每个用户一个，连接的写端持有它而不是具体的 bucket，
// 原地更新限速时替换里面的 bucket，已有连接下一次写入即按新速率；为 nil 表示不限速
type Bucket struct {
	p atomic.Pointer[ratelimit.Bucket]
}

// NewBucket 以 b 为初始值建立间接层
func NewBucket(b *ratelimit.Bucket) *Bucket {
	r := &Bucket{}
	r.p.Store(b)
	return r
}

// Load 当前的 bucket，nil 表示不限速
func (b *Bucket) Load() *ratelimit.Bucket { return b.p.Load() }

// Store 替换 bucket，nil 表示不限速
func (b *Bucket) Store(v *ratelimit.Bucket) { b.p.Store(v) }

// Rate 当前速率（字节每秒），不限速时为 0
func (b *Bucket) Rate() float64 {
	if cur := b.Load(); cur != nil {
		return cur.Rate()
	}
	return 0
}

// Capacity 当前容量（字节），不限速时为 0
func (b *Bucket) Capacity() int64 {
	if cur := b.Load(); cur != nil {
		return cur.Capacity()
	}
	return 0
}

// Wait 按当前 bucket 等待 n 个令牌；不限速时立即返回
func (b *Bucket) Wait(n int64) {
	if cur := b.Load(); cur != nil {
		cur.Wait(n)
	}
}

type Writer struct {
	writer  buf.Writer
	limiter *Bucket
}

func NewRateLimitWriter(writer buf.Writer, limiter *Bucket) buf.Writer {
	return &Writer{
		writer:  writer,
		limiter: limiter,
	}
}

func (w *Writer) Close() error {
	return common.Close(w.writer)
}

// WriteMultiBuffer 每次写入都读取最新的 bucket
func (w *Writer) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.limiter.Wait(int64(mb.Len()))
	return w.writer.WriteMultiBuffer(mb)
}
