package rate

import (
	"testing"
	"time"

	"github.com/juju/ratelimit"
	"github.com/xtls/xray-core/common/buf"
)

type sink struct{ n int32 }

func (s *sink) WriteMultiBuffer(mb buf.MultiBuffer) error {
	s.n += mb.Len()
	buf.ReleaseMulti(mb)
	return nil
}

func mb(n int) buf.MultiBuffer {
	b := buf.New()
	b.Extend(int32(n))
	return buf.MultiBuffer{b}
}

// 已有连接的写端每次写入都读取最新的 bucket（U5）：限速改为不限后不再等待；改为新速率后按新速率等待
func TestWriterUsesLatestBucket(t *testing.T) {
	ref := NewBucket(ratelimit.NewBucketWithQuantum(time.Second, 1, 1)) // 1 字节每秒
	s := &sink{}
	w := NewRateLimitWriter(s, ref)
	_ = w.WriteMultiBuffer(mb(1)) // 用掉初始的 1 个令牌
	ref.Store(nil)                // 原地改为不限速
	done := make(chan struct{})
	go func() {
		_ = w.WriteMultiBuffer(mb(1000))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bucket 改为不限后写入仍在按旧速率等待")
	}
	ref.Store(ratelimit.NewBucketWithQuantum(time.Second, 1<<20, 1<<20))
	if ref.Capacity() != 1<<20 || int64(ref.Rate()) != 1<<20 {
		t.Fatalf("rate=%v capacity=%d", ref.Rate(), ref.Capacity())
	}
	start := time.Now()
	_ = w.WriteMultiBuffer(mb(1000))
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("新速率未生效")
	}
}
