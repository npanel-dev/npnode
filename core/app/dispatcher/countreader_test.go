package dispatcher

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

type tailReader struct {
	timeout time.Duration
	err     error
}

func (r *tailReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	b := buf.New()
	b.Write([]byte("tail"))
	return buf.MultiBuffer{b}, r.err
}

func (r *tailReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	r.timeout = timeout
	return r.ReadMultiBuffer()
}

func TestCounterReaderPreservesFinalBytesAndTimeout(t *testing.T) {
	for _, timed := range []bool{false, true} {
		source := &tailReader{err: io.EOF}
		count := &atomic.Int64{}
		reader := &CounterReader{Reader: source, Counter: count}
		var mb buf.MultiBuffer
		var err error
		if timed {
			mb, err = reader.ReadMultiBufferTimeout(50 * time.Millisecond)
		} else {
			mb, err = reader.ReadMultiBuffer()
		}
		if err != io.EOF || mb.Len() != 4 || count.Load() != 4 {
			t.Fatalf("final read: bytes=%d count=%d err=%v", mb.Len(), count.Load(), err)
		}
		buf.ReleaseMulti(mb)
		if timed && source.timeout != 50*time.Millisecond {
			t.Fatalf("timeout = %s", source.timeout)
		}
	}
}

func TestSniffingCachePreservesFinalPayload(t *testing.T) {
	count := &atomic.Int64{}
	reader := &cachedReader{reader: &CounterReader{Reader: &tailReader{err: io.EOF}, Counter: count}}
	sniffed := buf.New()
	defer sniffed.Release()
	if err := reader.Cache(sniffed, time.Second); err != io.EOF {
		t.Fatalf("cache error = %v", err)
	}
	mb, err := reader.ReadMultiBuffer()
	defer buf.ReleaseMulti(mb)
	if mb.Len() != 4 || err != io.EOF || count.Load() != 4 {
		t.Fatalf("cached tail: bytes=%d count=%d err=%v", mb.Len(), count.Load(), err)
	}
}

func TestSniffingTimeoutAllowsLaterReads(t *testing.T) {
	source := &tailReader{err: buf.ErrReadTimeout}
	reader := &cachedReader{reader: source}
	sniffed := buf.New()
	defer sniffed.Release()
	if err := reader.Cache(sniffed, time.Second); err != buf.ErrReadTimeout {
		t.Fatalf("cache error = %v", err)
	}
	mb, err := reader.ReadMultiBuffer()
	buf.ReleaseMulti(mb)
	if err != nil {
		t.Fatalf("temporary timeout persisted in cache: %v", err)
	}
	source.err = io.EOF
	mb, err = reader.ReadMultiBuffer()
	defer buf.ReleaseMulti(mb)
	if mb.Len() != 4 || err != io.EOF {
		t.Fatalf("later read: bytes=%d err=%v", mb.Len(), err)
	}
}
