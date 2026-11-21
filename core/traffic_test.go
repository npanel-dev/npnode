package core

import (
	"sync"
	"testing"

	"github.com/npanel-dev/npnode/common/counter"
	"github.com/npanel-dev/npnode/core/app/dispatcher"
)

func trafficTestCore() (*XrayCore, *counter.TrafficStorage) {
	c := New(nil, nil)
	c.dispatcher = &dispatcher.DefaultDispatcher{}
	c.users.uidMap["user"] = 7
	counts := counter.NewTrafficCounter()
	c.dispatcher.Counter.Store("node", counts)
	return c, counts.GetCounter("user")
}

func TestTrafficThresholdUsesBytesAndRetainsSmallDeltas(t *testing.T) {
	c, storage := trafficTestCore()
	const threshold = 1024 * 1024
	storage.DownCounter.Store(threshold)
	got, _ := c.GetUserTrafficSlice("node", threshold)
	if len(got) != 0 || storage.DownCounter.Load() != threshold {
		t.Fatalf("threshold bytes must be retained: report=%+v pending=%d", got, storage.DownCounter.Load())
	}
	storage.DownCounter.Add(threshold)
	got, _ = c.GetUserTrafficSlice("node", threshold)
	if len(got) != 1 || got[0].UID != 7 || got[0].Download != 2*threshold {
		t.Fatalf("expected 2MiB above a 1MiB threshold, got %+v", got)
	}
	got, _ = c.GetUserTrafficSlice("node", 0)
	if len(got) != 0 {
		t.Fatalf("drained bytes reported twice: %+v", got)
	}
}

func TestTrafficDrainPreservesConcurrentWrites(t *testing.T) {
	c, storage := trafficTestCore()
	const writers, iterations = 8, 50000
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				storage.UpCounter.Add(1)
				storage.DownCounter.Add(2)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	var up, down int64
	drain := func() {
		values, err := c.GetUserTrafficSlice("node", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range values {
			up += value.Upload
			down += value.Download
		}
	}
	for {
		drain()
		select {
		case <-done:
			drain()
			if up != writers*iterations || down != 2*writers*iterations {
				t.Fatalf("lost traffic: up=%d down=%d", up, down)
			}
			return
		default:
		}
	}
}
