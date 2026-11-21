package node

import (
	"context"
	"errors"
	"sync"

	"github.com/npanel-dev/npnode/api/panel"
	log "github.com/sirupsen/logrus"
)

type trafficDestination struct {
	host, secret, protocol string
	id, port               int
}

type trafficBatch struct {
	client *panel.ClientV1
	users  map[int]panel.UserTraffic
}

// Failed requests are retried with at-least-once delivery: the panel API does
// not provide idempotency keys. Pending bytes are removed only after an ACK.
type trafficQueue struct {
	mu       sync.Mutex
	reportMu sync.Mutex
	pending  map[trafficDestination]*trafficBatch
}

func newTrafficQueue() *trafficQueue {
	return &trafficQueue{pending: make(map[trafficDestination]*trafficBatch)}
}

func (q *trafficQueue) add(client *panel.ClientV1, traffic []panel.UserTraffic) {
	if len(traffic) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	key := trafficDestination{client.APIHost, client.SecretKey, client.NodeType, client.NodeId, client.NodePort}
	batch := q.pending[key]
	if batch == nil {
		batch = &trafficBatch{users: make(map[int]panel.UserTraffic)}
		q.pending[key] = batch
	}
	batch.client = client
	for _, delta := range traffic {
		value := batch.users[delta.UID]
		value.UID = delta.UID
		value.Upload += delta.Upload
		value.Download += delta.Download
		batch.users[delta.UID] = value
	}
}

func (q *trafficQueue) report(ctx context.Context) error {
	q.reportMu.Lock()
	defer q.reportMu.Unlock()
	// Detach each batch so forwarding/collection need not wait for HTTP. Bytes
	// collected while a request is in flight remain in a separate pending batch.
	q.mu.Lock()
	batches := q.pending
	q.pending = make(map[trafficDestination]*trafficBatch)
	q.mu.Unlock()
	var result error
	for _, batch := range batches {
		traffic := make([]panel.UserTraffic, 0, len(batch.users))
		for _, value := range batch.users {
			traffic = append(traffic, value)
		}
		if err := batch.client.ReportUserTraffic(ctx, &traffic); err != nil {
			q.add(batch.client, traffic)
			result = errors.Join(result, err)
		} else {
			log.WithFields(log.Fields{
				"protocol":  batch.client.NodeType,
				"server_id": batch.client.NodeId,
				"port":      batch.client.NodePort,
			}).Infof("已上报 %d 名用户消耗流量", len(traffic))
		}
	}
	return result
}
