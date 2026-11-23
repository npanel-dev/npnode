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
	client   *panel.ClientV1
	users    map[int]panel.UserTraffic
	reportID string
	inflight []panel.UserTraffic
}

// HTTP retries provide at-least-once delivery. Sync retries preserve the report
// ID and payload for panel deduplication. Both queues survive configuration reloads.
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
	return q.reportVia(ctx, nil, nil)
}

func (q *trafficQueue) reportVia(ctx context.Context, current *panel.ClientV1, gc *panel.GrpcClient) error {
	q.reportMu.Lock()
	defer q.reportMu.Unlock()
	// Detach each batch so forwarding/collection need not wait for the panel. Bytes
	// collected while a request is in flight remain in a separate pending batch.
	q.mu.Lock()
	batches := q.pending
	q.pending = make(map[trafficDestination]*trafficBatch)
	q.mu.Unlock()
	var result error
	for key, batch := range batches {
		var traffic []panel.UserTraffic
		useSync := gc != nil && gc.IsSyncConnected() && current != nil &&
			key.host == current.APIHost && key.secret == current.SecretKey && key.id == current.NodeId
		var err error
		if useSync {
			err = batch.reportSync(gc)
		} else {
			traffic = make([]panel.UserTraffic, 0, len(batch.users))
			for _, value := range batch.users {
				traffic = append(traffic, value)
			}
			if len(traffic) > 0 {
				err = batch.client.ReportUserTraffic(ctx, &traffic)
			}
			if batch.reportID != "" {
				// Keep the possibly accepted Sync report for an idempotent retry,
				// while new collections can continue through HTTP independently.
				if err == nil {
					batch.users = make(map[int]panel.UserTraffic)
				}
				err = errors.Join(errors.New("waiting for Sync to acknowledge pending traffic"), err)
			}
		}
		if err != nil {
			if batch.reportID == "" {
				q.add(batch.client, traffic)
			} else {
				q.mu.Lock()
				if collected := q.pending[key]; collected != nil {
					for uid, delta := range collected.users {
						value := batch.users[uid]
						value.UID = uid
						value.Upload += delta.Upload
						value.Download += delta.Download
						batch.users[uid] = value
					}
				}
				q.pending[key] = batch
				q.mu.Unlock()
			}
			result = errors.Join(result, err)
		} else {
			log.WithFields(log.Fields{
				"protocol":  batch.client.NodeType,
				"server_id": batch.client.NodeId,
				"port":      batch.client.NodePort,
			}).Info("待上报流量已确认")
		}
	}
	return result
}

func (batch *trafficBatch) reportSync(gc *panel.GrpcClient) error {
	if batch.reportID != "" {
		if err := gc.SendTraffic(batch.reportID, batch.client.NodeType, batch.client.NodePort, batch.inflight); err != nil {
			return err
		}
		batch.reportID, batch.inflight = "", nil
	}
	if len(batch.users) == 0 {
		return nil
	}
	batch.inflight = make([]panel.UserTraffic, 0, len(batch.users))
	for _, value := range batch.users {
		batch.inflight = append(batch.inflight, value)
	}
	batch.users = make(map[int]panel.UserTraffic)
	batch.reportID = gc.NewTrafficReportID()
	if err := gc.SendTraffic(batch.reportID, batch.client.NodeType, batch.client.NodePort, batch.inflight); err != nil {
		return err
	}
	batch.reportID, batch.inflight = "", nil
	return nil
}
