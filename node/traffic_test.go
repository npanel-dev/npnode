package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
)

func trafficTestClient(t *testing.T, host string, port int) *panel.ClientV1 {
	t.Helper()
	client, err := panel.NewClientV1(&conf.NodeApiConfig{APIHost: host, NodeType: "vless", NodeID: 1, Port: port})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestTrafficRetriesHTTPAndBusinessFailuresAcrossReload(t *testing.T) {
	var requests []panel.ServerPushUserTrafficRequest
	var requestsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body panel.ServerPushUserTrafficRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requestsMu.Lock()
		requests = append(requests, body)
		count := len(requests)
		requestsMu.Unlock()
		switch count {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			fmt.Fprint(w, `{"code":500,"msg":"queue unavailable"}`)
		default:
			fmt.Fprint(w, `{"code":200,"msg":"success"}`)
		}
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	old := &Node{traffic: newTrafficQueue()}
	old.traffic.add(client, []panel.UserTraffic{{UID: 7, Upload: 10, Download: 20}})
	if err := old.traffic.report(context.Background()); err == nil {
		t.Fatal("expected HTTP failure")
	}
	old.traffic.add(client, []panel.UserTraffic{{UID: 7, Upload: 5, Download: 7}})
	if err := old.traffic.report(context.Background()); err == nil {
		t.Fatal("expected business failure")
	}
	next := &Node{controllers: []*Controller{{}}}
	next.InheritTraffic(old)
	if next.controllers[0].traffic != next.traffic {
		t.Fatal("controller did not inherit pending traffic")
	}
	if err := next.traffic.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := next.traffic.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(requests) != 3 {
		t.Fatalf("reports = %d, want 3", len(requests))
	}
	for _, request := range requests[1:] {
		if len(request.Traffic) != 1 || request.Traffic[0] != (panel.UserTraffic{UID: 7, Upload: 15, Download: 27}) {
			t.Fatalf("retry traffic = %+v", request.Traffic)
		}
	}
}

func TestTrafficCollectedDuringReportSurvivesAcknowledgement(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var requests []panel.ServerPushUserTrafficRequest
	var requestsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body panel.ServerPushUserTrafficRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requestsMu.Lock()
		requests = append(requests, body)
		count := len(requests)
		requestsMu.Unlock()
		if count == 1 {
			close(started)
			<-release
		}
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	client := trafficTestClient(t, server.URL, 8443)
	q := newTrafficQueue()
	q.add(client, []panel.UserTraffic{{UID: 7, Download: 10}})
	done := make(chan error, 1)
	go func() { done <- q.report(context.Background()) }()
	<-started
	q.add(client, []panel.UserTraffic{{UID: 7, Download: 5}})
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Concurrent reporters must not send the same batch twice.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.report(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(requests) != 2 || requests[0].Traffic[0].Download != 10 || requests[1].Traffic[0].Download != 5 {
		t.Fatalf("reports = %+v", requests)
	}
}

func TestTrafficDestinationsKeepSeparatePorts(t *testing.T) {
	ports := make(map[string]int64)
	var portsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body panel.ServerPushUserTrafficRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		portsMu.Lock()
		ports[r.URL.Query().Get("port")] += body.Traffic[0].Download
		portsMu.Unlock()
		fmt.Fprint(w, `{"code":200}`)
	}))
	defer server.Close()
	q := newTrafficQueue()
	q.add(trafficTestClient(t, server.URL, 8443), []panel.UserTraffic{{UID: 7, Download: 10}})
	q.add(trafficTestClient(t, server.URL, 9443), []panel.UserTraffic{{UID: 7, Download: 20}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.report(ctx); err == nil {
		t.Fatal("expected canceled report")
	}
	if err := q.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	portsMu.Lock()
	defer portsMu.Unlock()
	if len(ports) != 2 || ports["8443"] != 10 || ports["9443"] != 20 {
		t.Fatalf("traffic by port = %+v", ports)
	}
}
