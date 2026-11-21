package panel

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/npanel-dev/npnode/conf"
)

func TestGetUserListStreamsUsersAndReusesETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == "users-v1" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", "users-v1")
		fmt.Fprint(w, `{"users":[{"id":7,"uuid":"first","speed_limit":10,"device_limit":2},{"id":8,"uuid":"second"}]}`)
	}))
	defer server.Close()
	client, err := NewClientV1(&conf.NodeApiConfig{APIHost: server.URL, NodeType: "vless", NodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	users, err := client.GetUserList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users[0] != (UserInfo{Id: 7, Uuid: "first", SpeedLimit: 10, DeviceLimit: 2}) || users[1].Id != 8 {
		t.Fatalf("users = %+v", users)
	}
	users, err = client.GetUserList(context.Background())
	if err != nil || users != nil {
		t.Fatalf("unchanged users = %+v, error = %v", users, err)
	}
}

func TestGetUserListRejectsMalformedUsers(t *testing.T) {
	for _, body := range []string{`{"users":{}}`, `{"users":[{"id":7}`, `{"users":[{"id":"wrong"}]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			client, err := NewClientV1(&conf.NodeApiConfig{APIHost: server.URL, NodeType: "vless", NodeID: 1})
			if err != nil {
				t.Fatal(err)
			}
			if users, err := client.GetUserList(context.Background()); err == nil {
				t.Fatalf("accepted malformed users: %+v", users)
			}
		})
	}
}
