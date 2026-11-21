package core

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	"github.com/npanel-dev/npnode/limiter"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/vless/encoding"
)

func TestVLESSLoopbackTrafficMatchesTransferredPayload(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { echo.Close() })
	go func() {
		conn, err := echo.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn)
	}()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.Addr().(*net.TCPAddr).Port
	reserved.Close()
	vc := New(conf.New(), nil)
	if err := vc.Start(&panel.ServerConfigResponse{Data: &panel.Data{IPStrategy: "prefer_ipv4", PullInterval: 3600}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { vc.Close() })
	const tag = "test-vless-traffic"
	users := []panel.UserInfo{{Id: 7, Uuid: "6e8fac3f-bf7a-4a48-b28a-43bb660e2a19"}}
	limiter.Init()
	limiter.AddLimiter(tag, users, nil)
	t.Cleanup(func() { limiter.DeleteLimiter(tag) })
	info := &panel.NodeInfo{Id: 1, Type: "vless", Protocol: &panel.Protocol{Type: "vless", Port: port, Transport: "tcp", Encryption: "none"}}
	if err := vc.AddNode(tag, info); err != nil {
		t.Fatal(err)
	}
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, Users: users, NodeInfo: info}); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", xnet.Port(port).String()), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	user, err := buildVlessUser(tag, &users[0], "").ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}
	request := &protocol.RequestHeader{
		Version: 0, Command: protocol.RequestCommandTCP,
		Address: xnet.ParseAddress("127.0.0.1"), Port: xnet.Port(echo.Addr().(*net.TCPAddr).Port), User: user,
	}
	if err := encoding.EncodeRequestHeader(conn, request, &encoding.Addons{}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("vless-accounting"), 128*1024)
	uploadDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(conn, bytes.NewReader(payload))
		uploadDone <- err
	}()
	if _, err := encoding.DecodeResponseHeader(conn, request); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, received); err != nil {
		t.Fatal(err)
	}
	if err := <-uploadDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("echo payload differs from uploaded data")
	}
	traffic, err := vc.GetUserTrafficSlice(tag, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(traffic) != 1 || traffic[0].UID != 7 || traffic[0].Upload != int64(len(payload)) || traffic[0].Download != int64(len(payload)) {
		t.Fatalf("transferred %d bytes each way, reported %+v", len(payload), traffic)
	}
}
