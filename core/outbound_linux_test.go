package core

import (
	"encoding/binary"
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

// Linux routes all of 127.0.0.0/8 to loopback, so these connections exercise
// multiple local IPs on one wildcard listener without changing host networking.
func TestVLESSWildcardInboundPreservesEntryIP(t *testing.T) {
	const payload = "npnode-source-ip"
	tcpPort := startSourceIPEcho(t, "tcp", len(payload))
	udpPort := startSourceIPEcho(t, "udp", len(payload))
	reserved, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.Addr().(*net.TCPAddr).Port
	reserved.Close()

	config := conf.New()
	config.LogConfig.Level = "error"
	vc := New(config, nil)
	if err := vc.Start(&panel.ServerConfigResponse{Data: &panel.Data{IPStrategy: "prefer_ipv4", PullInterval: 3600}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { vc.Close() })
	const tag = "test-vless-source-ip"
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
	user, err := buildVlessUser(tag, &users[0], "").ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range []struct {
		name    string
		command protocol.RequestCommand
		port    int
	}{
		{"tcp", protocol.RequestCommandTCP, tcpPort},
		{"udp-over-tcp", protocol.RequestCommandUDP, udpPort},
	} {
		for _, entryIP := range []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"} {
			t.Run(target.name+"/"+entryIP, func(t *testing.T) {
				t.Parallel()
				conn, err := net.DialTimeout("tcp4", net.JoinHostPort(entryIP, xnet.Port(port).String()), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(10 * time.Second))
				request := &protocol.RequestHeader{
					Version: 0, Command: target.command, User: user,
					Address: xnet.LocalHostIP, Port: xnet.Port(target.port),
				}
				if err := encoding.EncodeRequestHeader(conn, request, &encoding.Addons{}); err != nil {
					t.Fatal(err)
				}
				body := []byte(payload)
				if target.command == protocol.RequestCommandUDP {
					body = append(binary.BigEndian.AppendUint16(nil, uint16(len(body))), body...)
				}
				if _, err := conn.Write(body); err != nil {
					t.Fatal(err)
				}
				if _, err := encoding.DecodeResponseHeader(conn, request); err != nil {
					t.Fatal(err)
				}
				if target.command == protocol.RequestCommandUDP {
					var size uint16
					if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
						t.Fatal(err)
					}
					if int(size) != len(entryIP) {
						t.Fatalf("response length = %d, want %d", size, len(entryIP))
					}
				}
				observed := make([]byte, len(entryIP))
				if _, err := io.ReadFull(conn, observed); err != nil {
					t.Fatal(err)
				}
				if string(observed) != entryIP {
					t.Fatalf("connected to entry %s, destination observed source %s", entryIP, observed)
				}
			})
		}
	}
}

func startSourceIPEcho(t *testing.T, network string, payloadSize int) int {
	t.Helper()
	if network == "tcp" {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(10 * time.Second))
					if _, err := io.ReadFull(conn, make([]byte, payloadSize)); err == nil {
						io.WriteString(conn, conn.RemoteAddr().(*net.TCPAddr).IP.String())
					}
				}()
			}
		}()
		return listener.Addr().(*net.TCPAddr).Port
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	go func() {
		payload := make([]byte, payloadSize)
		for {
			_, addr, err := conn.ReadFromUDP(payload)
			if err != nil {
				return
			}
			conn.WriteToUDP([]byte(addr.IP.String()), addr)
		}
	}()
	return conn.LocalAddr().(*net.UDPAddr).Port
}
