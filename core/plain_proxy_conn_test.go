package core

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/limiter"
)

func startEcho(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

// socks5Dial 通过 SOCKS5（用户名密码认证）连到 127.0.0.1:target
func socks5Dial(t *testing.T, proxyPort, target int, user string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(proxyPort), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	_, _ = c.Write([]byte{5, 1, 2})
	if _, err := io.ReadFull(c, buf[:2]); err != nil || buf[1] != 2 {
		t.Fatalf("socks greeting: %v %v", buf[:2], err)
	}
	auth := []byte{1, byte(len(user))}
	auth = append(auth, user...)
	auth = append(auth, byte(len(user)))
	auth = append(auth, user...)
	_, _ = c.Write(auth)
	if _, err := io.ReadFull(c, buf[:2]); err != nil || buf[1] != 0 {
		t.Fatalf("socks auth: %v %v", buf[:2], err)
	}
	req := []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 0}
	binary.BigEndian.PutUint16(req[8:], uint16(target))
	_, _ = c.Write(req)
	if _, err := io.ReadFull(c, buf[:10]); err != nil || buf[1] != 0 {
		t.Fatalf("socks connect: %v %v", buf[:10], err)
	}
	_ = c.SetDeadline(time.Time{})
	return c
}

func echoOK(c net.Conn, msg string) error {
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	defer c.SetDeadline(time.Time{})
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	return nil
}

// 观察：SOCKS 入站因用户变化整表重建时，其他用户已建立的 TCP 连接是否还能转发
func TestPlainProxyRebuildKeepsOtherUsersConnections(t *testing.T) {
	limiter.Init()
	info := &panel.NodeInfo{Id: 1, Type: "socks", Protocol: &panel.Protocol{Type: "socks", Port: freePort(t)}}
	vc, tag := seatTestCore(t, info)
	t.Cleanup(func() { _ = vc.DelNode(tag) })
	users := []panel.UserInfo{{Id: 1000000045, Uuid: uuidA}}
	limiter.AddLimiter(tag, users, nil)
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: users}); err != nil {
		t.Fatal(err)
	}
	echo := startEcho(t)
	conn := socks5Dial(t, info.Protocol.Port, echo, uuidA)
	defer conn.Close()
	if err := echoOK(conn, "before"); err != nil {
		t.Fatalf("echo before rebuild: %v", err)
	}
	// 新增另一个用户：触发整表重建
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 2, Uuid: uuidB}}}); err != nil {
		t.Fatal(err)
	}
	if err := echoOK(conn, "after"); err != nil {
		t.Fatalf("existing connection of an unchanged user broken by rebuild: %v", err)
	}
	// 新用户能连上
	c2 := socks5Dial(t, info.Protocol.Port, echo, uuidB)
	c2.Close()
}

func plainHandler(t *testing.T, vc *XrayCore, tag string) any {
	t.Helper()
	h, err := vc.ihm.GetHandler(context.Background(), tag)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// 用户集合没有变化（全部无效、要删的本来就不在）时不重建入站：
// 无效条目每轮同步都会被静默重试，10 万条时每次重建约 100～250 ms，且重建期间监听短暂关闭、UDP 会话中断
func TestPlainProxySkipsRebuildWithoutChange(t *testing.T) {
	limiter.Init()
	info := &panel.NodeInfo{Id: 1, Type: "socks", Protocol: &panel.Protocol{Type: "socks", Port: freePort(t)}}
	vc, tag := seatTestCore(t, info)
	t.Cleanup(func() { _ = vc.DelNode(tag) })
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 7, Uuid: uuidA}}}); err != nil {
		t.Fatal(err)
	}
	before := plainHandler(t, vc, tag)
	added, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 0, Uuid: uuidB}, {Id: 8, Uuid: uuidA}}})
	if err != nil || len(added) != 0 {
		t.Fatalf("added=%v err=%v", added, err)
	}
	if plainHandler(t, vc, tag) != before {
		t.Fatal("inbound rebuilt although no user was added")
	}
	removed, err := vc.DelUsers([]panel.UserInfo{{Id: 9, Uuid: uuidC}}, tag, info)
	if err != nil || len(removed) != 1 {
		t.Fatalf("removed=%v err=%v", removed, err)
	}
	if plainHandler(t, vc, tag) != before {
		t.Fatal("inbound rebuilt although the removed user was not present")
	}
	// 集合与整表重建结果一致
	plainProxies.mu.Lock()
	got := plainProxies.uuids(tag)
	plainProxies.mu.Unlock()
	if len(got) != 1 || got[0] != uuidA {
		t.Fatalf("accounts = %v, want [%s]", got, uuidA)
	}
}
