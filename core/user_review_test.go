package core

import (
	"context"
	"errors"
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy"
)

// 入站移除（Controller.Close → DelNode）后不再保留该入站已移除用户的计数器
func TestDelNodeDropsRetiredCounters(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 7, Uuid: uuidA}}}); err != nil {
		t.Fatal(err)
	}
	userCounter(vc, tag, uuidA).UpCounter.Add(10)
	if _, err := vc.DelUsers([]panel.UserInfo{{Id: 7, Uuid: uuidA}}, tag, info); err != nil {
		t.Fatal(err)
	}
	if err := vc.DelNode(tag); err != nil {
		t.Fatal(err)
	}
	vc.users.mapLock.RLock()
	_, left := vc.users.retired[tag]
	vc.users.mapLock.RUnlock()
	if left {
		t.Fatal("retired counters of a removed inbound must be dropped")
	}
}

// U2：SOCKS / HTTP 入站删除用户后，未上报流量同样以原 id 上报
func TestPlainProxyDeletedUserTrafficIsReported(t *testing.T) {
	info := &panel.NodeInfo{Id: 1, Type: "socks", Protocol: &panel.Protocol{Type: "socks", Port: freePort(t)}}
	vc, tag := seatTestCore(t, info)
	t.Cleanup(func() { _ = vc.DelNode(tag) })
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 1000000045, Uuid: uuidA}, {Id: 2, Uuid: uuidB}}}); err != nil {
		t.Fatal(err)
	}
	userCounter(vc, tag, uuidA).DownCounter.Add(4096)
	removed, err := vc.DelUsers([]panel.UserInfo{{Id: 1000000045, Uuid: uuidA}}, tag, info)
	if err != nil || len(removed) != 1 {
		t.Fatalf("removed=%v err=%v", removed, err)
	}
	got, _ := vc.GetUserTrafficSlice(tag, 1<<30)
	if len(got) != 1 || got[0] != (panel.UserTraffic{UID: 1000000045, Download: 4096}) {
		t.Fatalf("traffic = %+v", got)
	}
}

// failingUserManager RemoveUser 对指定 email 返回非 not found 的错误
type failingUserManager struct {
	failEmail string
	removed   []string
}

func (m *failingUserManager) AddUser(context.Context, *protocol.MemoryUser) error { return nil }
func (m *failingUserManager) RemoveUser(_ context.Context, email string) error {
	if email == m.failEmail {
		return errors.New("validator busy")
	}
	m.removed = append(m.removed, email)
	return nil
}
func (m *failingUserManager) GetUser(context.Context, string) *protocol.MemoryUser { return nil }
func (m *failingUserManager) GetUsers(context.Context) []*protocol.MemoryUser      { return nil }
func (m *failingUserManager) GetUsersCount(context.Context) int64                  { return 0 }

// U3：删除中某条返回非 not found 的错误：跳过它（仍在内核、仍记 uid）并继续删除后面的用户
func TestDelUsersContinuesPastOtherErrors(t *testing.T) {
	info := vlessInfo(t)
	vc, tag := seatTestCore(t, info)
	if _, err := vc.AddUsers(&AddUsersParams{Tag: tag, NodeInfo: info, Users: []panel.UserInfo{{Id: 1, Uuid: uuidA}, {Id: 2, Uuid: uuidB}, {Id: 3, Uuid: uuidC}}}); err != nil {
		t.Fatal(err)
	}
	fake := &failingUserManager{failEmail: format.UserTag(tag, uuidB)}
	vc.userManagerFor = func(string) (proxy.UserManager, error) { return fake, nil }
	removed, err := vc.DelUsers([]panel.UserInfo{{Id: 1, Uuid: uuidA}, {Id: 2, Uuid: uuidB}, {Id: 3, Uuid: uuidC}}, tag, info)
	if err == nil {
		t.Fatal("the failed removal must be reported")
	}
	if got := userIDs(removed); !equalInts(got, 1, 3) {
		t.Fatalf("removed = %v, want [1 3]", got)
	}
	if id, ok := vc.uidOf(tag, uuidB); !ok || id != 2 {
		t.Fatal("user whose removal failed must keep its uid")
	}
	if _, ok := vc.uidOf(tag, uuidC); ok {
		t.Fatal("user after the failed one was not removed")
	}
}

// 固定各协议内核对"用户不存在"的报错：DelUsers 靠 isUserNotFound 把它当作已移除
func TestIsUserNotFoundMatchesKernelErrors(t *testing.T) {
	for _, tc := range []struct {
		protocol string
		info     *panel.NodeInfo
	}{
		{"vless", vlessInfo(t)},
		{"vmess", &panel.NodeInfo{Id: 1, Type: "vmess", Protocol: &panel.Protocol{Type: "vmess", Port: freePort(t), Transport: "tcp"}}},
		{"trojan", &panel.NodeInfo{Id: 1, Type: "trojan", Protocol: &panel.Protocol{Type: "trojan", Port: freePort(t), Transport: "tcp"}}},
		{"shadowsocks", &panel.NodeInfo{Id: 1, Type: "shadowsocks", Protocol: &panel.Protocol{Type: "shadowsocks", Port: freePort(t), Cipher: "aes-128-gcm"}}},
		{"shadowsocks-2022", &panel.NodeInfo{Id: 1, Type: "shadowsocks", Protocol: &panel.Protocol{Type: "shadowsocks", Port: freePort(t), Cipher: "2022-blake3-aes-256-gcm", ServerKey: "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"}}},
	} {
		t.Run(tc.protocol, func(t *testing.T) {
			vc, tag := seatTestCore(t, tc.info)
			man, err := vc.GetUserManager(tag)
			if err != nil {
				t.Fatal(err)
			}
			err = man.RemoveUser(context.Background(), format.UserTag(tag, uuidC))
			if !isUserNotFound(err) {
				t.Fatalf("RemoveUser of a missing user returned %v, which isUserNotFound does not recognise", err)
			}
		})
	}
	// tuic 需要证书才能启动入站，这里固定其报错文本（proxy/tuic/inbound.go RemoveUser）
	if !isUserNotFound(errors.New("User not found: tag|uuid")) {
		t.Fatal("tuic not-found error not recognised")
	}
	for _, err := range []error{nil, errors.New("validator busy"), errors.New("Email must not be empty.")} {
		if isUserNotFound(err) {
			t.Fatalf("%v must not count as not found", err)
		}
	}
}
