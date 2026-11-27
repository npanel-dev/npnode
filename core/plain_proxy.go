package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

// SOCKS / HTTP 入站。Xray 的这两种入站不能在运行中增删用户（没有 UserManager，账号表是配置里不加锁的 map，
// 运行中改写会与握手时的读取竞争），所以本机记下每个入站的用户集合，账号集合实际变化时用全部账号整个重建入站。
// 重建只替换监听：已建立的 TCP 会话不受影响（见 TestPlainProxyRebuildKeepsOtherUsersConnections），
// 但重建期间监听短暂关闭，SOCKS 的 UDP 会话会中断；集合没有变化时不重建。
// 账号：用户名和密码都是用户 UUID；会话 email 在 dispatcher 里统一成“入站标签|UUID”。

func isPlainProxy(protocolType string) bool {
	return protocolType == "socks" || protocolType == "http"
}

type plainProxyInbound struct {
	info  *panel.NodeInfo
	users map[string]bool // uuid
	// broken 上一次重建失败（旧入站已移除，新入站没建起来或没监听）：下一次增删无论集合是否变化都重建
	broken bool
}

type plainProxyRegistry struct {
	mu       sync.Mutex
	inbounds map[string]*plainProxyInbound // tag →
}

var plainProxies = &plainProxyRegistry{inbounds: map[string]*plainProxyInbound{}}

// placeholderAccount 没有用户时放一个随机账号：HTTP 入站在没有账号时不校验身份，会变成开放代理
func placeholderAccount() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "npnode-empty-" + hex.EncodeToString(b)
}

// buildPlainProxy 生成 SOCKS / HTTP 入站设置；uuids 为当前全部用户
func buildPlainProxy(nodeInfo *panel.NodeInfo, in *coreConf.InboundDetourConfig, uuids []string) error {
	if len(uuids) == 0 {
		uuids = []string{placeholderAccount()}
	}
	var settings any
	switch nodeInfo.Type {
	case "socks":
		accounts := make([]map[string]string, 0, len(uuids))
		for _, u := range uuids {
			accounts = append(accounts, map[string]string{"user": u, "pass": u})
		}
		in.Protocol = "socks"
		settings = map[string]any{"auth": "password", "accounts": accounts, "udp": true}
	case "http":
		accounts := make([]map[string]string, 0, len(uuids))
		for _, u := range uuids {
			accounts = append(accounts, map[string]string{"user": u, "pass": u})
		}
		in.Protocol = "http"
		settings = map[string]any{"accounts": accounts, "allowTransparent": false}
	default:
		return fmt.Errorf("not a plain proxy: %s", nodeInfo.Type)
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	msg := json.RawMessage(raw)
	in.Settings = &msg
	return nil
}

func (r *plainProxyRegistry) uuids(tag string) []string {
	ib := r.inbounds[tag]
	if ib == nil {
		return nil
	}
	out := make([]string, 0, len(ib.users))
	for u := range ib.users {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// rebuildPlainProxy 用当前全部账号重建入站（调用方持有 plainProxies.mu）
func (v *XrayCore) rebuildPlainProxy(tag string) error {
	ib := plainProxies.inbounds[tag]
	if ib == nil {
		return fmt.Errorf("no plain proxy inbound %s", tag)
	}
	config, err := buildInboundWithUsers(ib.info, tag, plainProxies.uuids(tag))
	if err != nil {
		return err
	}
	_ = v.removeInbound(tag)
	err = v.addInbound(config)
	ib.broken = err != nil
	return err
}

// addPlainProxyUsers 校验后把用户并入账号集合并重建入站；单条无效只跳过。重建失败时撤回本次新增的用户。
func (v *XrayCore) addPlainProxyUsers(p *AddUsersParams) ([]panel.UserInfo, error) {
	plainProxies.mu.Lock()
	defer plainProxies.mu.Unlock()
	ib := plainProxies.inbounds[p.Tag]
	if ib == nil {
		return nil, fmt.Errorf("no plain proxy inbound %s", p.Tag)
	}
	v.users.mapLock.Lock()
	added := make([]panel.UserInfo, 0, len(p.Users))
	seen := make(map[string]struct{}, len(p.Users))
	for i := range p.Users {
		u := p.Users[i]
		email := format.UserTag(p.Tag, u.Uuid)
		if reason := v.invalidUserReason(p, u, email, seen); reason != "" {
			p.logSkip(u, "reason", reason, "用户无效，跳过")
			continue
		}
		seen[u.Uuid] = struct{}{}
		v.users.uidMap[email] = u.Id
		ib.users[u.Uuid] = true
		added = append(added, u)
	}
	v.users.mapLock.Unlock()
	if len(added) == 0 && !ib.broken {
		// 账号集合没变：不重建（无效条目每轮同步都会被静默重试，不能每次都整表重建）
		return added, nil
	}
	if err := v.rebuildPlainProxy(p.Tag); err != nil {
		v.users.mapLock.Lock()
		for _, u := range added {
			delete(v.users.uidMap, format.UserTag(p.Tag, u.Uuid))
			delete(ib.users, u.Uuid)
		}
		v.users.mapLock.Unlock()
		return nil, err
	}
	return added, nil
}

// delPlainProxyUsers 移出账号集合并重建入站；未上报的流量保留到下一次上报（U2）
func (v *XrayCore) delPlainProxyUsers(users []panel.UserInfo, tag string) ([]panel.UserInfo, error) {
	plainProxies.mu.Lock()
	defer plainProxies.mu.Unlock()
	ib := plainProxies.inbounds[tag]
	if ib == nil {
		return nil, fmt.Errorf("no plain proxy inbound %s", tag)
	}
	v.users.mapLock.Lock()
	changed := false
	for i := range users {
		user := format.UserTag(tag, users[i].Uuid)
		if ib.users[users[i].Uuid] {
			changed = true
		}
		delete(ib.users, users[i].Uuid)
		v.closeUserLinksLocked(user)
		v.retireUserCounterLocked(tag, user)
	}
	v.users.mapLock.Unlock()
	if !changed && !ib.broken {
		// 要删的账号本来就不在集合里（与 UserManager 的 not found 一样按已移除处理）：集合没变，不重建
		return users, nil
	}
	// 重建失败时入站已不可用（旧入站已移除），这些用户也不在内核里了，仍按已移除处理
	return users, v.rebuildPlainProxy(tag)
}
