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
	log "github.com/sirupsen/logrus"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

// SOCKS / HTTP 入站。Xray 的这两种入站不能在运行中增删用户（没有 UserManager），
// 所以本机记下每个入站的用户集合，用户变化时用全部账号整个重建入站（已连接的会话会断开重连）。
// 账号：用户名和密码都是用户 UUID；会话 email 在 dispatcher 里统一成“入站标签|UUID”。

func isPlainProxy(protocolType string) bool {
	return protocolType == "socks" || protocolType == "http"
}

type plainProxyInbound struct {
	info  *panel.NodeInfo
	users map[string]bool // uuid
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
	return v.addInbound(config)
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
			log.WithFields(log.Fields{"tag": p.Tag, "uid": u.Id, "reason": reason}).Warn("用户无效，跳过")
			continue
		}
		seen[u.Uuid] = struct{}{}
		v.users.uidMap[email] = u.Id
		ib.users[u.Uuid] = true
		added = append(added, u)
	}
	v.users.mapLock.Unlock()
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
	for i := range users {
		user := format.UserTag(tag, users[i].Uuid)
		delete(ib.users, users[i].Uuid)
		v.closeUserLinksLocked(user)
		v.retireUserCounterLocked(tag, user)
	}
	v.users.mapLock.Unlock()
	// 重建失败时入站已不可用（旧入站已移除），这些用户也不在内核里了，仍按已移除处理
	return users, v.rebuildPlainProxy(tag)
}
