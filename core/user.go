package core

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/counter"
	"github.com/npanel-dev/npnode/common/format"
	"github.com/npanel-dev/npnode/core/app/dispatcher"
	log "github.com/sirupsen/logrus"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/anytls"
	hyaccount "github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	"github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"github.com/xtls/xray-core/proxy/trojan"
	"github.com/xtls/xray-core/proxy/tuic"
	"github.com/xtls/xray-core/proxy/vless"
)

func (v *XrayCore) GetUserManager(tag string) (proxy.UserManager, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	handler, err := v.ihm.GetHandler(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("no such inbound tag: %s", err)
	}
	inboundInstance, ok := handler.(proxy.GetInbound)
	if !ok {
		return nil, fmt.Errorf("handler %s is not implement proxy.GetInbound", tag)
	}
	userManager, ok := inboundInstance.GetInbound().(proxy.UserManager)
	if !ok {
		return nil, fmt.Errorf("handler %s is not implement proxy.UserManager", tag)
	}
	return userManager, nil
}

// DelUsers 逐条从内核移除用户，返回实际移除的用户（内核里本来就没有的也算已移除）。
// 单条失败只记录并继续，失败的用户仍在内核里，调用方不应把它从本地记录中去掉。
// 被移除用户尚未上报的流量（含连接关闭过程中仍在写入的字节）保留到下一次上报，以原 id 上报。
func (vc *XrayCore) DelUsers(users []panel.UserInfo, tag string, info *panel.NodeInfo) ([]panel.UserInfo, error) {
	if info != nil && isPlainProxy(info.Type) {
		return vc.delPlainProxyUsers(users, tag)
	}
	userManager, err := vc.GetUserManager(tag)
	if err != nil {
		return nil, fmt.Errorf("get user manager error: %s", err)
	}
	vc.users.mapLock.Lock()
	defer vc.users.mapLock.Unlock()
	removed := make([]panel.UserInfo, 0, len(users))
	var errs []error
	for i := range users {
		user := format.UserTag(tag, users[i].Uuid)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := userManager.RemoveUser(ctx, user)
		cancel()
		if err != nil && !isUserNotFound(err) {
			log.WithFields(log.Fields{"tag": tag, "uid": users[i].Id, "err": err}).Warn("从内核移除用户失败，跳过")
			errs = append(errs, fmt.Errorf("remove user %d: %w", users[i].Id, err))
			continue
		}
		vc.closeUserLinksLocked(user)
		vc.retireUserCounterLocked(tag, user)
		removed = append(removed, users[i])
	}
	return removed, errors.Join(errs...)
}

// isUserNotFound 内核里没有这个用户（各协议的 UserManager 报错文本不同，统一按 not found 判断）
func isUserNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// closeUserLinksLocked 断开用户的全部连接（调用方持有 users.mapLock）
func (vc *XrayCore) closeUserLinksLocked(user string) {
	if v, ok := vc.dispatcher.LinkManagers.Load(user); ok {
		v.(*dispatcher.LinkManager).CloseAll()
		vc.dispatcher.LinkManagers.Delete(user)
	}
}

// retireUserCounterLocked 用户被移除：计数器移出计数表，但连同 uid 留到下一次上报时读出（调用方持有 users.mapLock）。
// 连接持有计数器指针，关闭过程中仍可能写入，所以不在这里清零，而是在上报时一次读出。
func (vc *XrayCore) retireUserCounterLocked(tag, user string) {
	uid := vc.users.uidMap[user]
	delete(vc.users.uidMap, user)
	v, ok := vc.dispatcher.Counter.Load(tag)
	if !ok {
		return
	}
	tc := v.(*counter.TrafficCounter)
	if storage, ok := tc.Counters.Load(user); ok {
		if uid > 0 {
			vc.users.retired[tag] = append(vc.users.retired[tag], retiredCounter{uid: uid, storage: storage.(*counter.TrafficStorage)})
		}
		tc.Delete(user)
	}
}

// GetUserTrafficSlice 读出并清零各用户流量。已移除用户的残留流量不受上报阈值限制，读出后按原 id 合并上报。
func (vc *XrayCore) GetUserTrafficSlice(tag string, mintraffic int) ([]panel.UserTraffic, error) {
	trafficSlice := make([]panel.UserTraffic, 0)
	vc.users.mapLock.Lock()
	defer vc.users.mapLock.Unlock()
	if v, ok := vc.dispatcher.Counter.Load(tag); ok {
		c := v.(*counter.TrafficCounter)
		c.Counters.Range(func(key, value interface{}) bool {
			email := key.(string)
			traffic := value.(*counter.TrafficStorage)
			up := traffic.UpCounter.Load()
			down := traffic.DownCounter.Load()
			if up+down > int64(mintraffic) {
				// Drain atomically so forwarding can keep adding bytes during collection.
				up = traffic.UpCounter.Swap(0)
				down = traffic.DownCounter.Swap(0)
				if vc.users.uidMap[email] == 0 {
					c.Delete(email)
					return true
				}
				trafficSlice = append(trafficSlice, panel.UserTraffic{
					UID:      vc.users.uidMap[email],
					Upload:   up,
					Download: down,
				})
			}
			return true
		})
	}
	if retired := vc.users.retired[tag]; len(retired) > 0 {
		delete(vc.users.retired, tag)
		trafficSlice = mergeRetiredTraffic(trafficSlice, retired)
	}
	if len(trafficSlice) == 0 {
		return nil, nil
	}
	return trafficSlice, nil
}

// mergeRetiredTraffic 把已移除用户的残留流量并入本次上报；同一 id（如先删后加、重置名额换 uuid）合并为一条
func mergeRetiredTraffic(traffic []panel.UserTraffic, retired []retiredCounter) []panel.UserTraffic {
	index := make(map[int]int, len(traffic))
	for i, t := range traffic {
		index[t.UID] = i
	}
	for _, r := range retired {
		up := r.storage.UpCounter.Swap(0)
		down := r.storage.DownCounter.Swap(0)
		if up+down == 0 {
			continue
		}
		if i, ok := index[r.uid]; ok {
			traffic[i].Upload += up
			traffic[i].Download += down
			continue
		}
		index[r.uid] = len(traffic)
		traffic = append(traffic, panel.UserTraffic{UID: r.uid, Upload: up, Download: down})
	}
	return traffic
}

// AddUsers 逐条把用户加入内核，返回实际加入的用户。
// 单条无效（id<=0、uuid 为空、同批 uuid 重复、已在本地记录中、SS2022 uuid 长度不足）或内核拒绝时只跳过这一条并记日志，
// 其余照常加入；只为加入成功的用户记录 uid，失败的不记入，下一轮同步会重试。
// 只有整个入站不可用（找不到入站、协议不支持）时才返回错误。
func (v *XrayCore) AddUsers(p *AddUsersParams) (added []panel.UserInfo, err error) {
	if p.NodeInfo != nil && isPlainProxy(p.NodeInfo.Type) {
		return v.addPlainProxyUsers(p)
	}
	if p.NodeInfo == nil {
		return nil, fmt.Errorf("node info is required")
	}
	switch p.NodeInfo.Type {
	case "vmess", "vless", "trojan", "shadowsocks", "hysteria2", "hysteria", "tuic", "anytls":
	default:
		return nil, fmt.Errorf("unsupported node type: %s", p.NodeInfo.Type)
	}
	man, err := v.GetUserManager(p.Tag)
	if err != nil {
		return nil, fmt.Errorf("get user manager error: %s", err)
	}
	v.users.mapLock.Lock()
	defer v.users.mapLock.Unlock()
	added = make([]panel.UserInfo, 0, len(p.Users))
	seen := make(map[string]struct{}, len(p.Users))
	for i := range p.Users {
		u := p.Users[i]
		email := format.UserTag(p.Tag, u.Uuid)
		if reason := v.invalidUserReason(p, u, email, seen); reason != "" {
			log.WithFields(log.Fields{"tag": p.Tag, "uid": u.Id, "reason": reason}).Warn("用户无效，跳过")
			continue
		}
		seen[u.Uuid] = struct{}{}
		mUser, err := buildUser(p, &u).ToMemoryUser()
		if err == nil {
			// 先记 uid 再加入内核：加入后立刻产生的流量也能对上 uid
			v.users.uidMap[email] = u.Id
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err = man.AddUser(ctx, mUser)
			cancel()
			if err != nil {
				delete(v.users.uidMap, email)
			}
		}
		if err != nil {
			log.WithFields(log.Fields{"tag": p.Tag, "uid": u.Id, "err": err}).Warn("用户加入内核失败，跳过")
			continue
		}
		added = append(added, u)
	}
	return added, nil
}

// invalidUserReason 加入内核前的校验，返回空字符串表示有效（调用方持有 users.mapLock）
func (v *XrayCore) invalidUserReason(p *AddUsersParams, u panel.UserInfo, email string, seen map[string]struct{}) string {
	switch {
	case u.Id <= 0:
		return "id 无效"
	case u.Uuid == "":
		return "uuid 为空"
	}
	if _, dup := seen[u.Uuid]; dup {
		return "同一批中 uuid 重复"
	}
	if _, exists := v.users.uidMap[email]; exists {
		return "uuid 已在本入站中"
	}
	if p.NodeInfo.Type == "shadowsocks" && p.Protocol != nil {
		if n := ss2022KeyLength(p.Protocol.Cipher); n > 0 && len(u.Uuid) < n {
			return fmt.Sprintf("uuid 长度不足 %d，无法派生 SS2022 密钥", n)
		}
	}
	return ""
}

// buildUser 按协议生成一个内核用户（类型已由调用方校验）
func buildUser(p *AddUsersParams, u *panel.UserInfo) *protocol.User {
	switch p.NodeInfo.Type {
	case "vmess":
		return buildVmessUser(p.Tag, u)
	case "vless":
		flow := ""
		if p.Protocol != nil {
			flow = p.Protocol.Flow
		}
		return buildVlessUser(p.Tag, u, flow)
	case "trojan":
		return buildTrojanUser(p.Tag, u)
	case "shadowsocks":
		cipher, serverKey := "", ""
		if p.Protocol != nil {
			cipher, serverKey = p.Protocol.Cipher, p.Protocol.ServerKey
		}
		return buildSSUser(p.Tag, u, cipher, serverKey)
	case "hysteria2", "hysteria":
		return buildHysteria2User(p.Tag, u)
	case "tuic":
		return buildTuicUser(p.Tag, u)
	default: // anytls
		return buildAnyTLSUser(p.Tag, u)
	}
}

// ss2022KeyLength SS2022 用户密钥长度（字节），非 2022 加密返回 0
func ss2022KeyLength(cipher string) int {
	switch cipher {
	case "2022-blake3-aes-128-gcm":
		return 16
	case "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305":
		return 32
	}
	return 0
}

func buildVmessUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	vmessAccount := &conf.VMessAccount{
		ID:       userInfo.Uuid,
		Security: "auto",
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(vmessAccount.Build()),
	}
}

func buildVlessUser(tag string, userInfo *panel.UserInfo, flow string) (user *protocol.User) {
	vlessAccount := &vless.Account{
		Id: userInfo.Uuid,
	}
	vlessAccount.Flow = flow
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(vlessAccount),
	}
}

func buildTrojanUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	trojanAccount := &trojan.Account{
		Password: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(trojanAccount),
	}
}

func buildSSUser(tag string, userInfo *panel.UserInfo, cypher string, serverKey string) (user *protocol.User) {
	if !strings.Contains(cypher, "2022") {
		ssAccount := &shadowsocks.Account{
			Password:   userInfo.Uuid,
			CipherType: getCipherFromString(cypher),
		}
		return &protocol.User{
			Level:   0,
			Email:   format.UserTag(tag, userInfo.Uuid),
			Account: serial.ToTypedMessage(ssAccount),
		}
	} else {
		keyLength := ss2022KeyLength(cypher)
		if len(userInfo.Uuid) < keyLength {
			// 防御：AddUsers 已校验长度，这里不能因切片越界 panic
			keyLength = len(userInfo.Uuid)
		}
		ssAccount := &shadowsocks_2022.Account{
			Key: base64.StdEncoding.EncodeToString([]byte(userInfo.Uuid[:keyLength])),
		}
		return &protocol.User{
			Level:   0,
			Email:   format.UserTag(tag, userInfo.Uuid),
			Account: serial.ToTypedMessage(ssAccount),
		}
	}
}

func getCipherFromString(c string) shadowsocks.CipherType {
	switch strings.ToLower(c) {
	case "aes-128-gcm", "aead_aes_128_gcm":
		return shadowsocks.CipherType_AES_128_GCM
	case "aes-256-gcm", "aead_aes_256_gcm":
		return shadowsocks.CipherType_AES_256_GCM
	case "chacha20-poly1305", "aead_chacha20_poly1305", "chacha20-ietf-poly1305":
		return shadowsocks.CipherType_CHACHA20_POLY1305
	default:
		return shadowsocks.CipherType_UNKNOWN
	}
}

func buildHysteria2User(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	hysteria2Account := &hyaccount.Account{
		Auth: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(hysteria2Account),
	}
}

func buildTuicUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	tuicAccount := &tuic.Account{
		Uuid:     userInfo.Uuid,
		Password: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(tuicAccount),
	}
}

func buildAnyTLSUser(tag string, userInfo *panel.UserInfo) (user *protocol.User) {
	anyTLSAccount := &anytls.Account{
		Password: userInfo.Uuid,
	}
	return &protocol.User{
		Level:   0,
		Email:   format.UserTag(tag, userInfo.Uuid),
		Account: serial.ToTypedMessage(anyTLSAccount),
	}
}
