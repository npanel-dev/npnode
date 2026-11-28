package node

import (
	"strconv"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
	vCore "github.com/npanel-dev/npnode/core"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
)

// pushFullCheckInterval 推送正常时，定时拉取改为每隔这么久做一次全量校对
const pushFullCheckInterval = 5 * time.Minute

// userSyncKey 用户在内核里的身份：UUID、限速、设备数任一变化都要删旧加新。
// （原先只比 UUID + 限速，只改设备数上限不会生效。）
func userSyncKey(u panel.UserInfo) string {
	return u.Uuid + "|" + strconv.Itoa(u.SpeedLimit) + "|" + strconv.Itoa(u.DeviceLimit)
}

// pushHealthyLocked 推送是否可用：已收到全量且流在线。调用方持有 userMu。
func (c *Controller) pushHealthyLocked() bool {
	return c.syncVersion > 0 && c.grpcClient != nil && c.grpcClient.IsSyncConnected()
}

// applyUserSetLocked 把内核用户同步成 users（全量）。调用方持有 userMu。
// 返回实际新增、删除的数量；单条失败的用户不计入，下一轮会重试。
func (c *Controller) applyUserSetLocked(users []panel.UserInfo) (added, deleted int, err error) {
	del, add := compareUserList(c.userList, users)
	added, deleted, err = c.applyChangesLocked(del, add)
	c.pruneRejectedLocked(users)
	return added, deleted, err
}

// rejectKey 无效条目记忆的键：id 与内核身份键（uuid、限速、设备数）都相同才算同一内容
func rejectKey(u panel.UserInfo) string {
	return strconv.Itoa(u.Id) + "|" + userSyncKey(u)
}

// isRejectedLocked 这条内容之前是否已加入失败并告警过（调用方持有 userMu，或在 Start 中单线程调用）
func (c *Controller) isRejectedLocked(u panel.UserInfo) bool {
	_, ok := c.rejected[rejectKey(u)]
	return ok
}

// recordRejectedLocked 按一次加入的结果更新记忆：失败的记下，成功的清掉。失败的条目之后仍会被静默重试
// （例如同批 uuid 重复的那条，在另一条被移除后就能加入）
func (c *Controller) recordRejectedLocked(tried, added []panel.UserInfo) {
	ok := make(map[string]int, len(added))
	for _, u := range added {
		ok[rejectKey(u)]++
	}
	for _, u := range tried {
		k := rejectKey(u)
		if ok[k] > 0 {
			ok[k]--
			delete(c.rejected, k)
			continue
		}
		if c.rejected == nil {
			c.rejected = make(map[string]struct{})
		}
		c.rejected[k] = struct{}{}
	}
}

// pruneRejectedLocked 全量对齐后只保留仍在面板列表里的记忆：内容变化或被移除的条目清掉
func (c *Controller) pruneRejectedLocked(users []panel.UserInfo) {
	if len(c.rejected) == 0 {
		return
	}
	keep := make(map[string]struct{}, len(c.rejected))
	for _, u := range users {
		k := rejectKey(u)
		if _, ok := c.rejected[k]; ok {
			keep[k] = struct{}{}
		}
	}
	c.rejected = keep
}

// forgetRejectedLocked 面板差量移除了这些条目：清掉它们的记忆
func (c *Controller) forgetRejectedLocked(removed []panel.UserInfo) {
	for _, u := range removed {
		delete(c.rejected, rejectKey(u))
	}
}

// applyChangesLocked 先删后加，并保证 c.userList 始终与内核实际状态一致：
// 只把内核里实际移除、实际加入的用户移出 / 并入 c.userList 和限速器。
// 单条失败只跳过（内核已记日志），失败的用户不记入本地，下一轮同步会重试，也不会反复 already exists；
// 只有整个入站不可用时才返回错误。
func (c *Controller) applyChangesLocked(deleted, added []panel.UserInfo) (addedN, deletedN int, err error) {
	var updates []limitUpdate
	if c.inPlaceUpdateEnabled() {
		deleted, added, updates = splitLimitUpdates(deleted, added)
	}
	if len(deleted) > 0 {
		removed, err := c.server.DelUsers(deleted, c.tag, c.info)
		c.userList = removeUsers(c.userList, removed)
		c.limiter.UpdateUser(c.tag, nil, removed)
		deletedN = len(removed)
		if err != nil {
			if len(removed) == 0 {
				return 0, 0, err
			}
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("部分用户移除失败，保留到下一轮重试")
		}
	}
	// 顺序：删除 → 原地更新 → 新增（三者互不重叠，与 DIFF 的 removed→users 一致）
	c.applyLimitUpdatesLocked(updates)
	if len(added) > 0 {
		ok, err := c.server.AddUsers(&vCore.AddUsersParams{Tag: c.tag, NodeInfo: c.info, Users: added, Quiet: c.isRejectedLocked})
		if err != nil {
			return 0, deletedN, err
		}
		c.recordRejectedLocked(added, ok)
		c.userList = append(c.userList, ok...)
		c.limiter.UpdateUser(c.tag, ok, nil)
		addedN = len(ok)
	}
	return addedN, deletedN, nil
}

// inPlaceUpdateEnabled U5 是否生效：只在声明 admission_v1（限速器挂了准入）时。
// 开启准入后名额条目的 device_limit 会随面板 seat_realtime_admission 在 0/1 之间切换，删旧加新会让全量用户断线重连；
// 未声明时保持原先的删旧加新
func (c *Controller) inPlaceUpdateEnabled() bool {
	return c.limiter != nil && c.limiter.Admission != nil
}

// limitUpdate 同一用户（id、uuid 都相同）只有限速或设备数变化
type limitUpdate struct {
	old, new panel.UserInfo
}

func identityKey(u panel.UserInfo) string {
	return strconv.Itoa(u.Id) + "|" + u.Uuid
}

// splitLimitUpdates 从“待删除 + 待新增”中挑出 id、uuid 都相同的配对，作为原地更新（U5）；其余照旧删除、新增
func splitLimitUpdates(deleted, added []panel.UserInfo) (del, add []panel.UserInfo, updates []limitUpdate) {
	if len(deleted) == 0 || len(added) == 0 {
		return deleted, added, nil
	}
	idx := make(map[string]int, len(deleted))
	for i, u := range deleted {
		if _, dup := idx[identityKey(u)]; !dup {
			idx[identityKey(u)] = i
		}
	}
	paired := make(map[int]bool)
	for _, u := range added {
		if i, ok := idx[identityKey(u)]; ok && !paired[i] {
			paired[i] = true
			updates = append(updates, limitUpdate{old: deleted[i], new: u})
			continue
		}
		add = append(add, u)
	}
	for i, u := range deleted {
		if !paired[i] {
			del = append(del, u)
		}
	}
	return del, add, updates
}

// applyLimitUpdatesLocked 原地更新：不删用户、不断连接，只更新限速器（UserLimitInfo 与 bucket）和本地列表
func (c *Controller) applyLimitUpdatesLocked(updates []limitUpdate) {
	if len(updates) == 0 {
		return
	}
	pos := make(map[string]int, len(c.userList))
	for i, u := range c.userList {
		pos[userSyncKey(u)] = i
	}
	for _, up := range updates {
		i, ok := pos[userSyncKey(up.old)]
		if !ok {
			continue
		}
		c.limiter.UpdateUserLimit(format.UserTag(c.tag, up.new.Uuid), up.new.SpeedLimit, up.new.DeviceLimit)
		c.userList[i] = up.new
		delete(pos, userSyncKey(up.old))
	}
	log.WithField("节点", c.tag).Infof("原地更新 %d 个用户的限速 / 设备数", len(updates))
}

func removeUsers(list, remove []panel.UserInfo) []panel.UserInfo {
	drop := make(map[string]int, len(remove))
	for _, u := range remove {
		drop[userSyncKey(u)]++
	}
	out := make([]panel.UserInfo, 0, len(list))
	for _, u := range list {
		k := userSyncKey(u)
		if drop[k] > 0 {
			drop[k]--
			continue
		}
		out = append(out, u)
	}
	return out
}

// ApplyUserUpdate 应用面板推送。FULL 直接对齐；DIFF 要求版本连续，断档则重新订阅取全量。
func (c *Controller) ApplyUserUpdate(u panel.UserUpdate) {
	c.userMu.Lock()
	if c.limiter == nil || c.tag == "" {
		c.userMu.Unlock()
		return // 尚未启动
	}

	switch u.Action {
	case nodev1.UserUpdateAction_USER_UPDATE_FULL:
		added, deleted, err := c.applyUserSetLocked(u.Users)
		if err != nil {
			c.syncVersion = 0
			c.userMu.Unlock()
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("应用推送全量失败，继续靠定时拉取")
			return
		}
		c.syncVersion = u.Version
		c.lastFullSync = time.Now()
		c.userMu.Unlock()
		log.WithField("节点", c.tag).Infof("收到推送全量 v%d：删除 %d 个、新增 %d 个用户", u.Version, deleted, added)

	case nodev1.UserUpdateAction_USER_UPDATE_DIFF:
		if c.syncVersion == 0 || u.Version != c.syncVersion+1 {
			local := c.syncVersion
			c.syncVersion = 0
			c.userMu.Unlock()
			log.WithField("节点", c.tag).Warnf("推送版本断档（本地 v%d，收到 v%d），重新订阅取全量", local, u.Version)
			c.resubscribe()
			return
		}
		// 只删除确实存在的、只新增尚不存在的，避免重复添加 / 删除不存在用户导致内核报错
		present := make(map[string]bool, len(c.userList))
		for _, x := range c.userList {
			present[userSyncKey(x)] = true
		}
		c.forgetRejectedLocked(u.Removed)
		var deleted, added []panel.UserInfo
		for _, x := range u.Removed {
			if present[userSyncKey(x)] {
				deleted = append(deleted, x)
				delete(present, userSyncKey(x))
			}
		}
		for _, x := range u.Users {
			if !present[userSyncKey(x)] {
				added = append(added, x)
				present[userSyncKey(x)] = true
			}
		}
		addedN, deletedN, err := c.applyChangesLocked(deleted, added)
		if err != nil {
			c.syncVersion = 0
			c.userMu.Unlock()
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("应用推送差异失败，重新订阅取全量")
			c.resubscribe()
			return
		}
		c.syncVersion = u.Version
		c.userMu.Unlock()
		log.WithField("节点", c.tag).Infof("收到推送 v%d：删除 %d 个、新增 %d 个用户", u.Version, deletedN, addedN)

	default:
		c.userMu.Unlock()
	}
}

func (c *Controller) resubscribe() {
	if c.grpcClient == nil {
		return
	}
	if err := c.grpcClient.Subscribe(panel.Inbound{Protocol: c.info.Type, Port: c.info.Protocol.Port}); err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("重新订阅失败，继续靠定时拉取")
	}
}

// resetSync 流断开：之后的差异不能接在旧版本上
func (c *Controller) resetSync() {
	c.userMu.Lock()
	c.syncVersion = 0
	c.userMu.Unlock()
}

// ===== Node 作为 panel.SyncHandler =====

func (n *Node) OnUserUpdate(u panel.UserUpdate) {
	for _, c := range n.controllers {
		if c == nil || c.grpcClient == nil || !c.info.Protocol.Enable {
			continue
		}
		if c.info.Type == u.Protocol && c.info.Protocol.Port == u.Port {
			c.ApplyUserUpdate(u)
		}
	}
}

// OnUserUpdateLost 某个入站的分片推送没有收齐：已有部分不能应用，作废本地版本并重新订阅取全量
func (n *Node) OnUserUpdateLost(protocol string, port int) {
	for _, c := range n.controllers {
		if c == nil || c.grpcClient == nil || !c.info.Protocol.Enable {
			continue
		}
		if c.info.Type == protocol && c.info.Protocol.Port == port {
			c.resetSync()
			c.resubscribe()
		}
	}
}

// OnConfigUpdate 面板通知配置变了：立即走一次带 ETag 的配置检查（有变化才重载），
// 不必等定时检查。在单独的协程里做，避免阻塞 Sync 接收循环；连续通知合并成一次。
func (n *Node) OnConfigUpdate(reason string) {
	if n.core == nil {
		return
	}
	select {
	case n.configCheck <- struct{}{}:
	default:
		return // 已有一次检查在排队
	}
	log.WithField("reason", reason).Info("收到面板配置变更通知，立即检查配置")
	go func() {
		<-n.configCheck
		n.core.CheckServerConfigNow()
	}()
}

func (n *Node) SyncInbounds() []panel.Inbound {
	var out []panel.Inbound
	for _, c := range n.controllers {
		if c == nil || c.grpcClient == nil || !c.info.Protocol.Enable || !c.started {
			continue
		}
		out = append(out, panel.Inbound{Protocol: c.info.Type, Port: c.info.Protocol.Port})
	}
	return out
}

func (n *Node) OnSyncDisconnected() {
	for _, c := range n.controllers {
		if c != nil {
			c.resetSync()
		}
	}
}
