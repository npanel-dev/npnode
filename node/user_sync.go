package node

import (
	"strconv"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
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
func (c *Controller) applyUserSetLocked(users []panel.UserInfo) (added, deleted int, err error) {
	del, add := compareUserList(c.userList, users)
	if err := c.applyChangesLocked(del, add); err != nil {
		return 0, 0, err
	}
	return len(add), len(del), nil
}

// applyChangesLocked 先删后加，并保证 c.userList 始终与内核实际状态一致：
// 删除成功而新增失败时，列表里也要去掉已删除的用户，否则下一轮会反复删除不存在的用户。
func (c *Controller) applyChangesLocked(deleted, added []panel.UserInfo) error {
	if len(deleted) > 0 {
		if err := c.server.DelUsers(deleted, c.tag, c.info); err != nil {
			return err
		}
		c.userList = removeUsers(c.userList, deleted)
		c.limiter.UpdateUser(c.tag, nil, deleted)
	}
	if len(added) > 0 {
		if _, err := c.server.AddUsers(&vCore.AddUsersParams{Tag: c.tag, NodeInfo: c.info, Users: added}); err != nil {
			return err
		}
		c.userList = append(c.userList, added...)
		c.limiter.UpdateUser(c.tag, added, nil)
	}
	return nil
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
		if err := c.applyChangesLocked(deleted, added); err != nil {
			c.syncVersion = 0
			c.userMu.Unlock()
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("应用推送差异失败，重新订阅取全量")
			c.resubscribe()
			return
		}
		c.syncVersion = u.Version
		c.userMu.Unlock()
		log.WithField("节点", c.tag).Infof("收到推送 v%d：删除 %d 个、新增 %d 个用户", u.Version, len(deleted), len(added))

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

// OnConfigUpdate 配置推送在后续阶段接入，目前仍靠定时检查配置
func (n *Node) OnConfigUpdate(string) {}

func (n *Node) SyncInbounds() []panel.Inbound {
	var out []panel.Inbound
	for _, c := range n.controllers {
		if c == nil || c.grpcClient == nil || !c.info.Protocol.Enable {
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
