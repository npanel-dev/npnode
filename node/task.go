package node

import (
	"context"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/serverstatus"
	"github.com/npanel-dev/npnode/common/task"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch user list task
	c.userListMonitorPeriodic = &task.Task{
		Name:     "userListMonitor",
		Interval: time.Duration(node.PullInterval) * time.Second,
		Execute:  c.userListMonitor,
		ReloadCh: c.server.ReloadCh,
	}
	// report user traffic task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTraffic",
		Interval: time.Duration(node.PushInterval) * time.Second,
		Execute:  c.reportUserTrafficTask,
		ReloadCh: c.server.ReloadCh,
	}
	_ = c.userListMonitorPeriodic.Start(false)
	log.WithField("节点", c.tag).Info("用户列表监控任务已启动")
	_ = c.userReportPeriodic.Start(false)
	log.WithField("节点", c.tag).Info("用户流量报告任务已启动")
	var security string
	switch node.Type {
	case "vless":
		security = node.Protocol.Security
	case "vmess":
		security = node.Protocol.Security
	case "trojan":
		security = node.Protocol.Security
	case "anytls":
		security = node.Protocol.Security
	case "shadowsocks":
		security = ""
	case "tuic":
		security = "tls"
	case "hysteria", "hysteria2":
		security = "tls"
	default:
		security = ""
	}

	if security == "tls" {
		switch node.Protocol.CertMode {
		case "none", "", "self":
		default:
			interval := 24 * time.Hour
			if node.Protocol.CertMode == "file" {
				c.fileCertHash, _ = c.fileCertDigest()
				interval = time.Minute
			}
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCert",
				Interval: interval,
				Execute:  c.renewCertTask,
				ReloadCh: c.server.ReloadCh,
			}
			log.WithField("节点", c.tag).Info("证书定期更新任务已启动")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

func (c *Controller) reloadTask() {
	c.userListMonitorPeriodic.Close()
	c.userReportPeriodic.Close()
	if c.renewCertPeriodic != nil {
		c.renewCertPeriodic.Close()
	}
	c.startTasks(c.info)
}

func (c *Controller) userListMonitor(ctx context.Context) (err error) {
	// 推送正常时只做低频全量校对
	c.userMu.Lock()
	pushing := c.pushHealthyLocked()
	recent := time.Since(c.lastFullSync) < pushFullCheckInterval
	c.userMu.Unlock()
	if pushing && recent {
		return nil
	}

	// get user info — prefer gRPC, fall back to HTTP when it fails
	var newU []panel.UserInfo
	fetched := false
	if c.grpcClient != nil {
		newU, err = c.grpcClient.GetUserList(ctx, c.info.Type, c.info.Protocol.Port)
		if err == nil {
			fetched = true
		} else {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Warn("gRPC 获取用户列表失败，改用 HTTP")
		}
	}
	if !fetched {
		newU, err = c.apiClient.GetUserList(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Get user list failed")
			return nil
		}
		if newU == nil {
			return nil // HTTP 304 Not Modified
		}
	}
	// get user alive
	newA, err := c.apiClient.GetUserAlive()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get alive list failed")
		return nil
	}
	// update alive list
	if newA != nil {
		c.limiter.AliveList = newA
	}

	c.userMu.Lock()
	defer c.userMu.Unlock()
	added, deleted, err := c.applyUserSetLocked(newU)
	if err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("更新用户失败")
		return nil
	}
	if pushing {
		c.lastFullSync = time.Now()
		if added+deleted != 0 {
			log.WithField("节点", c.tag).Warnf("全量校对发现推送遗漏：删除 %d 个、新增 %d 个用户，已修正", deleted, added)
		}
	} else if added+deleted != 0 {
		log.WithField("节点", c.tag).Infof("删除 %d 个用户，新增 %d 个用户", deleted, added)
	}
	return nil
}

func (c *Controller) reportUserTrafficTask(ctx context.Context) (err error) {
	var reportmin = 0
	if c.info.TrafficReportThreshold > 0 {
		reportmin = c.info.TrafficReportThreshold
	}
	// 读取即清零：读出来的流量必须最终上报成功，否则就丢了，所以先放进待上报队列
	userTraffic, _ := c.server.GetUserTrafficSlice(c.tag, reportmin)
	c.queueTrafficReport(userTraffic)

	protocol, port := c.info.Type, c.info.Protocol.Port
	useSync := c.grpcClient != nil && c.grpcClient.IsSyncConnected()

	// 流量：按顺序上报待上报队列，失败的保留到下一轮
	c.flushPendingTraffic(ctx, useSync)

	// 在线用户
	if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
		log.Print(err)
	} else {
		result := onlineUsersForReport(*onlineDevice, userTraffic)
		if useSync {
			err = c.grpcClient.SendOnlineUsers(protocol, port, result)
		} else {
			err = c.apiClient.ReportNodeOnlineUsers(ctx, &result)
		}
		if err != nil {
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Info("Report online users failed")
		} else {
			log.WithField("节点", c.tag).Infof("总计 %d 名在线用户, %d 名已上报", len(*onlineDevice), len(result))
		}
	}

	// 状态
	CPU, Mem, Disk, Uptime, err := serverstatus.GetSystemInfo()
	if err != nil {
		log.Print(err)
	}
	if useSync {
		err = c.grpcClient.SendStatus(protocol, port, CPU, Mem, Disk)
	} else {
		err = c.apiClient.ReportNodeStatus(&panel.NodeStatus{CPU: CPU, Mem: Mem, Disk: Disk, Uptime: Uptime})
	}
	if err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Info("Report status failed")
	}
	return nil
}

// queueTrafficReport keeps collected bytes in the node-wide queue across reloads.
func (c *Controller) queueTrafficReport(traffic []panel.UserTraffic) {
	if c.traffic == nil {
		c.traffic = newTrafficQueue()
	}
	c.traffic.add(c.apiClient, traffic)
}

func (c *Controller) flushPendingTraffic(ctx context.Context, useSync bool) {
	if c.traffic == nil {
		return
	}
	var gc *panel.GrpcClient
	if useSync {
		gc = c.grpcClient
	}
	if err := c.traffic.reportVia(ctx, c.apiClient, gc); err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Warn("上报用户流量失败，保留到下一轮重试")
	}
}

func onlineUsersForReport(onlineDevices []panel.OnlineUser, userTraffic []panel.UserTraffic) []panel.OnlineUser {
	result := make([]panel.OnlineUser, 0, len(onlineDevices))
	noCountUID := make(map[int]struct{})
	for _, traffic := range userTraffic {
		if traffic.Upload+traffic.Download <= 0 {
			noCountUID[traffic.UID] = struct{}{}
		}
	}
	for _, online := range onlineDevices {
		if _, excluded := noCountUID[online.UID]; !excluded {
			result = append(result, online)
		}
	}
	return result
}

func compareUserList(old, new []panel.UserInfo) (deleted, added []panel.UserInfo) {
	oldMap := make(map[string]int)
	for i, user := range old {
		oldMap[userSyncKey(user)] = i
	}

	for _, user := range new {
		key := userSyncKey(user)
		if _, exists := oldMap[key]; !exists {
			added = append(added, user)
		} else {
			delete(oldMap, key)
		}
	}

	for _, index := range oldMap {
		deleted = append(deleted, old[index])
	}

	return deleted, added
}
