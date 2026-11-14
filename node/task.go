package node

import (
	"context"
	"strconv"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/serverstatus"
	"github.com/npanel-dev/npnode/common/task"
	vCore "github.com/npanel-dev/npnode/core"
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
		case "none", "", "file", "self":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCert",
				Interval: time.Hour * 24,
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
	// get user info — prefer gRPC if available
	var newU []panel.UserInfo
	if c.grpcClient != nil {
		newU, err = c.grpcClient.GetUserList(ctx, c.info.Type, c.info.Protocol.Port)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("gRPC Get user list failed")
			return nil
		}
	} else {
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
	// update user list
	deleted, added := compareUserList(c.userList, newU)
	if len(deleted) > 0 {
		// have deleted users
		err = c.server.DelUsers(deleted, c.tag, c.info)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			return nil
		}
	}
	if len(added) > 0 {
		// have added users
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			return nil
		}
	}
	if len(added) > 0 || len(deleted) > 0 {
		// update Limiter
		c.limiter.UpdateUser(c.tag, added, deleted)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("limiter users failed")
			return nil
		}
	}
	c.userList = newU
	if len(added)+len(deleted) != 0 {
		log.WithField("节点", c.tag).
			Infof("删除 %d 个用户，新增 %d 个用户", len(deleted), len(added))
	}
	return nil
}

func (c *Controller) reportUserTrafficTask(ctx context.Context) (err error) {
	var reportmin = 0
	if c.info.TrafficReportThreshold > 0 {
		reportmin = c.info.TrafficReportThreshold
	}
	userTraffic, _ := c.server.GetUserTrafficSlice(c.tag, reportmin)
	c.traffic.add(c.apiClient, userTraffic)
	// Sync.Send has no per-report acknowledgement. Use HTTP for accounted bytes
	// and retain the batch until the panel confirms it accepted the report.
	if err := c.traffic.report(ctx); err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).Info("Report user traffic failed; retained for retry")
	}

	// Online users and status can still use the Sync stream.
	if c.grpcClient != nil && c.grpcClient.IsSyncConnected() {
		// Report online users via Sync stream
		if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
			log.Print(err)
		} else {
			result := onlineUsersForReport(*onlineDevice, userTraffic)
			if err = c.grpcClient.SendOnlineUsers(result); err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Info("gRPC Report online users failed")
			} else {
				log.WithField("节点", c.tag).Infof("gRPC 已上报 %d 名在线用户", len(result))
			}
		}

		// Report status via Sync stream
		CPU, Mem, Disk, _, err := serverstatus.GetSystemInfo()
		if err != nil {
			log.Print(err)
		}
		if err = c.grpcClient.SendStatus(CPU, Mem, Disk); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Info("gRPC Report status failed")
		}
	} else {
		if onlineDevice, err := c.limiter.GetOnlineDevice(); err != nil {
			log.Print(err)
		} else {
			result := onlineUsersForReport(*onlineDevice, userTraffic)
			if err = c.apiClient.ReportNodeOnlineUsers(ctx, &result); err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Info("Report online users failed")
			} else {
				log.WithField("节点", c.tag).Infof("总计 %d 名在线用户, %d 名已上报", len(*onlineDevice), len(result))
			}
		}

		CPU, Mem, Disk, Uptime, err := serverstatus.GetSystemInfo()
		if err != nil {
			log.Print(err)
		}
		err = c.apiClient.ReportNodeStatus(
			&panel.NodeStatus{
				CPU:    CPU,
				Mem:    Mem,
				Disk:   Disk,
				Uptime: Uptime,
			})
		if err != nil {
			log.Print(err)
		}
	}

	userTraffic = nil
	return nil
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
		key := user.Uuid + strconv.Itoa(user.SpeedLimit)
		oldMap[key] = i
	}

	for _, user := range new {
		key := user.Uuid + strconv.Itoa(user.SpeedLimit)
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
