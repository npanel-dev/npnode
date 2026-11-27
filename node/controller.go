package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/task"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	log "github.com/sirupsen/logrus"
)

type Controller struct {
	// userMu 保护 userList / syncVersion / lastFullSync：推送与定时拉取会并发修改
	userMu sync.Mutex
	// syncVersion 已应用的推送版本，0 表示推送未就绪（未收到全量或流已断开）
	syncVersion uint64
	// lastFullSync 推送模式下最近一次全量（推送全量或拉取校对）的时间
	lastFullSync            time.Time
	server                  *vCore.XrayCore
	apiClient               *panel.ClientV1
	grpcClient              *panel.GrpcClient
	tag                     string
	limiter                 *limiter.Limiter
	userList                []panel.UserInfo
	// rejected 本入站加入失败过的条目（键 rejectKey：id + uuid + 参数），同一内容再失败不重复告警（U3 ④）。
	// 内容变化（键变了）或面板移除该条目时清掉；受 userMu 保护
	rejected map[string]struct{}
	aliveMap                map[int]int
	info                    *panel.NodeInfo
	traffic                 *trafficQueue
	userListMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	renewCertPeriodic       *task.Task
	fileCertHash            [32]byte
	// started 入站已加入内核；证书不可用时为 false，由 certRetryStop 对应的协程定时重试
	started                bool
	certRetryStop          chan struct{}
	onlineIpReportPeriodic *task.Task
}

// NewController return a Node controller with default parameters.
func NewController(core *vCore.XrayCore, api *panel.ClientV1, info *panel.NodeInfo) *Controller {
	controller := &Controller{
		server:    core,
		apiClient: api,
		info:      info,
		traffic:   newTrafficQueue(),
	}
	return controller
}

// SetGrpcClient attaches an optional gRPC client for better performance.
func (c *Controller) SetGrpcClient(gc *panel.GrpcClient) {
	c.grpcClient = gc
}

// Start implement the Start() function of the service interface
func (c *Controller) Start() error {
	var err error
	// Update user — prefer gRPC
	fetched := false
	if c.grpcClient != nil {
		c.userList, err = c.grpcClient.GetUserList(context.Background(), c.info.Type, c.info.Protocol.Port)
		if err == nil {
			fetched = true
		} else {
			log.WithFields(log.Fields{"tag": c.buildNodeTag(c.info), "err": err}).Warn("gRPC 获取用户列表失败，改用 HTTP")
		}
	}
	if !fetched {
		c.userList, err = c.apiClient.GetUserList(context.Background())
		if err != nil {
			return fmt.Errorf("get user list error: %s", err)
		}
	}
	if len(c.userList) == 0 {
		// 没有有效用户也照常启动：入站和定时任务都要建立，
		// 之后面板有了用户，userListMonitor 会自动把用户加进来。
		log.WithField("节点", c.buildNodeTag(c.info)).Warn("当前没有有效用户，节点照常启动，等待面板下发用户")
	}
	c.aliveMap, err = c.apiClient.GetUserAlive()
	if err != nil {
		return fmt.Errorf("failed to get user alive list: %s", err)
	}
	c.tag = c.buildNodeTag(c.info)

	// add limiter
	l := limiter.AddLimiter(c.tag, c.userList, c.aliveMap)
	c.limiter = l

	if c.info.Protocol.Security == "tls" {
		err = c.requestCert()
		if err != nil {
			if errors.Is(err, errCertPending) {
				// 不用错误的证书启动：撤掉限速器，定时重试，成功后重载节点
				limiter.DeleteLimiter(c.tag)
				c.limiter = nil
				c.startCertRetry()
			}
			return fmt.Errorf("request cert error: %w", err)
		}
	}
	// Add new tag
	err = c.server.AddNode(c.tag, c.info)
	if err != nil {
		return fmt.Errorf("add new node error: %s", err)
	}
	added, err := c.server.AddUsers(&vCore.AddUsersParams{
		Tag:      c.tag,
		Users:    c.userList,
		NodeInfo: c.info,
		Quiet:    c.isRejectedLocked,
	})
	if err != nil {
		return fmt.Errorf("add users error: %s", err)
	}
	c.recordRejectedLocked(c.userList, added)
	// 单条失败只跳过（内核已记日志）：本地只记实际加入的用户，下一轮同步重试其余的
	if len(added) != len(c.userList) {
		c.syncLimiterAfterPartialAdd(added)
		c.userList = added
	}
	log.WithField("节点", c.tag).Infof("已添加 %d 个新用户", len(added))
	c.startTasks(c.info)
	c.started = true
	return nil
}

// syncLimiterAfterPartialAdd 限速器按完整列表建立，启动时有用户没加进内核：移除这些用户，再按实际加入的用户重写
// （同一 uuid 重复时，被跳过的那条可能覆盖了实际加入那条的限速信息）
func (c *Controller) syncLimiterAfterPartialAdd(added []panel.UserInfo) {
	ok := make(map[string]struct{}, len(added))
	for _, u := range added {
		ok[u.Uuid] = struct{}{}
	}
	var dropped []panel.UserInfo
	for _, u := range c.userList {
		if _, in := ok[u.Uuid]; !in {
			dropped = append(dropped, u)
		}
	}
	c.limiter.UpdateUser(c.tag, added, dropped)
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
	c.stopCertRetry()
	if !c.started {
		return nil // 入站没有启动（证书不可用），没有可清理的
	}
	limiter.DeleteLimiter(c.tag)
	if c.userListMonitorPeriodic != nil {
		c.userListMonitorPeriodic.Close()
	}
	if c.userReportPeriodic != nil {
		c.userReportPeriodic.Close()
	}
	if c.renewCertPeriodic != nil {
		c.renewCertPeriodic.Close()
	}
	if c.onlineIpReportPeriodic != nil {
		c.onlineIpReportPeriodic.Close()
	}
	err := c.server.DelNode(c.tag)
	if err != nil {
		return fmt.Errorf("del node error: %s", err)
	}
	return nil
}

func (c *Controller) buildNodeTag(node *panel.NodeInfo) string {
	port := 0
	if node.Protocol != nil {
		port = node.Protocol.Port
	}
	return vCore.InboundTag(c.apiClient.APIHost, node.Id, node.Type, port)
}
