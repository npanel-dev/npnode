package node

import (
	"context"
	"fmt"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/task"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	log "github.com/sirupsen/logrus"
)

type Controller struct {
	server                  *vCore.XrayCore
	apiClient               *panel.ClientV1
	grpcClient              *panel.GrpcClient
	tag                     string
	limiter                 *limiter.Limiter
	userList                []panel.UserInfo
	aliveMap                map[int]int
	info                    *panel.NodeInfo
	traffic                 *trafficQueue
	userListMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	renewCertPeriodic       *task.Task
	onlineIpReportPeriodic  *task.Task
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
	if c.grpcClient != nil {
		c.userList, err = c.grpcClient.GetUserList(context.Background(), c.info.Type)
		if err != nil {
			return fmt.Errorf("gRPC get user list error: %s", err)
		}
	} else {
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
			return fmt.Errorf("request cert error: %s", err)
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
	})
	if err != nil {
		return fmt.Errorf("add users error: %s", err)
	}
	log.WithField("节点", c.tag).Infof("已添加 %d 个新用户", added)
	c.startTasks(c.info)
	return nil
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
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
	if port > 0 {
		return fmt.Sprintf("[%s]-%s:%d:%d", c.apiClient.APIHost, node.Type, node.Id, port)
	}
	return fmt.Sprintf("[%s]-%s:%d", c.apiClient.APIHost, node.Type, node.Id)
}
