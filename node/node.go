package node

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
)

type Node struct {
	controllers []*Controller
	traffic     *trafficQueue
	core        *vCore.XrayCore
	// configCheck 合并面板连续推送的配置变更：已有检查在排队时不再重复
	configCheck chan struct{}
}

func New(core *vCore.XrayCore, config *conf.Conf, serverconfig *panel.ServerConfigResponse, grpcClient *panel.GrpcClient) (*Node, error) {
	node := &Node{
		controllers: make([]*Controller, len(*serverconfig.Data.Protocols)),
		traffic:     newTrafficQueue(),
		core:        core,
		configCheck: make(chan struct{}, 1),
	}
	pushinterval := serverconfig.Data.PushInterval
	if pushinterval <= 0 {
		pushinterval = 60
	}
	pullinterval := serverconfig.Data.PullInterval
	if pullinterval <= 0 {
		pullinterval = 60
	}
	for i, nodeconfig := range *serverconfig.Data.Protocols {
		n := &panel.NodeInfo{
			Id:                     config.ApiConfig.ServerId,
			Type:                   nodeconfig.Type,
			TrafficReportThreshold: serverconfig.Data.TrafficReportThreshold,
			PushInterval:           pushinterval,
			PullInterval:           pullinterval,
			Protocol:               &nodeconfig,
		}
		p, err := panel.NewClientV1(&conf.NodeApiConfig{
			APIHost:   config.ApiConfig.ApiHost,
			NodeType:  nodeconfig.Type,
			NodeID:    config.ApiConfig.ServerId,
			SecretKey: config.ApiConfig.SecretKey,
			Port:      nodeconfig.Port,
		})
		if err != nil {
			return nil, err
		}
		controller := NewController(core, p, n)
		controller.traffic = node.traffic
		// gRPC 用户列表按协议+端口区分，同协议多端口也可以走 gRPC
		if grpcClient != nil {
			controller.SetGrpcClient(grpcClient)
		}
		node.controllers[i] = controller
	}

	return node, nil
}

// InheritTraffic retains unacknowledged reports across configuration reloads,
// including reports whose protocol or destination was removed.
func (n *Node) InheritTraffic(old *Node) {
	if old == nil || old.traffic == nil {
		return
	}
	n.traffic = old.traffic
	for _, c := range n.controllers {
		c.traffic = n.traffic
	}
}

func (n *Node) Start() error {
	for i := range n.controllers {
		if !n.controllers[i].info.Protocol.Enable {
			continue
		}
		err := n.controllers[i].Start()
		if errors.Is(err, errCertPending) {
			// 证书不可用的入站不启动，其他入站照常运行；失败原因已同步给面板，之后自动重试
			log.WithFields(log.Fields{"协议": n.controllers[i].info.Type, "端口": n.controllers[i].info.Protocol.Port, "err": err}).Error("入站因证书不可用暂不启动")
			continue
		}
		if err != nil {
			return fmt.Errorf("启动节点 [%s-%s-%d-%d] 失败: %s",
				n.controllers[i].apiClient.APIHost,
				n.controllers[i].info.Type,
				n.controllers[i].info.Id,
				n.controllers[i].info.Protocol.Port,
				err)
		}
	}
	return nil
}

func (n *Node) Close() {
	for _, c := range n.controllers {
		err := c.Close()
		if err != nil {
			panic(err)
		}
	}
	n.controllers = nil
}
