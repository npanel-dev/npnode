package node

import (
	"fmt"
	"strings"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
)

type Node struct {
	controllers []*Controller
}

func New(core *vCore.XrayCore, config *conf.Conf, serverconfig *panel.ServerConfigResponse, grpcClient *panel.GrpcClient) (*Node, error) {
	node := &Node{
		controllers: make([]*Controller, len(*serverconfig.Data.Protocols)),
	}
	pushinterval := serverconfig.Data.PushInterval
	if pushinterval <= 0 {
		pushinterval = 60
	}
	pullinterval := serverconfig.Data.PullInterval
	if pullinterval <= 0 {
		pullinterval = 60
	}
	disableGrpc := hasSameProtocolDifferentPorts(serverconfig.Data.Protocols)
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
		if grpcClient != nil && !disableGrpc {
			controller.SetGrpcClient(grpcClient)
		}
		node.controllers[i] = controller
	}

	return node, nil
}

func (n *Node) Start() error {
	for i := range n.controllers {
		if !n.controllers[i].info.Protocol.Enable {
			continue
		}
		err := n.controllers[i].Start()
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

func hasSameProtocolDifferentPorts(protocols *[]panel.Protocol) bool {
	if protocols == nil {
		return false
	}
	portsByType := make(map[string]map[int]struct{})
	for _, protocol := range *protocols {
		if !protocol.Enable {
			continue
		}
		protocolType := strings.ToLower(protocol.Type)
		ports, ok := portsByType[protocolType]
		if !ok {
			portsByType[protocolType] = map[int]struct{}{protocol.Port: {}}
			continue
		}
		if _, exists := ports[protocol.Port]; !exists {
			return true
		}
	}
	return false
}
