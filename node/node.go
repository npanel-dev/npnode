package node

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/logreport"
	"github.com/npanel-dev/npnode/common/serverstatus"
	"github.com/npanel-dev/npnode/common/task"
	"github.com/npanel-dev/npnode/conf"
	vCore "github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
)

type Node struct {
	controllers []*Controller
	traffic     *trafficQueue
	core        *vCore.XrayCore
	// 没有任何入站时由服务器级任务上报状态，否则面板会一直显示离线
	statusClient *panel.ClientV2
	grpcClient   *panel.GrpcClient
	statusTask   *task.Task
	pushInterval int
	// configCheck 合并面板连续推送的配置变更：已有检查在排队时不再重复
	configCheck chan struct{}
	// sessionClient 准入接口客户端（admission_v1）；未声明时为 nil
	sessionClient *panel.SessionClient
	serverID      int
	admission     *limiter.Admission
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
	node.statusClient = panel.NewClientV2(&config.ApiConfig)
	node.serverID = config.ApiConfig.ServerId
	if panel.AdmissionV1Enabled() {
		node.sessionClient = panel.NewSessionClient(&config.ApiConfig)
	}
	node.grpcClient = grpcClient
	node.pushInterval = pushinterval
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

// startAdmission admission_v1：建立（或沿用）进程级准入并启动后台循环，须在入站（限速器）启动之前；
// 未声明时撤掉进程级准入（重载时关闭了开关）
func (n *Node) startAdmission() {
	if n.sessionClient == nil {
		limiter.DisableAdmission()
		return
	}
	core := n.core
	n.admission = limiter.EnsureAdmission(limiter.AdmissionConfig{
		Checker:  n.sessionClient,
		ServerID: int64(n.serverID),
		// 保活被拒：关闭本进程所有入站上该 (uid, ip) 的全部连接（严格模式要求跨所有入站）
		Closer: func(uid int, ip string) {
			for _, taguuid := range limiter.TagUUIDsForUID(uid) {
				core.CloseUserLinksByIP(taguuid, ip)
			}
		},
	})
	n.admission.Start()
}

func (n *Node) Start() error {
	n.startAdmission()
	if len(n.controllers) == 0 {
		n.startServerStatusTask()
		log.Info("服务器还没有协议，已对接面板，暂不启动入站")
		logreport.Resolve(logreport.ScopeServer, "", 0, []string{})
		return nil
	}
	active := []string{}
	for i := range n.controllers {
		if !n.controllers[i].info.Protocol.Enable {
			continue
		}
		err := n.controllers[i].Start()
		if errors.Is(err, errCertPending) {
			// 证书不可用的入站不启动，其他入站照常运行；失败原因已同步给面板，之后自动重试
			certErr := logreport.WithInbound(logreport.Wrap(logreport.CodeCertUnavailable, logreport.StageCert, err),
				n.controllers[i].info.Type, n.controllers[i].info.Protocol.Port)
			log.WithFields(log.Fields{"协议": n.controllers[i].info.Type, "端口": n.controllers[i].info.Protocol.Port, "err": certErr}).Error("入站因证书不可用暂不启动")
			continue
		}
		if err != nil {
			return logreport.WithInbound(fmt.Errorf("启动节点 [%s-%s-%d-%d] 失败: %w",
				n.controllers[i].apiClient.APIHost,
				n.controllers[i].info.Type,
				n.controllers[i].info.Id,
				n.controllers[i].info.Protocol.Port,
				err), n.controllers[i].info.Type, n.controllers[i].info.Protocol.Port)
		}
		active = append(active, fmt.Sprintf("%s:%d", n.controllers[i].info.Type, n.controllers[i].info.Protocol.Port))
	}
	logreport.Resolve(logreport.ScopeServer, "", 0, active)
	return nil
}

func (n *Node) Close() {
	if n.statusTask != nil {
		n.statusTask.Close()
		n.statusTask = nil
	}
	for _, c := range n.controllers {
		err := c.Close()
		if err != nil {
			panic(err)
		}
	}
	n.controllers = nil
	// 准入后台循环随节点停止；会话表保留，重载后的新节点沿用（已有连接关闭时仍能对上并 release）
	if n.admission != nil {
		n.admission.Stop()
		n.admission = nil
	}
}

func (n *Node) startServerStatusTask() {
	n.statusTask = &task.Task{
		Name:     "reportServerStatus",
		Interval: time.Duration(n.pushInterval) * time.Second,
		Execute:  n.reportServerStatus,
		ReloadCh: n.core.ReloadCh,
	}
	_ = n.statusTask.Start(true)
}

// reportServerStatus 不带协议上报 CPU/内存/磁盘，让面板知道节点已在线。
// 失败只记日志，不返回错误，否则任务会停止。
func (n *Node) reportServerStatus(ctx context.Context) error {
	cpu, mem, disk, uptime, err := serverstatus.GetSystemInfo()
	if err != nil {
		log.WithField("err", err).Warn("读取系统状态失败")
	}
	if n.grpcClient != nil && n.grpcClient.IsSyncConnected() {
		err = n.grpcClient.SendStatus("", 0, cpu, mem, disk)
	} else {
		err = n.statusClient.ReportServerStatus(&panel.NodeStatus{CPU: cpu, Mem: mem, Disk: disk, Uptime: uptime})
	}
	if err != nil {
		log.WithField("err", logreport.Wrap(logreport.CodeStatusReport, logreport.StageReport, err)).Warn("Report server status failed")
	}
	return nil
}
