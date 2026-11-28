package core

import (
	"context"
	"sync"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/counter"
	"github.com/npanel-dev/npnode/common/task"
	"github.com/npanel-dev/npnode/conf"
	"github.com/npanel-dev/npnode/core/app/dispatcher"
	_ "github.com/npanel-dev/npnode/core/distro/all"
	"github.com/npanel-dev/npnode/upgrade"
	log "github.com/sirupsen/logrus"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	coreConf "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy"
	"google.golang.org/protobuf/proto"
)

type AddUsersParams struct {
	Tag   string
	Users []panel.UserInfo
	*panel.NodeInfo
	// Quiet 返回 true 的用户之前已经失败并告警过（内容未变）：这次再失败只记 Debug，不重复 Warn。可为 nil
	Quiet func(panel.UserInfo) bool
}

// logSkip 单条用户被跳过：首次 Warn，已告警过的同一内容降为 Debug
func (p *AddUsersParams) logSkip(u panel.UserInfo, key string, value any, msg string) {
	entry := log.WithFields(log.Fields{"tag": p.Tag, "uid": u.Id, key: value})
	if p.Quiet != nil && p.Quiet(u) {
		entry.Debug(msg)
		return
	}
	entry.Warn(msg)
}

type XrayCore struct {
	Config                      *conf.Conf
	Client                      *panel.ClientV2
	ReloadCh                    chan struct{}
	serverConfigMonitorPeriodic *task.Task
	// configCheckMu 串行化配置检查：定时检查与面板推送触发的检查共用 ETag / 响应哈希状态
	configCheckMu sync.Mutex
	access        sync.Mutex
	Server        *core.Instance
	users         *UserMap
	ihm           inbound.Manager
	ohm           outbound.Manager
	dispatcher    *dispatcher.DefaultDispatcher
	// userManagerFor 仅供测试注入 UserManager（模拟内核报错），生产为 nil
	userManagerFor func(tag string) (proxy.UserManager, error)
}

type UserMap struct {
	uidMap map[string]int
	// retired 已移除用户的计数器（按入站标签），下一次上报时读出并以原 id 上报（U2）
	retired map[string][]retiredCounter
	mapLock sync.RWMutex
}

// retiredCounter 已移除用户的计数器与原 id
type retiredCounter struct {
	uid     int
	storage *counter.TrafficStorage
}

func New(config *conf.Conf, client *panel.ClientV2) *XrayCore {
	core := &XrayCore{
		Config: config,
		Client: client,
		users: &UserMap{
			uidMap:  make(map[string]int),
			retired: make(map[string][]retiredCounter),
		},
	}
	return core
}

func (v *XrayCore) Start(serverconfig *panel.ServerConfigResponse) error {
	v.access.Lock()
	defer v.access.Unlock()
	v.Server = getCore(v.Config, serverconfig)
	if err := v.Server.Start(); err != nil {
		return err
	}
	v.ihm = v.Server.GetFeature(inbound.ManagerType()).(inbound.Manager)
	v.ohm = v.Server.GetFeature(outbound.ManagerType()).(outbound.Manager)
	v.dispatcher = v.Server.GetFeature(routing.DispatcherType()).(*dispatcher.DefaultDispatcher)
	v.startTasks(serverconfig)
	return nil
}

func (v *XrayCore) Close() error {
	v.access.Lock()
	defer v.access.Unlock()
	if v.serverConfigMonitorPeriodic != nil {
		v.serverConfigMonitorPeriodic.Close()
	}
	v.Config = nil
	v.ihm = nil
	v.ohm = nil
	v.dispatcher = nil
	err := v.Server.Close()
	if err != nil {
		return err
	}
	return nil
}

func getCore(c *conf.Conf, serverconfig *panel.ServerConfigResponse) *core.Instance {
	// Log Config
	coreLogConfig := &coreConf.LogConfig{
		LogLevel:  c.LogConfig.Level,
		AccessLog: c.LogConfig.Access,
		ErrorLog:  c.LogConfig.Output,
	}
	// Custom config
	dnsConfig, outBoundConfig, routeConfig, err := GetCustomConfig(serverconfig, func(protocolType string, port int) string {
		return InboundTag(c.ApiConfig.ApiHost, c.ApiConfig.ServerId, protocolType, port)
	})
	if err != nil {
		log.WithField("err", err).Panic("failed to build custom config")
	}
	// Inbound config
	var inBoundConfig []*core.InboundHandlerConfig

	// Policy config
	levelPolicyConfig := &coreConf.Policy{
		StatsUserUplink:   true,
		StatsUserDownlink: true,
		Handshake:         proto.Uint32(4),
		ConnectionIdle:    proto.Uint32(30),
		UplinkOnly:        proto.Uint32(2),
		DownlinkOnly:      proto.Uint32(4),
		BufferSize:        proto.Int32(64),
	}
	corePolicyConfig := &coreConf.PolicyConfig{}
	corePolicyConfig.Levels = map[uint32]*coreConf.Policy{0: levelPolicyConfig}
	policyConfig, _ := corePolicyConfig.Build()
	// Build Xray conf
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(coreLogConfig.Build()),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(policyConfig),
			serial.ToTypedMessage(dnsConfig),
			serial.ToTypedMessage(routeConfig),
		},
		Inbound:  inBoundConfig,
		Outbound: outBoundConfig,
	}
	server, err := core.New(config)
	if err != nil {
		log.WithField("err", err).Panic("failed to create instance")
	}
	return server
}

// CheckServerConfigNow 立即检查一次服务端配置（面板推送配置变更时调用），有变化则触发重载。
func (c *XrayCore) CheckServerConfigNow() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = c.ServerConfigMonitor(ctx)
}

func (c *XrayCore) startTasks(serverconfig *panel.ServerConfigResponse) {
	// fetch node info task
	pullinverval := serverconfig.Data.PullInterval
	if pullinverval <= 0 {
		pullinverval = 60
	}
	c.serverConfigMonitorPeriodic = &task.Task{
		Interval: time.Duration(pullinverval) * time.Second,
		Execute:  c.ServerConfigMonitor,
		ReloadCh: c.ReloadCh,
	}
	_ = c.serverConfigMonitorPeriodic.Start(false)
}

func (c *XrayCore) ServerConfigMonitor(ctx context.Context) (err error) {
	c.configCheckMu.Lock()
	defer c.configCheckMu.Unlock()
	newServerConfig, err := panel.GetServerConfig(ctx, c.Client)
	if err != nil {
		log.WithField("err", err).Error("获取服务端配置失败")
		return nil
	}
	if newServerConfig != nil {
		// 配置里带了远程升级请求：先升级（成功时进程被新版本替换），失败则照常重载
		upgrade.Check(newServerConfig)
		log.Error("检测到服务端配置变更，正在重启节点...")
		// Non-blocking signal to avoid goroutine stuck when channel is full or nil
		if c.ReloadCh != nil {
			select {
			case c.ReloadCh <- struct{}{}:
			default:
			}
		}
	}
	return nil
}
