package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/logreport"
	"github.com/npanel-dev/npnode/conf"
	"github.com/npanel-dev/npnode/core"
	"github.com/npanel-dev/npnode/limiter"
	"github.com/npanel-dev/npnode/node"
	"github.com/npanel-dev/npnode/upgrade"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	config string
	watch  bool
)

var serverCommand = cobra.Command{
	Use:   "server",
	Short: "Run npnode server",
	Run:   serverHandle,
	Args:  cobra.NoArgs,
}

func init() {
	serverCommand.PersistentFlags().
		StringVarP(&config, "config", "c",
			"/etc/NPanel-node/config.yml", "config file path")
	serverCommand.PersistentFlags().
		BoolVarP(&watch, "watch", "w",
			true, "watch file path change")
	command.AddCommand(&serverCommand)
}

// logRingSize 环形缓冲保留的最近日志行数（上报事件的上下文取自这里）
const logRingSize = 200

// sigFlushTimeout 收到退出信号时尽力上报的最长时间
const sigFlushTimeout = 2 * time.Second

func serverHandle(_ *cobra.Command, _ []string) {
	// 日志上报器必须在安装 Hook、启动任何 goroutine 之前就位
	logreport.Default = logreport.NewReporter(logreport.Options{
		Version:   version,
		SpoolPath: filepath.Join(filepath.Dir(config), "pending-logs.json"),
	})
	// 任何未捕获的 panic：记录后尽力上报再以非 0 退出。
	// 记录本身若再 panic 也只吞掉，保证一定走到 fatalExit，不会递归。
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		func() {
			defer func() { _ = recover() }()
			log.WithField("err", logreport.Wrap(logreport.CodePanic, logreport.StageRuntime, fmt.Errorf("%v", rec))).
				WithField("exiting", true).Error("节点异常退出")
		}()
		fatalExit(logreport.Default, os.Exit)
	}()

	showVersion()
	panel.NodeVersion = version
	c := conf.New()
	err := c.LoadFromPath(config)
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: true,
		DisableQuote:     true,
		PadLevelText:     false,
	})
	if err != nil {
		logreport.Install(logreport.NewRing(logRingSize), logreport.Default.Add, log.WarnLevel, os.Stdout)
		log.WithField("err", logreport.Wrap(logreport.CodeConfigLoad, logreport.StageConfig, err)).
			WithField("exiting", true).Error("读取配置文件失败")
		// 此时还没有 Sender，FlushFatal 直接落盘，下次启动补发
		fatalExit(logreport.Default, os.Exit)
		return
	}
	// 全局级别由 Install 放开到 Debug 供环形缓冲采集，配置的级别只用于过滤实际输出
	outLevel := log.InfoLevel
	switch c.LogConfig.Level {
	case "debug":
		outLevel = log.DebugLevel
	case "info":
		outLevel = log.InfoLevel
	case "warn", "warning":
		outLevel = log.WarnLevel
	case "error":
		outLevel = log.ErrorLevel
	}
	var out io.Writer = os.Stderr
	var openErr error
	if c.LogConfig.Output != "" {
		f, err := os.OpenFile(c.LogConfig.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			openErr = err
			out = os.Stdout
		} else {
			out = f
		}
	}
	logreport.Install(logreport.NewRing(logRingSize), logreport.Default.Add, outLevel, out)
	if openErr != nil {
		log.WithField("err", openErr).Error("打开日志文件失败，使用stdout替代")
	}
	// Enable pprof if configured
	if c.PprofPort != 0 {
		go func() {
			log.Infof("Starting pprof server on :%d", c.PprofPort)
			if err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", c.PprofPort), nil); err != nil {
				log.WithField("err", err).Error("pprof server failed")
			}
		}()
	}
	limiter.Init()
	// 能力声明必须在创建任何面板客户端之前设置，同一进程内所有请求声明同一组能力
	panel.SetSeatV1(c.ApiConfig.SeatV1)
	panel.SetOnlineV1(c.ApiConfig.OnlineV1)
	panel.SetTrafficIdempotent(c.ApiConfig.TrafficIdempotent)
	panel.SetAdmissionV1(c.ApiConfig.AdmissionV1)
	p := panel.NewClientV2(&c.ApiConfig)
	// 先走 HTTP 上报；补发上次未送达的报告（有界，避免之后的 FlushFatal 等 spool 锁）
	logreport.Default.SetSender(panel.LogSender{HTTP: p})
	replaySpool(context.Background(), logreport.Default)
	reportCtx, stopReport := context.WithCancel(context.Background())
	defer stopReport()
	go logreport.Default.Run(reportCtx)

	serverconfig, err := panel.GetServerConfig(context.Background(), p)
	if err != nil {
		log.WithField("err", logreport.Wrap(logreport.CodePanelConfig, logreport.StageConfig, err)).
			WithField("exiting", true).Error("获取服务端配置失败")
		fatalExit(logreport.Default, os.Exit)
		return
	}
	// 启动时也检查远程升级请求（已处理过的请求会跳过）
	upgrade.StateDir = filepath.Dir(config)
	upgrade.Check(serverconfig)

	// gRPC client (optional)
	var grpcClient *panel.GrpcClient
	if c.GrpcConfig.Enable {
		grpcClient = newCheckedGrpcClient(&c.GrpcConfig, c.ApiConfig.SecretKey, c.ApiConfig.ServerId)
	}
	// gRPC 可用时优先经 Sync 上报，断开时 LogSender 自动回退 HTTP
	logreport.Default.SetSender(panel.LogSender{Grpc: grpcClient, HTTP: p})
	// 重载会替换 gRPC 客户端，退出时关闭当前那个
	defer func() {
		if grpcClient != nil {
			_ = grpcClient.Close()
		}
	}()

	var reloadCh = make(chan struct{}, 1)
	xraycore := core.New(c, p)
	xraycore.ReloadCh = reloadCh
	err = xraycore.Start(serverconfig)
	if err != nil {
		log.WithField("err", logreport.Wrap(logreport.CodeCoreStart, logreport.StageCore, err)).
			WithField("exiting", true).Error("启动Xray核心失败")
		fatalExit(logreport.Default, os.Exit)
		return
	}
	defer xraycore.Close()
	nodes, err := node.New(xraycore, c, serverconfig, grpcClient)
	if err != nil {
		log.WithField("err", logreport.Wrap(logreport.CodePanelConfig, logreport.StageConfig, err)).
			WithField("exiting", true).Error("获取节点配置失败")
		fatalExit(logreport.Default, os.Exit)
		return
	}
	err = nodes.Start()
	if err != nil {
		// 入站失败的错误已由 node 包带码，不再包装
		log.WithField("err", err).WithField("exiting", true).Error("启动节点失败")
		fatalExit(logreport.Default, os.Exit)
		return
	}
	// 入站全部启动后再建立 Sync：面板确认连接后节点会订阅用户推送，推送要能落到已启动的入站上
	if grpcClient != nil {
		grpcClient.StartSync(context.Background(), nodes)
	}
	log.Infof("已启动 %d 个节点", serverconfig.Data.Total)
	if watch {
		// On file change, just signal reload; do not run reload concurrently here
		err = c.Watch(config, func() {
			select {
			case reloadCh <- struct{}{}:
			default: // drop if a reload is already queued
			}
		})
		if err != nil {
			log.WithField("err", logreport.Wrap(logreport.CodeReload, logreport.StageRuntime, err)).
				WithField("exiting", true).Error("start watch failed")
			fatalExit(logreport.Default, os.Exit)
			return
		}
	}
	// clear memory
	runtime.GC()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	// 回退到 HTTP 后定时检查面板 gRPC，恢复了就重载切回 gRPC
	grpcRecovery := time.NewTicker(grpcRecoveryInterval)
	defer grpcRecovery.Stop()

	for {
		select {
		case <-grpcRecovery.C:
			if grpcClient == nil && grpcAvailableAgain(config) {
				log.Warn("面板 gRPC 已恢复，重载节点切回 gRPC")
				if err := reload(config, &nodes, &xraycore, &grpcClient); err != nil {
					log.WithField("err", logreport.Wrap(logreport.CodeReload, logreport.StageRuntime, err)).Error("重启失败")
				}
			}
		case <-osSignals:
			nodes.Close()
			_ = xraycore.Close()
			// 正常退出：停掉周期发送后尽力送出剩余日志（送不出则落盘），退出码 0
			stopReport()
			if err := logreport.Default.FlushFatal(sigFlushTimeout); err != nil {
				log.WithField("err", err).Debug("[log] 退出前上报节点日志未完成")
			}
			return
		case <-reloadCh:
			log.Info("收到重启信号，正在重新加载配置...")
			if err := reload(config, &nodes, &xraycore, &grpcClient); err != nil {
				log.WithField("err", logreport.Wrap(logreport.CodeReload, logreport.StageRuntime, err)).Error("重启失败")
			}
		}
	}
}

func reload(config string, nodes **node.Node, xcore **core.XrayCore, grpcClient **panel.GrpcClient) error {
	// Preserve old reload channel so new core continues to receive signals
	var oldReloadCh chan struct{}

	if *xcore != nil {
		oldReloadCh = (*xcore).ReloadCh
	}

	(*nodes).Close()
	// 关闭旧的 gRPC 客户端：原先每次重载都新建一个而不关旧的，旧的 Sync 循环会一直运行
	if *grpcClient != nil {
		_ = (*grpcClient).Close()
		*grpcClient = nil
	}
	if err := (*xcore).Close(); err != nil {
		return err
	}

	newConf := conf.New()
	if err := newConf.LoadFromPath(config); err != nil {
		return err
	}
	panel.SetSeatV1(newConf.ApiConfig.SeatV1)
	panel.SetOnlineV1(newConf.ApiConfig.OnlineV1)
	panel.SetTrafficIdempotent(newConf.ApiConfig.TrafficIdempotent)
	panel.SetAdmissionV1(newConf.ApiConfig.AdmissionV1)
	p := panel.NewClientV2(&newConf.ApiConfig)
	serverconfig, err := panel.GetServerConfig(context.Background(), p)
	if err != nil {
		log.WithField("err", err).Error("获取服务端配置失败")
		return err
	}

	newCore := core.New(newConf, p)
	// Reattach reload channel
	newCore.ReloadCh = oldReloadCh
	if err := newCore.Start(serverconfig); err != nil {
		return err
	}

	// Reconnect gRPC if enabled
	var newGrpc *panel.GrpcClient
	if newConf.GrpcConfig.Enable {
		newGrpc = newCheckedGrpcClient(&newConf.GrpcConfig, newConf.ApiConfig.SecretKey, newConf.ApiConfig.ServerId)
	}

	newNodes, err := node.New(newCore, newConf, serverconfig, newGrpc)
	if err != nil {
		if newGrpc != nil {
			_ = newGrpc.Close()
		}
		return err
	}
	newNodes.InheritTraffic(*nodes)
	if err := newNodes.Start(); err != nil {
		if newGrpc != nil {
			_ = newGrpc.Close()
		}
		return err
	}
	if newGrpc != nil {
		newGrpc.StartSync(context.Background(), newNodes)
	}
	*grpcClient = newGrpc
	if logreport.Default != nil {
		logreport.Default.SetSender(panel.LogSender{Grpc: newGrpc, HTTP: p})
	}

	*nodes = newNodes
	*xcore = newCore
	log.Infof("%d 个节点重启成功", serverconfig.Data.Total)
	runtime.GC()
	return nil
}

// newCheckedGrpcClient 建立 gRPC 客户端并确认面板可用；连不上（未转发 gRPC、旧版面板）时返回 nil，节点走 HTTP。
func newCheckedGrpcClient(cfg *conf.GrpcConfig, secretKey string, serverID int) *panel.GrpcClient {
	gc, err := panel.NewGrpcClient(cfg, secretKey, serverID)
	if err != nil {
		log.WithField("err", err).Warn("gRPC 连接失败，使用 HTTP")
		return nil
	}
	if err := gc.Probe(context.Background()); err != nil {
		_ = gc.Close()
		log.WithFields(log.Fields{"addr": cfg.Addr, "err": err}).Warn("面板 gRPC 不可用，使用 HTTP")
		return nil
	}
	return gc
}

// grpcRecoveryInterval 节点因面板 gRPC 不可用改走 HTTP 后，每隔这么久再探测一次
const grpcRecoveryInterval = 4 * time.Hour

// grpcAvailableAgain 配置仍要求 gRPC 且面板 gRPC 已经可用时返回 true（探测用的连接随即关闭，重载时重新建立）
func grpcAvailableAgain(config string) bool {
	c := conf.New()
	if err := c.LoadFromPath(config); err != nil || !c.GrpcConfig.Enable {
		return false
	}
	gc := newCheckedGrpcClient(&c.GrpcConfig, c.ApiConfig.SecretKey, c.ApiConfig.ServerId)
	if gc == nil {
		return false
	}
	_ = gc.Close()
	return true
}
