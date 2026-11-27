package conf

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Conf struct {
	LogConfig  LogConfig       `mapstructure:"Log"`
	ApiConfig  ServerApiConfig `mapstructure:"Api"`
	GrpcConfig GrpcConfig      `mapstructure:"Grpc"`
	PprofPort  int             `mapstructure:"PprofPort"`
}

type GrpcConfig struct {
	Enable bool   `mapstructure:"Enable"`
	Addr   string `mapstructure:"Addr"` // e.g. "https://panel.example.com" or "127.0.0.1:9090"
}

type LogConfig struct {
	Level  string `mapstructure:"Level"`
	Output string `mapstructure:"Output"`
	Access string `mapstructure:"Access"`
}

type ServerApiConfig struct {
	ApiHost   string `mapstructure:"ApiHost"`
	ServerId  int    `mapstructure:"ServerID"`
	SecretKey string `mapstructure:"SecretKey"`
	Timeout   int    `mapstructure:"Timeout"`
	// SeatV1 向面板声明 seat_v1（设备名额）：限速按 bit/s 解释、接收 Sync 分片推送。
	// 默认关闭，与旧版行为一致；面板升级到支持设备名额后再逐台打开。
	SeatV1 bool `mapstructure:"SeatV1"`
	// OnlineV1 向面板声明 online_v1：在线上报改为基于活跃连接（长连接、空闲连接也上报），周期不超过 60 秒。
	// 默认关闭，在线上报的内容与时机与旧版一致；与 SeatV1 相互独立。
	OnlineV1 bool `mapstructure:"OnlineV1"`
}

type NodeApiConfig struct {
	APIHost   string `mapstructure:"ApiHost"`
	NodeID    int    `mapstructure:"NodeID"`
	SecretKey string `mapstructure:"SecretKey"`
	NodeType  string `mapstructure:"NodeType"`
	Port      int    `mapstructure:"Port"`
	Timeout   int    `mapstructure:"Timeout"`
}

func New() *Conf {
	return &Conf{
		LogConfig: LogConfig{
			Level:  "info",
			Output: "",
			Access: "none",
		},
		// 默认走 gRPC；配置里没写 Grpc 时也开启，地址从 ApiHost 推出。连不上会自动回退 HTTP。
		GrpcConfig: GrpcConfig{Enable: true},
	}
}

func (p *Conf) LoadFromPath(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open config file error: %s", err)
	}
	defer f.Close()
	v := viper.New()
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file error: %s", err)
	}
	if err := v.Unmarshal(p); err != nil {
		return fmt.Errorf("unmarshal config error: %s", err)
	}
	if p.GrpcConfig.Enable && strings.TrimSpace(p.GrpcConfig.Addr) == "" {
		p.GrpcConfig.Addr = grpcAddrFromAPIHost(p.ApiConfig.ApiHost)
		if p.GrpcConfig.Addr == "" {
			p.GrpcConfig.Enable = false
		}
	}
	return nil
}

// grpcAddrFromAPIHost 面板的 gRPC 和 API 在同一个域名：https://panel.example.com/api → https://panel.example.com
func grpcAddrFromAPIHost(apiHost string) string {
	u, err := url.Parse(strings.TrimSpace(apiHost))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
