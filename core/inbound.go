package core

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/logreport"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/inbound"
	coreConf "github.com/xtls/xray-core/infra/conf"
)

func (v *XrayCore) removeInbound(tag string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return v.ihm.RemoveHandler(ctx, tag)
}

func (v *XrayCore) addInbound(config *core.InboundHandlerConfig) error {
	rawHandler, err := core.CreateObject(v.Server, config)
	if err != nil {
		return err
	}
	handler, ok := rawHandler.(inbound.Handler)
	if !ok {
		return fmt.Errorf("not an InboundHandler: %s", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := v.ihm.AddHandler(ctx, handler); err != nil {
		return err
	}
	return nil
}

// BuildInbound build Inbound config for different protocol
func buildInbound(nodeInfo *panel.NodeInfo, tag string) (*core.InboundHandlerConfig, error) {
	return buildInboundWithUsers(nodeInfo, tag, nil)
}

// buildInboundWithUsers plainUsers 只对 SOCKS / HTTP 有用：它们的账号写在入站设置里
func buildInboundWithUsers(nodeInfo *panel.NodeInfo, tag string, plainUsers []string) (*core.InboundHandlerConfig, error) {
	in := &coreConf.InboundDetourConfig{}
	var err error
	switch nodeInfo.Type {
	case "socks", "http":
		err = buildPlainProxy(nodeInfo, in, plainUsers)
	case "vless":
		err = buildVLess(nodeInfo, in)
	case "vmess":
		err = buildVMess(nodeInfo, in)
	case "trojan":
		err = buildTrojan(nodeInfo, in)
	case "shadowsocks":
		err = buildShadowsocks(nodeInfo, in)
	case "hysteria2", "hysteria":
		err = buildHysteria2(nodeInfo, in)
	case "tuic":
		err = buildTuic(nodeInfo, in)
	case "anytls":
		err = buildAnyTLS(nodeInfo, in)
	default:
		return nil, fmt.Errorf("unsupported node type: %s", nodeInfo.Type)
	}
	if err != nil {
		return nil, err
	}
	// Set network protocol
	// Set server port
	in.PortList = &coreConf.PortList{
		Range: []coreConf.PortRange{
			{
				From: uint32(nodeInfo.Protocol.Port),
				To:   uint32(nodeInfo.Protocol.Port),
			}},
	}
	// Set Listen IP address
	ipAddress := net.ParseAddress("0.0.0.0")
	in.ListenOn = &coreConf.Address{Address: ipAddress}
	// Set SniffingConfig
	sniffingConfig := &coreConf.SniffingConfig{
		Enabled:      true,
		DestOverride: coreConf.StringList{"http", "tls", "quic"},
		RouteOnly:    true,
	}
	in.SniffingConfig = sniffingConfig

	// Set TLS or Reality settings
	switch nodeInfo.Protocol.Security {
	case "tls":
		switch nodeInfo.Protocol.CertMode {
		case "none", "":
			break
		default:
			if in.StreamSetting == nil {
				in.StreamSetting = &coreConf.StreamConfig{}
			}
			in.StreamSetting.Security = "tls"
			certFile, keyFile := nodeInfo.CertFilePath(), nodeInfo.KeyFilePath()
			if nodeInfo.Protocol.CertMode == "file" {
				if _, err := os.Stat(certFile); err != nil {
					legacyCert, legacyKey := nodeInfo.LegacyCertFilePath(), nodeInfo.LegacyKeyFilePath()
					if _, certErr := os.Stat(legacyCert); certErr == nil {
						if _, keyErr := os.Stat(legacyKey); keyErr == nil {
							certFile, keyFile = legacyCert, legacyKey
						}
					}
				}
			}
			in.StreamSetting.TLSSettings = &coreConf.TLSConfig{
				Certs: []*coreConf.TLSCertConfig{
					buildTLSCertificate(certFile, keyFile),
				},
			}
			applyQUICALPN(nodeInfo.Type, in.StreamSetting.TLSSettings)
			applyECH(nodeInfo, in.StreamSetting.TLSSettings)
		}
	case "reality":
		if in.StreamSetting == nil {
			in.StreamSetting = &coreConf.StreamConfig{}
		}
		in.StreamSetting.Security = "reality"
		v := nodeInfo.Protocol
		add := v.RealityServerAddr
		if add == "" {
			add = v.SNI
		}
		d, err := json.Marshal(fmt.Sprintf(
			"%s:%d",
			add,
			v.RealityServerPort))
		if err != nil {
			return nil, fmt.Errorf("marshal reality dest error: %s", err)
		}
		in.StreamSetting.REALITYSettings = &coreConf.REALITYConfig{
			Dest:        d,
			Xver:        uint64(0),
			Show:        false,
			ServerNames: []string{v.SNI},
			PrivateKey:  v.RealityPrivateKey,
			// 不限制客户端版本：上游 Xray 默认只放行 v26.3.27 以上的客户端
			MinClientVer: "0.0.1",
			ShortIds:     []string{v.RealityShortID},
			//Mldsa65Seed: v.RealityMldsa65Seed,
		}
	default:
		break
	}
	in.Tag = tag
	return in.Build()
}

// Node certificate tasks reload the inbound after renewal or file replacement.
// Avoid the core's concurrent certificate-slice mutation during handshakes.
func buildTLSCertificate(certFile, keyFile string) *coreConf.TLSCertConfig {
	return &coreConf.TLSCertConfig{CertFile: certFile, KeyFile: keyFile, OneTimeLoading: true}
}

func buildStreamSetting(p *panel.Protocol) (*coreConf.StreamConfig, error) {
	transport := normalizeTransport(p.Transport)
	obfs := normalizeOptionalSetting(p.Obfs)
	if obfs == "http" {
		transport = "tcp"
	} else if obfs != "" {
		return nil, fmt.Errorf("unsupported obfs type: %s", p.Obfs)
	}
	t := coreConf.TransportProtocol(transport)
	stream := &coreConf.StreamConfig{Network: &t}
	switch transport {
	case "tcp":
		stream.TCPSettings = &coreConf.TCPConfig{}
		if obfs == "http" {
			obfsPath := p.ObfsPath
			if obfsPath == "" {
				obfsPath = "/"
			}
			httpHeader := map[string]interface{}{
				"type":    "http",
				"request": map[string]interface{}{},
			}
			request := httpHeader["request"].(map[string]interface{})
			request["path"] = []string{obfsPath}
			if p.ObfsHost != "" {
				request["headers"] = map[string]interface{}{
					"Host": []string{p.ObfsHost},
				}
			}
			headerJSON, err := json.Marshal(httpHeader)
			if err != nil {
				return nil, fmt.Errorf("marshal tcp http header error: %s", err)
			}
			stream.TCPSettings.HeaderConfig = json.RawMessage(headerJSON)
		}
	case "ws", "websocket":
		stream.WSSettings = &coreConf.WebSocketConfig{
			Host: p.Host,
			Path: p.Path,
		}
	case "grpc":
		stream.GRPCSettings = &coreConf.GRPCConfig{
			ServiceName: p.ServiceName,
		}
	case "httpupgrade":
		stream.HTTPUPGRADESettings = &coreConf.HttpUpgradeConfig{
			Host: p.Host,
			Path: p.Path,
		}
	case "splithttp", "xhttp":
		xhttpExtra := p.XHTTPExtra
		if xhttpExtra == "" {
			xhttpExtra = "{}"
		}
		stream.SplitHTTPSettings = &coreConf.SplitHTTPConfig{
			Host:  p.Host,
			Path:  p.Path,
			Mode:  p.XHTTPMode,
			Extra: json.RawMessage(xhttpExtra),
		}
	default:
		return nil, fmt.Errorf("unsupported transport type: %s", transport)
	}
	applyProxyProtocolSockopt(stream, p.ProxyProtocol)
	applyTrustedXForwardedFor(stream, p.TrustedXForwardedFor)
	return stream, nil
}

// applyTrustedXForwardedFor 套 CDN / 反代时识别用户真实 IP。新版 Xray 默认不信任
// X-Forwarded-For，只有请求带着这里任一个请求头时才采用（WS/HTTPUpgrade/XHTTP/gRPC 生效）。
func applyTrustedXForwardedFor(stream *coreConf.StreamConfig, headers []string) {
	if stream == nil || len(headers) == 0 {
		return
	}
	if stream.SocketSettings == nil {
		stream.SocketSettings = &coreConf.SocketConfig{}
	}
	stream.SocketSettings.TrustedXForwardedFor = headers
}

func applyProxyProtocolSockopt(stream *coreConf.StreamConfig, enabled bool) {
	if stream == nil || !enabled {
		return
	}
	if stream.SocketSettings == nil {
		stream.SocketSettings = &coreConf.SocketConfig{}
	}
	stream.SocketSettings.AcceptProxyProtocol = true
}

func buildVLess(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "vless"
	decryption, err := vlessDecryption(nodeInfo.Protocol)
	if err != nil {
		return err
	}
	s, err := json.Marshal(&coreConf.VLessInboundConfig{
		Decryption: decryption,
	})
	if err != nil {
		return fmt.Errorf("marshal vless config error: %s", err)
	}
	inbound.Settings = (*json.RawMessage)(&s)
	inbound.StreamSetting, err = buildStreamSetting(nodeInfo.Protocol)
	return err
}

// defaultVLESSTicket 0-RTT 未填票据时间时使用的有效期（秒）
const defaultVLESSTicket = "600"

// vlessDecryption 按 Xray 格式拼服务端 decryption：
// mlkem768x25519plus.<模式>.<票据秒数>s[.<填充>].<私钥>
// 1-RTT 的票据段固定为 0s；0-RTT 取面板票据时间，兼容带或不带 s 后缀，留空用默认值。
func vlessDecryption(p *panel.Protocol) (string, error) {
	switch p.Encryption {
	case "", "none":
		return "none", nil
	case "mlkem768x25519plus":
	default:
		return "", logreport.Wrap(logreport.CodeVlessEncUnsupported, logreport.StageInbound,
			fmt.Errorf("vless decryption method %s is not support", p.Encryption))
	}
	mode := strings.TrimSpace(p.EncryptionMode)
	if mode == "" {
		return "", logreport.Wrap(logreport.CodeVlessEncModeEmpty, logreport.StageInbound,
			fmt.Errorf("vless encryption mode is empty"))
	}
	privateKey := strings.TrimSpace(p.EncryptionPrivateKey)
	if privateKey == "" {
		return "", logreport.Wrap(logreport.CodeVlessEncKeyEmpty, logreport.StageInbound,
			fmt.Errorf("vless encryption private key is empty, generate a key pair in the panel"))
	}
	ticket := "0"
	if strings.TrimSpace(p.EncryptionRTT) == "0rtt" {
		ticket = strings.TrimSuffix(strings.TrimSpace(p.EncryptionTicket), "s")
		if ticket == "" {
			ticket = defaultVLESSTicket
		}
	}
	parts := []string{"mlkem768x25519plus", mode, ticket + "s"}
	if padding := strings.TrimSpace(p.EncryptionServerPadding); padding != "" {
		parts = append(parts, padding)
	}
	parts = append(parts, privateKey)
	return strings.Join(parts, "."), nil
}

func buildVMess(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "vmess"
	s, err := json.Marshal(&coreConf.VMessInboundConfig{})
	if err != nil {
		return fmt.Errorf("marshal vmess settings error: %s", err)
	}
	inbound.Settings = (*json.RawMessage)(&s)
	inbound.StreamSetting, err = buildStreamSetting(nodeInfo.Protocol)
	return err
}

func buildTrojan(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "trojan"
	s, err := json.Marshal(&coreConf.TrojanServerConfig{})
	if err != nil {
		return fmt.Errorf("marshal trojan settings error: %s", err)
	}
	inbound.Settings = (*json.RawMessage)(&s)
	inbound.StreamSetting, err = buildStreamSetting(nodeInfo.Protocol)
	return err
}

func buildShadowsocks(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "shadowsocks"
	cipher := nodeInfo.Protocol.Cipher
	settings := &coreConf.ShadowsocksServerConfig{
		Cipher: cipher,
	}
	// SS2022 的占位用户密钥必须和加密算法的密钥长度一致（128 位 16 字节，256 位 32 字节）
	keyLength := 32
	if cipher == "2022-blake3-aes-128-gcm" {
		keyLength = 16
	}
	p := make([]byte, keyLength)
	_, err := rand.Read(p)
	if err != nil {
		return fmt.Errorf("generate random password error: %s", err)
	}
	randomPasswd := hex.EncodeToString(p)

	if nodeInfo.Protocol.ServerKey != "" && strings.Contains(cipher, "2022") {
		nodeInfo.Protocol.ServerKey = base64.StdEncoding.EncodeToString([]byte(nodeInfo.Protocol.ServerKey))
		settings.Password = nodeInfo.Protocol.ServerKey
		randomPasswd = base64.StdEncoding.EncodeToString(p)
		cipher = ""
	}
	defaultSSuser := &coreConf.ShadowsocksUserConfig{
		Cipher:   cipher,
		Password: randomPasswd,
	}
	settings.Users = append(settings.Users, defaultSSuser)
	if normalizeOptionalSetting(nodeInfo.Protocol.Obfs) == "http" {
		settings.NetworkList = &coreConf.NetworkList{"tcp"}
	} else {
		settings.NetworkList = &coreConf.NetworkList{"tcp", "udp"}
	}

	inbound.StreamSetting, err = buildStreamSetting(nodeInfo.Protocol)
	if err != nil {
		return err
	}

	sets, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal shadowsocks settings error: %s", err)
	}
	inbound.Settings = (*json.RawMessage)(&sets)
	return nil
}

func normalizeOptionalSetting(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", "none", "null", "false":
		return ""
	default:
		return value
	}
}

func normalizeTransport(value string) string {
	value = normalizeOptionalSetting(value)
	if value == "" {
		return "tcp"
	}
	return value
}

// applyQUICALPN 给 Hysteria2 入站固定 ALPN 为 h3。
// Xray 在未配置 ALPN 时默认 h2/http1.1，Hysteria2 客户端（mihomo、sing-box、官方客户端）
// 握手时只发 h3，服务端会以 "no application protocol" 拒绝，节点完全无法连接。
func applyQUICALPN(nodeType string, tlsSettings *coreConf.TLSConfig) {
	if tlsSettings == nil {
		return
	}
	switch nodeType {
	case "hysteria2", "hysteria":
		if tlsSettings.ALPN == nil || len(*tlsSettings.ALPN) == 0 {
			alpn := coreConf.StringList{"h3"}
			tlsSettings.ALPN = &alpn
		}
	}
}

func buildHysteria2(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "hysteria"
	settings := &coreConf.HysteriaServerConfig{
		Version: 2,
	}
	t := coreConf.TransportProtocol("hysteria")
	up := coreConf.Bandwidth(strconv.Itoa(nodeInfo.Protocol.UpMbps) + "mbps")
	down := coreConf.Bandwidth(strconv.Itoa(nodeInfo.Protocol.DownMbps) + "mbps")
	inbound.StreamSetting = &coreConf.StreamConfig{Network: &t}
	hysteriasetting := &coreConf.HysteriaConfig{
		Version: 2,
	}

	finalmask := &coreConf.FinalMask{}
	obfs := nodeInfo.Protocol.Obfs
	obfs_password := nodeInfo.Protocol.ObfsPassword
	if obfs != "" {
		if obfs == "none" {
			obfs = ""
			obfs_password = ""
		}
	}
	if nodeInfo.Protocol.UpMbps > 0 || nodeInfo.Protocol.DownMbps > 0 {
		finalmask.QuicParams = &coreConf.QuicParamsConfig{
			Congestion: "force-brutal",
			BrutalUp:   up,
			BrutalDown: down,
		}
	}
	if obfs != "" && obfs_password != "" {
		if finalmask == nil {
			finalmask = &coreConf.FinalMask{}
		}
		rawobfsJSON := json.RawMessage(fmt.Sprintf(`{"password":"%s"}`, obfs_password))
		finalmask.Udp = []coreConf.Mask{
			{
				Type:     obfs,
				Settings: &rawobfsJSON,
			},
		}
	}

	inbound.StreamSetting.FinalMask = finalmask
	sets, err := json.Marshal(settings)
	inbound.Settings = (*json.RawMessage)(&sets)
	inbound.StreamSetting.HysteriaSettings = hysteriasetting
	if err != nil {
		return fmt.Errorf("marshal hysteria2 settings error: %s", err)
	}
	return nil
}

func buildTuic(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "tuic"
	settings := &coreConf.TuicServerConfig{
		CongestionControl: nodeInfo.Protocol.CongestionController,
		ZeroRttHandshake:  nodeInfo.Protocol.ReduceRTT,
	}
	t := coreConf.TransportProtocol("tuic")
	inbound.StreamSetting = &coreConf.StreamConfig{Network: &t}
	sets, err := json.Marshal(settings)
	inbound.Settings = (*json.RawMessage)(&sets)
	if err != nil {
		return fmt.Errorf("marshal tuic settings error: %s", err)
	}
	return nil
}

func buildAnyTLS(nodeInfo *panel.NodeInfo, inbound *coreConf.InboundDetourConfig) error {
	inbound.Protocol = "anytls"
	var padding []string
	//nodeInfo.Protocol.PaddingScheme "stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"
	if nodeInfo.Protocol.PaddingScheme != "" {
		padding = strings.Split(nodeInfo.Protocol.PaddingScheme, "\n")
	}
	settings := &coreConf.AnyTLSServerConfig{
		PaddingScheme: padding,
	}
	t := coreConf.TransportProtocol("tcp")
	inbound.StreamSetting = &coreConf.StreamConfig{Network: &t}
	applyProxyProtocolSockopt(inbound.StreamSetting, nodeInfo.Protocol.ProxyProtocol)
	sets, err := json.Marshal(settings)
	inbound.Settings = (*json.RawMessage)(&sets)
	if err != nil {
		return fmt.Errorf("marshal anytls settings error: %s", err)
	}
	return nil
}

// applyECH 面板开启 ECH 时把服务端密钥交给 Xray（echServerKeys）。只用于 TCP 上的 TLS；
// 基于 QUIC 的协议（Hysteria2 / TUIC）不处理。
func applyECH(nodeInfo *panel.NodeInfo, tlsSettings *coreConf.TLSConfig) {
	if tlsSettings == nil || nodeInfo == nil || nodeInfo.Protocol == nil {
		return
	}
	if !nodeInfo.Protocol.EchEnabled || strings.TrimSpace(nodeInfo.Protocol.EchServerKeys) == "" {
		return
	}
	switch nodeInfo.Type {
	case "vless", "vmess", "trojan", "anytls":
		tlsSettings.ECHServerKeys = strings.TrimSpace(nodeInfo.Protocol.EchServerKeys)
	}
}
