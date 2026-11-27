package panel

import "sync/atomic"

// 能力声明（设备名额对接）。
// 打开 seat_v1 后面板对本节点：用户条目 speed_limit 改按 bit/s 下发，Sync 推送启用分片。
// 本节点对应地按 bit/s 解释限速、重组分片，所以三者必须一起生效：开关只在这里，所有客户端和限速器都读它。
// 关闭（默认）时，与面板的请求和限速口径与旧版完全一致：不带 capabilities，也不带 X-Node-Agent。

// CapabilitySeatV1 设备名额第一版能力
const CapabilitySeatV1 = "seat_v1"

var seatV1 atomic.Bool

// SetSeatV1 设置是否声明 seat_v1；须在创建面板客户端之前调用（启动、重载时由 cmd 按配置设置）
func SetSeatV1(on bool) {
	seatV1.Store(on)
}

// SeatV1Enabled 本进程是否声明了 seat_v1
func SeatV1Enabled() bool {
	return seatV1.Load()
}

// CapabilityParam HTTP 查询参数 capabilities 的值；未声明任何能力时为空，调用方不得附加该参数
func CapabilityParam() string {
	if SeatV1Enabled() {
		return CapabilitySeatV1
	}
	return ""
}

// CapabilityList gRPC 请求的 capabilities 字段；未声明时为 nil，线上编码与旧版一致
func CapabilityList() []string {
	if SeatV1Enabled() {
		return []string{CapabilitySeatV1}
	}
	return nil
}

// NodeAgent X-Node-Agent / x-node-agent 的值；未声明能力时为空，调用方不得发送该头
func NodeAgent() string {
	if !SeatV1Enabled() {
		return ""
	}
	if NodeVersion == "" {
		return "npnode"
	}
	return "npnode/" + NodeVersion
}
