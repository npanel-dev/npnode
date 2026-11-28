package panel

import (
	"strings"
	"sync/atomic"
)

// 能力声明（设备名额对接）。
// 打开 seat_v1 后面板对本节点：用户条目 speed_limit 改按 bit/s 下发，Sync 推送启用分片。
// 本节点对应地按 bit/s 解释限速、重组分片，所以三者必须一起生效：开关只在这里，所有客户端和限速器都读它。
// online_v1：在线上报改为基于活跃连接、周期不超过 60 秒（面板据此把本实例的在线数据视为完整数据）。
// admission_v1：实时准入（A1～A7）。面板按 admission_v1 单独渲染条目的 device_limit，准入接口也只看 admission_v1，
// 不依赖 seat_v1，所以三项相互独立。
// 都关闭（默认）时，与面板的请求和限速口径与旧版完全一致：不带 capabilities，也不带 X-Node-Agent。

// CapabilitySeatV1 设备名额第一版能力
const CapabilitySeatV1 = "seat_v1"

// CapabilityOnlineV1 在线上报基于活跃连接
const CapabilityOnlineV1 = "online_v1"

// CapabilityAdmissionV1 实时准入（session/check、session/release）
const CapabilityAdmissionV1 = "admission_v1"

var seatV1 atomic.Bool
var onlineV1 atomic.Bool
var admissionV1 atomic.Bool

// SetSeatV1 设置是否声明 seat_v1；须在创建面板客户端之前调用（启动、重载时由 cmd 按配置设置）
func SetSeatV1(on bool) {
	seatV1.Store(on)
}

// SeatV1Enabled 本进程是否声明了 seat_v1
func SeatV1Enabled() bool {
	return seatV1.Load()
}

// SetOnlineV1 设置是否声明 online_v1；与 SetSeatV1 一样须在创建面板客户端和限速器之前调用
func SetOnlineV1(on bool) {
	onlineV1.Store(on)
}

// OnlineV1Enabled 本进程是否声明了 online_v1
func OnlineV1Enabled() bool {
	return onlineV1.Load()
}

// SetAdmissionV1 设置是否声明 admission_v1；与 SetSeatV1 一样须在创建面板客户端和限速器之前调用
func SetAdmissionV1(on bool) {
	admissionV1.Store(on)
}

// AdmissionV1Enabled 本进程是否声明了 admission_v1
func AdmissionV1Enabled() bool {
	return admissionV1.Load()
}

// CapabilityParam HTTP 查询参数 capabilities 的值；未声明任何能力时为空，调用方不得附加该参数
func CapabilityParam() string {
	return strings.Join(CapabilityList(), ",")
}

// CapabilityList gRPC 请求的 capabilities 字段，顺序固定 seat_v1、online_v1、admission_v1；未声明时为 nil，线上编码与旧版一致
func CapabilityList() []string {
	var caps []string
	if SeatV1Enabled() {
		caps = append(caps, CapabilitySeatV1)
	}
	if OnlineV1Enabled() {
		caps = append(caps, CapabilityOnlineV1)
	}
	if AdmissionV1Enabled() {
		caps = append(caps, CapabilityAdmissionV1)
	}
	return caps
}

// NodeAgent X-Node-Agent / x-node-agent 的值；未声明任何能力时为空，调用方不得发送该头
func NodeAgent() string {
	if !SeatV1Enabled() && !OnlineV1Enabled() && !AdmissionV1Enabled() {
		return ""
	}
	if NodeVersion == "" {
		return "npnode"
	}
	return "npnode/" + NodeVersion
}
