package logreport

import nodev1 "github.com/npanel-dev/npnode/proto/node/v1"

// Resolve 的 scope 取值。
const (
	ScopeServer  = "server"  // 节点整体启动成功
	ScopeInbound = "inbound" // 某入站启动成功
	ScopeCert    = "cert"    // 某入站证书可用
)

// Default 为进程级 Reporter，由启动流程设置；为 nil 时包级便捷函数无操作。
var Default *Reporter

// Resolve 通过 Default 上报恢复通知；active 为 scope=server 时当前已启动的入站 "protocol:port"。
func Resolve(scope, protocol string, port int, active []string) {
	r := Default
	if r == nil {
		return
	}
	r.Resolve(&nodev1.LogResolve{
		Scope:          scope,
		Protocol:       protocol,
		Port:           uint32(port),
		ActiveInbounds: active,
		ResolvedAt:     r.now().UnixMilli(),
	})
}
