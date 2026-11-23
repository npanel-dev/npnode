package dispatcher

import "github.com/xtls/xray-core/common/session"

// Bind default direct traffic to the local entry IP, not the client's IP.
// Leave proxy chains and explicitly selected source addresses unchanged.
func bindDefaultSource(inbound *session.Inbound, outbound *session.Outbound) {
	if outbound.Tag != "Default" || outbound.Gateway != nil || inbound == nil || !inbound.Local.IsValid() || !outbound.Target.IsValid() {
		return
	}
	source := inbound.Local.Address
	if !source.Family().IsIP() {
		return
	}
	target := outbound.Target.Address
	if target.Family().IsIP() && target.Family() != source.Family() {
		return
	}
	ip := source.IP()
	if !ip.IsGlobalUnicast() {
		// A local reverse proxy must not force Internet traffic out of loopback.
		// Wildcard UDP listeners also lack a usable per-connection entry IP.
		if !ip.IsLoopback() || !target.Family().IsIP() || !target.IP().IsLoopback() {
			return
		}
	}
	outbound.Gateway = source
}
