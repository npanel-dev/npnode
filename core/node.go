package core

import (
	"fmt"

	"github.com/npanel-dev/npnode/api/panel"
)

func (v *XrayCore) AddNode(tag string, info *panel.NodeInfo) error {
	if isPlainProxy(info.Type) {
		plainProxies.mu.Lock()
		defer plainProxies.mu.Unlock()
		plainProxies.inbounds[tag] = &plainProxyInbound{info: info, users: map[string]bool{}}
	}
	inBoundConfig, err := buildInbound(info, tag)
	if err != nil {
		return fmt.Errorf("build inbound error: %s", err)
	}
	err = v.addInbound(inBoundConfig)
	if err != nil {
		return fmt.Errorf("add inbound error: %s", err)
	}
	return nil
}

func (v *XrayCore) DelNode(tag string) error {
	plainProxies.mu.Lock()
	delete(plainProxies.inbounds, tag)
	plainProxies.mu.Unlock()
	// 入站已移除：不再有人读这些已移除用户的计数器，不留指针
	v.users.mapLock.Lock()
	delete(v.users.retired, tag)
	v.users.mapLock.Unlock()
	err := v.removeInbound(tag)
	if err != nil {
		return fmt.Errorf("remove in error: %s", err)
	}
	return nil
}
