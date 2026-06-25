package panel

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"time"
)

type NodeInfo struct {
	Id                     int
	Type                   string
	PushInterval           int
	PullInterval           int
	TrafficReportThreshold int
	Protocol               *Protocol
}

type ServerPushStatusRequest struct {
	Cpu       float64 `json:"cpu"`
	Mem       float64 `json:"mem"`
	Disk      float64 `json:"disk"`
	UpdatedAt int64   `json:"updated_at"`
}

type NodeStatus struct {
	CPU    float64
	Mem    float64
	Disk   float64
	Uptime uint64
}

func (n *NodeInfo) CertFilePath() string {
	return filepath.Join("/etc/NPanel-node/", n.certBaseName()+".cer")
}

func (n *NodeInfo) KeyFilePath() string {
	return filepath.Join("/etc/NPanel-node/", n.certBaseName()+".key")
}

func (n *NodeInfo) LegacyCertFilePath() string {
	return filepath.Join("/etc/NPanel-node/", n.legacyCertBaseName()+".cer")
}

func (n *NodeInfo) LegacyKeyFilePath() string {
	return filepath.Join("/etc/NPanel-node/", n.legacyCertBaseName()+".key")
}

func (n *NodeInfo) certBaseName() string {
	base := n.legacyCertBaseName()
	if n != nil && n.Protocol != nil && n.Protocol.Port > 0 {
		return base + "-" + strconv.Itoa(n.Protocol.Port)
	}
	return base
}

func (n *NodeInfo) legacyCertBaseName() string {
	if n == nil {
		return ""
	}
	return n.Type + strconv.Itoa(n.Id)
}

func (c *ClientV1) ReportNodeStatus(nodeStatus *NodeStatus) (err error) {
	p := "/v1/server/status"
	status := ServerPushStatusRequest{
		Cpu:       nodeStatus.CPU,
		Mem:       nodeStatus.Mem,
		Disk:      nodeStatus.Disk,
		UpdatedAt: time.Now().UnixMilli(),
	}
	if _, err = c.Client.R().SetBody(status).ForceContentType("application/json").Post(p); err != nil {
		return fmt.Errorf("访问 %s 失败: %v", path.Join(c.APIHost+p), err.Error())
	}
	return nil
}
