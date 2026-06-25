package panel

import "testing"

func TestNodeInfoCertPathsIncludeProtocolPort(t *testing.T) {
	info := &NodeInfo{
		Id:   7,
		Type: "vless",
		Protocol: &Protocol{
			Port: 8443,
		},
	}

	if got := info.CertFilePath(); got != "/etc/NPanel-node/vless7-8443.cer" {
		t.Fatalf("CertFilePath = %q", got)
	}
	if got := info.KeyFilePath(); got != "/etc/NPanel-node/vless7-8443.key" {
		t.Fatalf("KeyFilePath = %q", got)
	}
	if got := info.LegacyCertFilePath(); got != "/etc/NPanel-node/vless7.cer" {
		t.Fatalf("LegacyCertFilePath = %q", got)
	}
}

func TestNodeInfoCertPathsFallbackWithoutPort(t *testing.T) {
	info := &NodeInfo{
		Id:   7,
		Type: "vless",
	}

	if got := info.CertFilePath(); got != "/etc/NPanel-node/vless7.cer" {
		t.Fatalf("CertFilePath = %q", got)
	}
	if got := info.KeyFilePath(); got != "/etc/NPanel-node/vless7.key" {
		t.Fatalf("KeyFilePath = %q", got)
	}
}
