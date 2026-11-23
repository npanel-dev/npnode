package core

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	gotls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	coreConf "github.com/xtls/xray-core/infra/conf"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

// 面板（NPanel-backend internal/pkg/ech）生成的一组 ECH 密钥，外层域名 cover.example.net
const (
	testECHServerKeys = "ACC02G4UTBc591fx6IHWdoJve1PLurojuGQUKncpzH2QewBk/g0AYAAAIAAgfSLLvAGidD7Zs4v+4u8Gh/FxxIINtmKNC8P2wbnXk2gAJAABAAEAAQACAAEAAwACAAEAAgACAAIAAwADAAEAAwACAAMAAwARY292ZXIuZXhhbXBsZS5uZXQAAA=="
	testECHConfigList = "AGT+DQBgAAAgACB9Isu8AaJ0Ptmzi/7i7waH8XHEgg22Yo0Lw/bBudeTaAAkAAEAAQABAAIAAQADAAIAAQACAAIAAgADAAMAAQADAAIAAwADABFjb3Zlci5leGFtcGxlLm5ldAAA"
)

func writeTestCert(t *testing.T, dir string) (string, string, *x509.Certificate) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "real.example.com"},
		DNSNames: []string{"real.example.com", "cover.example.net"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	leaf, _ := x509.ParseCertificate(der)
	return certFile, keyFile, leaf
}

// 面板下发的 ech_server_keys 经 applyECH 写进 Xray TLS 配置，再由 Xray 自己的代码生成 tls.Config，
// 用标准库客户端带面板下发给订阅的 ech_config_list 握手：ECH 必须被接受。
func TestECHServerKeysAcceptedByXray(t *testing.T) {
	certFile, keyFile, leaf := writeTestCert(t, t.TempDir())
	tlsSettings := &coreConf.TLSConfig{Certs: []*coreConf.TLSCertConfig{buildTLSCertificate(certFile, keyFile)}}
	applyECH(&panel.NodeInfo{Type: "vless", Protocol: &panel.Protocol{EchEnabled: true, EchServerKeys: testECHServerKeys}}, tlsSettings)
	if tlsSettings.ECHServerKeys == "" {
		t.Fatal("applyECH did not set echServerKeys")
	}
	built, err := tlsSettings.Build()
	if err != nil {
		t.Fatalf("xray rejected tls config: %v", err)
	}
	serverCfg := built.(*xtls.Config).GetTLSConfig()
	if len(serverCfg.EncryptedClientHelloKeys) == 0 {
		t.Fatal("xray tls config has no ECH keys")
	}
	serverCfg.MinVersion = gotls.VersionTLS13

	ln, err := gotls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.(*gotls.Conn).Handshake()
			c.Close()
		}
	}()
	list, _ := base64.StdEncoding.DecodeString(testECHConfigList)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	conn, err := gotls.Dial("tcp", ln.Addr().String(), &gotls.Config{ServerName: "real.example.com", RootCAs: pool,
		MinVersion: gotls.VersionTLS13, EncryptedClientHelloConfigList: list})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer conn.Close()
	if !conn.ConnectionState().ECHAccepted {
		t.Fatal("ECH not accepted by xray tls config")
	}

	// QUIC 协议和关闭时不写
	other := &coreConf.TLSConfig{}
	applyECH(&panel.NodeInfo{Type: "hysteria2", Protocol: &panel.Protocol{EchEnabled: true, EchServerKeys: testECHServerKeys}}, other)
	applyECH(&panel.NodeInfo{Type: "vless", Protocol: &panel.Protocol{EchEnabled: false, EchServerKeys: testECHServerKeys}}, other)
	if other.ECHServerKeys != "" {
		t.Fatal("echServerKeys set for unsupported case")
	}
}
