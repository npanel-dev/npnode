package node

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	vCore "github.com/npanel-dev/npnode/core"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

func TestFileCertificateReloadWaitsForMatchingKey(t *testing.T) {
	c, certFile, keyFile, _ := newCertTestController(t, "file", "example.com")
	c.server = &vCore.XrayCore{ReloadCh: make(chan struct{}, 1)}
	writeCASigned(t, certFile, keyFile, "example.com", 24*time.Hour)
	var err error
	c.fileCertHash, err = c.fileCertDigest()
	if err != nil {
		t.Fatal(err)
	}
	oldKey, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	c.renewCertTask(context.Background())
	if len(c.server.ReloadCh) != 0 {
		t.Fatal("unchanged certificate triggered reload")
	}
	writeCASigned(t, certFile, keyFile, "example.com", 48*time.Hour)
	newKey, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, oldKey, 0o600); err != nil {
		t.Fatal(err)
	}
	c.renewCertTask(context.Background())
	if len(c.server.ReloadCh) != 0 {
		t.Fatal("mismatched certificate and key triggered reload")
	}
	if err := os.WriteFile(keyFile, newKey, 0o600); err != nil {
		t.Fatal(err)
	}
	c.renewCertTask(context.Background())
	select {
	case <-c.server.ReloadCh:
	default:
		t.Fatal("replacement certificate did not trigger reload")
	}
	c.renewCertTask(context.Background())
	if len(c.server.ReloadCh) != 0 {
		t.Fatal("unchanged replacement triggered duplicate reload")
	}
}

// writeCASigned 写一张由测试 CA 签发的证书（非自签）
func writeCASigned(t *testing.T, certFile, keyFile, domain string, validFor time.Duration) {
	t.Helper()
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Test CA"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour)}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: domain}, DNSNames: []string{domain},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(validFor)}
	der, err := x509.CreateCertificate(rand.Reader, leaf, caCert, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644)
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600)
}

func certSubject(t *testing.T, certFile string) (cn string, selfSigned bool) {
	t.Helper()
	raw, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	block, rest := pem.Decode(raw)
	if block == nil || len(rest) != 0 {
		t.Fatalf("certificate file must hold exactly one PEM block (rest=%d bytes)", len(rest))
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert.Subject.CommonName, cert.Issuer.String() == cert.Subject.String()
}

type certTestEnv struct {
	c        *Controller
	certFile string
	keyFile  string
	calls    *int
	reports  *[]*nodev1.CertReport
}

func newCertTestController(t *testing.T, mode, sni string) (*Controller, string, string, *int) {
	env := newCertTestEnv(t, mode, sni)
	return env.c, env.certFile, env.keyFile, env.calls
}

func newCertTestEnv(t *testing.T, mode, sni string) certTestEnv {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "x.cer"), filepath.Join(dir, "x.key")
	oldPaths, oldObtain, oldSend, oldRetry := certPathsFor, obtainCert, sendCertReport, certRetryInterval
	t.Cleanup(func() {
		certPathsFor, obtainCert, sendCertReport, certRetryInterval = oldPaths, oldObtain, oldSend, oldRetry
	})
	certPathsFor = func(*Controller) (string, string) { return certFile, keyFile }
	calls := 0
	obtainCert = func(*Controller) error { calls++; return errors.New("acme unavailable") }
	var reports []*nodev1.CertReport
	var mu sync.Mutex
	sendCertReport = func(_ *Controller, r *nodev1.CertReport) { mu.Lock(); reports = append(reports, r); mu.Unlock() }
	c := &Controller{tag: "t", info: &panel.NodeInfo{Type: "hysteria", Protocol: &panel.Protocol{CertMode: mode, SNI: sni, Port: 5553}}}
	return certTestEnv{c: c, certFile: certFile, keyFile: keyFile, calls: &calls, reports: &reports}
}

func lastReport(t *testing.T, env certTestEnv) *nodev1.CertReport {
	t.Helper()
	if len(*env.reports) == 0 {
		t.Fatal("no certificate report sent")
	}
	return (*env.reports)[len(*env.reports)-1]
}

func TestRequestCertReplacesSelfSignedWhenSwitchingToHTTP(t *testing.T) {
	env := newCertTestEnv(t, "http", "hysteria-20261115-1.bearru.cc")
	// self 模式留下的自签证书（旧 SNI）
	if err := generateSelfSslCertificate("baidu.com", env.certFile, env.keyFile); err != nil {
		t.Fatal(err)
	}
	obtainCert = func(*Controller) error {
		*env.calls++
		writeCASigned(t, env.certFile, env.keyFile, "hysteria-20261115-1.bearru.cc", 90*24*time.Hour)
		return nil
	}
	if err := env.c.requestCert(); err != nil {
		t.Fatal(err)
	}
	if r := lastReport(t, env); *env.calls != 1 || !r.GetOk() || r.GetError() != "" || r.GetNotAfter() == 0 ||
		r.GetDomain() != "hysteria-20261115-1.bearru.cc" || r.GetPort() != 5553 || r.GetProtocol() != "hysteria" || r.GetMode() != "http" {
		t.Fatalf("calls=%d report=%v", *env.calls, r)
	}
	// 有效证书：不再申请
	if err := env.c.requestCert(); err != nil || *env.calls != 1 {
		t.Fatalf("valid cert must be reused, calls=%d err=%v", *env.calls, err)
	}
}

func TestRequestCertFailsWithoutSelfSignedFallback(t *testing.T) {
	// 没有证书、申请失败：入站不启动，也不生成自签证书
	env := newCertTestEnv(t, "http", "a.example.com")
	err := env.c.requestCert()
	if !errors.Is(err, errCertPending) {
		t.Fatalf("err = %v, want errCertPending", err)
	}
	if _, statErr := os.Stat(env.certFile); !os.IsNotExist(statErr) {
		t.Fatal("must not create a self-signed fallback certificate")
	}
	if r := lastReport(t, env); r.GetOk() || !strings.Contains(r.GetError(), "acme unavailable") {
		t.Fatalf("report = %v", r)
	}

	// 只有旧的自签证书：同样不可用，保持文件不变
	env2 := newCertTestEnv(t, "http", "b.example.com")
	_ = generateSelfSslCertificate("baidu.com", env2.certFile, env2.keyFile)
	before, _ := os.ReadFile(env2.certFile)
	if err := env2.c.requestCert(); !errors.Is(err, errCertPending) {
		t.Fatalf("self-signed leftover: err = %v", err)
	}
	after, _ := os.ReadFile(env2.certFile)
	if string(before) != string(after) || lastReport(t, env2).GetOk() {
		t.Fatal("self-signed leftover must not be used or replaced")
	}

	// 可信证书但域名不对：不可用
	env3 := newCertTestEnv(t, "dns", "c.example.com")
	writeCASigned(t, env3.certFile, env3.keyFile, "other.example.com", 60*24*time.Hour)
	if err := env3.c.requestCert(); !errors.Is(err, errCertPending) {
		t.Fatalf("wrong-domain cert: err = %v", err)
	}
}

func TestRequestCertKeepsTrustedCertWhenRenewalFails(t *testing.T) {
	env := newCertTestEnv(t, "dns", "a.example.com")
	writeCASigned(t, env.certFile, env.keyFile, "a.example.com", 10*24*time.Hour)
	if err := env.c.requestCert(); err != nil {
		t.Fatalf("trusted cert still valid must be used, err=%v", err)
	}
	if r := lastReport(t, env); *env.calls != 1 || !r.GetOk() || !strings.Contains(r.GetError(), "续期失败") {
		t.Fatalf("calls=%d report=%v", *env.calls, r)
	}
}

func TestSelfModeRegeneratesWhenSNIChanges(t *testing.T) {
	c, certFile, keyFile, calls := newCertTestController(t, "self", "short.cn")
	// 先写一张长域名的证书，再换成短域名：文件必须被完整覆盖（不能残留旧内容）
	if err := generateSelfSslCertificate("a-very-long-domain-name-for-the-old-certificate.example.com", certFile, keyFile); err != nil {
		t.Fatal(err)
	}
	if err := c.requestCert(); err != nil {
		t.Fatal(err)
	}
	if cn, self := certSubject(t, certFile); cn != "short.cn" || !self || *calls != 0 {
		t.Fatalf("self cert cn=%q self=%v calls=%d", cn, self, *calls)
	}
	// SNI 没变：不重新生成
	before, _ := os.ReadFile(certFile)
	_ = c.requestCert()
	after, _ := os.ReadFile(certFile)
	if string(before) != string(after) {
		t.Fatal("self cert must be reused when SNI is unchanged")
	}
}

func TestCertRetryReloadsOnceCertificateIsIssued(t *testing.T) {
	env := newCertTestEnv(t, "http", "a.example.com")
	reload := make(chan struct{}, 1)
	env.c.server = &vCore.XrayCore{ReloadCh: reload}
	certRetryInterval = 20 * time.Millisecond
	var mu sync.Mutex
	attempts := 0
	obtainCert = func(*Controller) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts < 3 {
			return errors.New("dns not propagated yet")
		}
		writeCASigned(t, env.certFile, env.keyFile, "a.example.com", 90*24*time.Hour)
		return nil
	}
	env.c.startCertRetry()
	defer env.c.stopCertRetry()
	select {
	case <-reload:
	case <-time.After(3 * time.Second):
		t.Fatal("retry did not trigger a reload after the certificate was issued")
	}
}

func TestCloseSkipsInboundThatNeverStarted(t *testing.T) {
	env := newCertTestEnv(t, "http", "a.example.com")
	certRetryInterval = time.Hour
	env.c.startCertRetry()
	// 入站没启动（证书不可用）：Close 只停止重试，不去删除内核里不存在的入站
	if err := env.c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if env.c.certRetryStop != nil {
		t.Fatal("retry must be stopped on close")
	}
	n := &Node{controllers: []*Controller{env.c}}
	env.c.grpcClient = &panel.GrpcClient{}
	env.c.info.Protocol.Enable = true
	if got := n.SyncInbounds(); len(got) != 0 {
		t.Fatalf("an inbound that never started must not subscribe to user pushes: %v", got)
	}
}

func TestRenewCertTaskRetriesUntrustedCert(t *testing.T) {
	env := newCertTestEnv(t, "http", "a.example.com")
	if err := generateSelfSslCertificate("a.example.com", env.certFile, env.keyFile); err != nil {
		t.Fatal(err)
	}
	_ = env.c.renewCertTask(nil)
	if *env.calls != 1 {
		t.Fatalf("self-signed cert in http mode must be re-requested by the daily task, calls=%d", *env.calls)
	}
	writeCASigned(t, env.certFile, env.keyFile, "a.example.com", 90*24*time.Hour)
	_ = env.c.renewCertTask(nil)
	if *env.calls != 1 {
		t.Fatalf("valid cert must not be re-requested, calls=%d", *env.calls)
	}
}
