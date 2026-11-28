package node

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/npanel-dev/npnode/common/file"
	"github.com/npanel-dev/npnode/common/logreport"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	log "github.com/sirupsen/logrus"
)

// certRenewBefore 证书剩余有效期少于这个时间就重新申请
const certRenewBefore = 30 * 24 * time.Hour

// certRetryInterval 申请失败的入站每隔这么久重试一次
var certRetryInterval = 10 * time.Minute

// errCertPending http / dns 模式拿不到可用证书：入站不启动（不使用自签证书），等待重试。
var errCertPending = errors.New("证书不可用，入站暂不启动")

// certInfo 现有证书的情况
type certInfo struct {
	exists     bool
	covers     bool // 包含当前 SNI
	selfSigned bool
	notAfter   time.Time
}

// trusted http / dns 模式可以使用：覆盖 SNI、不是自签、还没过期
func (ci certInfo) trusted() bool {
	return ci.exists && ci.covers && !ci.selfSigned && time.Now().Before(ci.notAfter)
}

// fresh 可用且离过期还远，不需要续期
func (ci certInfo) fresh() bool {
	return ci.trusted() && time.Until(ci.notAfter) >= certRenewBefore
}

// inspectCert 读取现有证书。证书文件按“协议+服务器+端口”命名，改了证书模式或 SNI 后文件名不变，
// 必须核对内容，否则会一直沿用旧证书（例如 self 模式留下的自签证书）。
func inspectCert(certFile, keyFile, domain string) certInfo {
	if !file.IsExist(certFile) || !file.IsExist(keyFile) {
		return certInfo{}
	}
	raw, err := os.ReadFile(certFile)
	if err != nil {
		return certInfo{}
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return certInfo{}
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return certInfo{}
	}
	return certInfo{
		exists:     true,
		covers:     domain == "" || cert.VerifyHostname(domain) == nil,
		selfSigned: cert.Issuer.String() == cert.Subject.String(),
		notAfter:   cert.NotAfter,
	}
}

// certPathsFor 证书与私钥路径（测试时替换到临时目录）
var certPathsFor = func(c *Controller) (string, string) {
	return c.info.CertFilePath(), c.info.KeyFilePath()
}

// obtainCert 用 lego 申请证书（http / dns 模式）。
var obtainCert = func(c *Controller) error {
	l, err := NewLego(c.info)
	if err != nil {
		return fmt.Errorf("create lego object error: %s", err)
	}
	if err := l.CreateCert(); err != nil {
		return fmt.Errorf("create lego cert error: %s", err)
	}
	return nil
}

// sendCertReport 发送证书状态（测试时替换）
var sendCertReport = func(c *Controller, r *nodev1.CertReport) {
	if c.grpcClient != nil {
		c.grpcClient.ReportCert(r)
	}
}

// reportCert 把证书状态同步给面板（gRPC 可用时）。
func (c *Controller) reportCert(ok bool, errMsg string, notAfter time.Time) {
	if c.info == nil || c.info.Protocol == nil {
		return
	}
	var na int64
	if !notAfter.IsZero() {
		na = notAfter.Unix()
	}
	if ok {
		// 证书可用：通知面板该入站此前的证书类问题已恢复
		logreport.Resolve(logreport.ScopeCert, c.info.Type, c.info.Protocol.Port, nil)
	}
	sendCertReport(c, &nodev1.CertReport{
		Protocol:  c.info.Type,
		Port:      uint32(c.info.Protocol.Port),
		Domain:    c.info.Protocol.SNI,
		Mode:      c.info.Protocol.CertMode,
		Ok:        ok,
		Error:     errMsg,
		NotAfter:  na,
		CheckedAt: time.Now().Unix(),
	})
}

// ensureACMECert http / dns 模式：现有证书可用就沿用，否则申请；申请失败时只在现有证书仍可信
// （覆盖 SNI、非自签、未过期）时继续使用，绝不用自签证书顶替。结果同步给面板。
// renewed 表示这次写入了新证书。
func (c *Controller) ensureACMECert() (renewed bool, err error) {
	certFile, keyFile := certPathsFor(c)
	sni := c.info.Protocol.SNI
	before := inspectCert(certFile, keyFile, sni)
	if before.fresh() {
		c.reportCert(true, "", before.notAfter)
		return false, nil
	}
	if obtainErr := obtainCert(c); obtainErr == nil {
		after := inspectCert(certFile, keyFile, sni)
		if after.trusted() {
			log.WithFields(log.Fields{"节点": c.tag, "域名": sni}).Warn("证书申请成功")
			c.reportCert(true, "", after.notAfter)
			return true, nil
		}
		err = fmt.Errorf("申请到的证书不可用（不包含 %s 或已过期）", sni)
	} else {
		err = obtainErr
	}
	if before.trusted() {
		msg := fmt.Sprintf("续期失败，继续使用现有证书（%s 到期）：%v", before.notAfter.Format("2006-01-02"), err)
		log.WithFields(log.Fields{"节点": c.tag, "域名": sni}).Error(msg)
		c.reportCert(true, msg, before.notAfter)
		return false, nil
	}
	log.WithFields(log.Fields{"节点": c.tag, "域名": sni, "err": err}).Error("申请证书失败，入站暂不启动")
	c.reportCert(false, err.Error(), time.Time{})
	return false, fmt.Errorf("%w：%v", errCertPending, err)
}

// renewCertTask 每天检查一次：证书快过期 / 不可用就重新申请，成功后重载节点使新证书生效。
func (c *Controller) renewCertTask(_ context.Context) error {
	if c.info.Protocol.CertMode == "file" {
		digest, err := c.fileCertDigest()
		if err != nil {
			log.WithField("err", err).Warn("证书文件尚未就绪，继续使用已加载的证书")
			return nil
		}
		if digest != c.fileCertHash {
			c.fileCertHash = digest
			c.triggerReload()
		}
		return nil
	}
	if mode := c.info.Protocol.CertMode; mode != "http" && mode != "dns" {
		return nil
	}
	renewed, _ := c.ensureACMECert()
	if renewed {
		c.triggerReload()
	}
	return nil
}

func (c *Controller) fileCertDigest() ([32]byte, error) {
	certFile, keyFile := certPathsFor(c)
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		certFile, keyFile = c.info.LegacyCertFilePath(), c.info.LegacyKeyFilePath()
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	for _, cert := range pair.Certificate {
		hash.Write(cert)
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func (c *Controller) triggerReload() {
	if c.server != nil && c.server.ReloadCh != nil {
		select {
		case c.server.ReloadCh <- struct{}{}:
		default:
		}
	}
}

// startCertRetry 申请失败的入站定时重试，成功后重载节点把它启动起来。
func (c *Controller) startCertRetry() {
	stop := make(chan struct{})
	c.certRetryStop = stop
	interval := certRetryInterval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, err := c.ensureACMECert(); err == nil {
					log.WithField("节点", c.tag).Warn("证书已可用，重载节点启动该入站")
					c.triggerReload()
					return
				}
			}
		}
	}()
}

func (c *Controller) stopCertRetry() {
	if c.certRetryStop != nil {
		close(c.certRetryStop)
		c.certRetryStop = nil
	}
}

// requestCert 启动入站前准备证书。
func (c *Controller) requestCert() error {
	certFile, keyFile := certPathsFor(c)
	sni := c.info.Protocol.SNI
	switch c.info.Protocol.CertMode {
	case "none", "", "file":
	case "dns", "http":
		_, err := c.ensureACMECert()
		return err
	case "self":
		if ci := inspectCert(certFile, keyFile, sni); ci.exists && ci.covers && time.Now().Before(ci.notAfter) {
			return nil
		}
		if err := generateSelfSslCertificate(sni, certFile, keyFile); err != nil {
			return fmt.Errorf("generate self cert error: %s", err)
		}
	default:
		return fmt.Errorf("unsupported certmode: %s", c.info.Protocol.CertMode)
	}
	return nil
}

func generateSelfSslCertificate(domain, certPath, keyPath string) error {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := &x509.Certificate{
		Version:      3,
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			CommonName: domain,
		},
		DNSNames:              []string{domain},
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(30, 0, 0),
	}
	cert, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return err
	}
	if err := checkPath(certPath); err != nil {
		return err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return err
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return err
	}
	return nil
}
