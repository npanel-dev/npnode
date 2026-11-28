// Package upgrade 按面板下发的请求远程升级节点程序。
//
// 安全约束：下载地址固定为官方发布仓库，面板只能指定目标版本（latest 或 x.y.z）；
// 安装包必须与同一发布里的 checksums.txt 的 SHA-256 一致；同一个请求只处理一次。
//
// 重试：查询最新版本、下载失败这类网络问题不记下请求，按指数退避在之后拉配置时重试，
// 最多 maxAttempts 次；校验不一致、版本无效等确定性错误立即记下，不再重试。
package upgrade

import (
	"archive/zip"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/npanel-dev/npnode/api/panel"
	log "github.com/sirupsen/logrus"
)

var (
	// ReleaseBase 官方发布仓库，不接受面板指定
	ReleaseBase = "https://github.com/npanel-dev/npnode/releases"
	// StateDir 记录已处理的请求，启动时设为配置文件所在目录
	StateDir = "/etc/NPanel-node"

	httpClient = &http.Client{Timeout: 5 * time.Minute}
	noRedirect = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	executable = os.Executable
	execFn     = syscall.Exec
	now        = time.Now
	mu         sync.Mutex
	// pending 因网络问题失败、等待重试的请求，受 mu 保护
	pending *pendingRequest
)

// pendingRequest 等待重试的请求：已尝试 attempts 次，next 之前不再尝试
type pendingRequest struct {
	req      *panel.UpgradeRequest
	attempts int
	next     time.Time
}

// retryableError 网络类错误，可以稍后重试
type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

func retryable(err error) error { return &retryableError{err: err} }

func isRetryable(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}

const (
	maxZipSize       = 200 << 20
	maxBinarySize    = 300 << 20
	maxChecksumsSize = 64 << 10
	stateFileName    = "upgrade_request"
	// maxAttempts 同一请求最多尝试次数（含首次）
	maxAttempts = 6
	// retryBase / retryMax 重试退避：1、2、4、8、16 分钟，封顶 30 分钟
	retryBase = time.Minute
	retryMax  = 30 * time.Minute
)

var versionPattern = regexp.MustCompile(`^\d{1,4}\.\d{1,4}\.\d{1,4}$`)

// Check 配置变化时调用：带了升级请求就执行；成功时进程被新版本替换，不会返回。
// 配置里没有升级请求（面板撤回或已过期）时，放弃等待中的重试。
// 失败只记日志，节点继续用当前版本运行。
func Check(cfg *panel.ServerConfigResponse) {
	if cfg == nil || cfg.Data == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	req := cfg.Data.Upgrade
	if req == nil {
		pending = nil
		return
	}
	if pending != nil && pending.req.RequestID != req.RequestID {
		pending = nil // 新请求重新计数
	}
	run(req)
}

// Retry 每次定时拉配置时调用：有等待重试的请求且退避时间已到就再试一次。
func Retry() {
	mu.Lock()
	defer mu.Unlock()
	if pending == nil || now().Before(pending.next) {
		return
	}
	run(pending.req)
}

// run 执行一次请求并按结果更新 pending，调用方持有 mu
func run(req *panel.UpgradeRequest) {
	err := handle(req, panel.NodeVersion)
	if err == nil || !isRetryable(err) {
		pending = nil
		if err != nil {
			log.WithField("err", err).Error("远程升级失败，继续使用当前版本")
		}
		return
	}
	attempts := 1
	if pending != nil {
		attempts = pending.attempts + 1
	}
	if attempts >= maxAttempts {
		pending = nil
		if markErr := markHandled(req.RequestID); markErr != nil {
			log.WithField("err", markErr).Error("记录升级请求失败")
		}
		log.WithFields(log.Fields{"err": err, "attempts": attempts}).
			Error("远程升级多次重试仍失败，放弃本次升级，继续使用当前版本")
		return
	}
	delay := retryBase << (attempts - 1)
	if delay > retryMax {
		delay = retryMax
	}
	pending = &pendingRequest{req: req, attempts: attempts, next: now().Add(delay)}
	log.WithFields(log.Fields{"err": err, "attempts": attempts, "retry_in": delay.String()}).
		Warn("远程升级暂时失败，稍后重试")
}

// Handle 执行一次升级请求。已处理过、或已经是目标版本时直接返回 nil。
func Handle(req *panel.UpgradeRequest, current string) error {
	mu.Lock()
	defer mu.Unlock()
	return handle(req, current)
}

func markHandled(requestID string) error {
	return os.WriteFile(filepath.Join(StateDir, stateFileName), []byte(requestID), 0o600)
}

// handle 成功、跳过或确定性失败时记下请求，同一请求不再处理；网络类失败不记，留给重试。
func handle(req *panel.UpgradeRequest, current string) (err error) {
	if req == nil || strings.TrimSpace(req.RequestID) == "" {
		return nil
	}
	if last, _ := os.ReadFile(filepath.Join(StateDir, stateFileName)); strings.TrimSpace(string(last)) == req.RequestID {
		return nil
	}
	defer func() {
		if isRetryable(err) {
			return
		}
		if markErr := markHandled(req.RequestID); markErr != nil && err == nil {
			err = fmt.Errorf("记录升级请求失败: %w", markErr)
		}
	}()

	target := strings.TrimSpace(req.Version)
	if target == "" || strings.EqualFold(target, "latest") {
		latest, err := resolveLatest()
		if err != nil {
			return err
		}
		target = latest
	}
	if !versionPattern.MatchString(target) {
		return fmt.Errorf("无效的目标版本 %q", target)
	}
	if target == current {
		log.WithField("version", current).Warn("已经是目标版本，跳过远程升级")
		return nil
	}

	asset, err := assetName()
	if err != nil {
		return err
	}
	base := ReleaseBase + "/download/" + target + "/"
	sums, err := download(base+"checksums.txt", maxChecksumsSize)
	if err != nil {
		return fmt.Errorf("下载 checksums.txt 失败: %w", err)
	}
	want, err := checksumFor(sums, asset)
	if err != nil {
		return err
	}
	zipData, err := download(base+asset, maxZipSize)
	if err != nil {
		return fmt.Errorf("下载安装包失败: %w", err)
	}
	sum := sha256.Sum256(zipData)
	if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, want) {
		return fmt.Errorf("安装包校验失败: 期望 %s，实际 %s", want, got)
	}
	bin, err := extractBinary(zipData)
	if err != nil {
		return err
	}

	exe, err := executable()
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	tmp := exe + ".new"
	if err := os.WriteFile(tmp, bin, 0o755); err != nil {
		return fmt.Errorf("写入新版本失败: %w", err)
	}
	if err := os.Rename(tmp, exe); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("替换程序失败: %w", err)
	}
	// exec 成功后不会返回，defer 来不及执行，先记下请求
	if err := markHandled(req.RequestID); err != nil {
		return fmt.Errorf("记录升级请求失败: %w", err)
	}
	log.WithFields(log.Fields{"from": current, "to": target}).Warn("远程升级完成，正在重启节点")
	return execFn(exe, os.Args, os.Environ())
}

// resolveLatest 读取 releases/latest 跳转到的版本号
func resolveLatest() (string, error) {
	resp, err := noRedirect.Get(ReleaseBase + "/latest")
	if err != nil {
		return "", retryable(fmt.Errorf("查询最新版本失败: %w", err))
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", retryable(fmt.Errorf("查询最新版本失败: HTTP %d", resp.StatusCode))
	}
	v := path.Base(strings.TrimRight(loc, "/"))
	if !versionPattern.MatchString(v) {
		return "", fmt.Errorf("无法识别最新版本 %q", v)
	}
	return v, nil
}

func assetName() (string, error) {
	switch runtime.GOARCH {
	case "amd64":
		return "npnode-linux-64.zip", nil
	case "arm64":
		return "npnode-linux-arm64-v8a.zip", nil
	case "s390x":
		return "npnode-linux-s390x.zip", nil
	}
	return "", fmt.Errorf("不支持远程升级的架构 %s", runtime.GOARCH)
}

func download(url string, limit int64) ([]byte, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, retryable(err)
	}
	defer resp.Body.Close()
	// 非 200 都可重试：包括 Release 刚创建、安装包还没传完时的 404
	if resp.StatusCode != http.StatusOK {
		return nil, retryable(fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, retryable(err)
	}
	if int64(len(data)) > limit {
		return nil, errors.New("文件过大")
	}
	return data, nil
}

func checksumFor(sums []byte, asset string) (string, error) {
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == asset && len(fields[0]) == 64 {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("checksums.txt 里没有 %s", asset)
}

func extractBinary(zipData []byte) ([]byte, error) {
	zr, err := zip.NewReader(strings.NewReader(string(zipData)), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("解压安装包失败: %w", err)
	}
	for _, f := range zr.File {
		if f.Name != "npnode" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		bin, err := io.ReadAll(io.LimitReader(rc, maxBinarySize+1))
		if err != nil {
			return nil, err
		}
		if len(bin) > maxBinarySize || len(bin) == 0 {
			return nil, errors.New("安装包里的程序大小异常")
		}
		return bin, nil
	}
	return nil, errors.New("安装包里没有 npnode")
}
