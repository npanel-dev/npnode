// Package upgrade 按面板下发的请求远程升级节点程序。
//
// 安全约束：下载地址固定为官方发布仓库，面板只能指定目标版本（latest 或 x.y.z）；
// 安装包必须与同一发布里的 checksums.txt 的 SHA-256 一致；同一个请求只处理一次。
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
	mu         sync.Mutex
)

const (
	maxZipSize       = 200 << 20
	maxBinarySize    = 300 << 20
	maxChecksumsSize = 64 << 10
	stateFileName    = "upgrade_request"
)

var versionPattern = regexp.MustCompile(`^\d{1,4}\.\d{1,4}\.\d{1,4}$`)

// Check 配置里带了新的升级请求就执行；成功时进程被新版本替换，不会返回。
// 失败只记日志，节点继续用当前版本运行。
func Check(cfg *panel.ServerConfigResponse) {
	if cfg == nil || cfg.Data == nil || cfg.Data.Upgrade == nil {
		return
	}
	if err := Handle(cfg.Data.Upgrade, panel.NodeVersion); err != nil {
		log.WithField("err", err).Error("远程升级失败，继续使用当前版本")
	}
}

// Handle 执行一次升级请求。已处理过、或已经是目标版本时直接返回 nil。
func Handle(req *panel.UpgradeRequest, current string) error {
	mu.Lock()
	defer mu.Unlock()
	if req == nil || strings.TrimSpace(req.RequestID) == "" {
		return nil
	}
	stateFile := filepath.Join(StateDir, stateFileName)
	if last, _ := os.ReadFile(stateFile); strings.TrimSpace(string(last)) == req.RequestID {
		return nil
	}
	// 先记下请求：即使升级中途失败也不会反复重试同一个请求
	if err := os.WriteFile(stateFile, []byte(req.RequestID), 0o600); err != nil {
		return fmt.Errorf("记录升级请求失败: %w", err)
	}

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
	log.WithFields(log.Fields{"from": current, "to": target}).Warn("远程升级完成，正在重启节点")
	return execFn(exe, os.Args, os.Environ())
}

// resolveLatest 读取 releases/latest 跳转到的版本号
func resolveLatest() (string, error) {
	resp, err := noRedirect.Get(ReleaseBase + "/latest")
	if err != nil {
		return "", fmt.Errorf("查询最新版本失败: %w", err)
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", fmt.Errorf("查询最新版本失败: HTTP %d", resp.StatusCode)
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
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
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
