package logreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// spoolMaxBytes 为 spool 文件大小上限，超出时丢最旧的报告/事件。
const spoolMaxBytes = 1 << 20

var (
	spoolMarshal   = protojson.MarshalOptions{UseProtoNames: true}
	spoolUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// readSpool 读取 spool 文件：JSON 数组，每项为 protojson 编码的 LogReport。文件不存在返回 (nil, nil)。
func readSpool(path string) ([]*nodev1.LogReport, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, fmt.Errorf("logreport: 解析 spool 失败: %w", err)
	}
	out := make([]*nodev1.LogReport, 0, len(raws))
	for _, raw := range raws {
		rep := &nodev1.LogReport{}
		if err := spoolUnmarshal.Unmarshal(raw, rep); err != nil {
			return nil, fmt.Errorf("logreport: 解析 spool 报告失败: %w", err)
		}
		out = append(out, rep)
	}
	return out, nil
}

// writeSpool 按 spoolMaxBytes 截断后原子写入；reports 为空时删除文件。
func writeSpool(path string, reports []*nodev1.LogReport) error {
	if len(reports) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	data, err := encodeSpool(reports, spoolMaxBytes)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// encodeSpool 编码为 JSON 数组，总大小超过 max 时从最旧的报告开始丢事件，
// 报告的事件丢光（且无 resolve）则整条丢弃。不修改传入的报告。
func encodeSpool(reports []*nodev1.LogReport, max int) ([]byte, error) {
	reports = append([]*nodev1.LogReport(nil), reports...)
	encoded := make([][]byte, len(reports))
	total := 1 // "["
	for i, rep := range reports {
		b, err := spoolMarshal.Marshal(rep)
		if err != nil {
			return nil, err
		}
		encoded[i] = b
		total += len(b) + 1 // 逗号或 "]"
	}

	for total > max && len(reports) > 0 {
		head := reports[0]
		excess := total - max
		// 估算需要丢弃的事件数，然后重新编码校验（protojson 输出不保证逐字节稳定）
		drop := 0
		for freed := 0; drop < len(head.Events) && freed < excess; drop++ {
			b, err := spoolMarshal.Marshal(head.Events[drop])
			if err != nil {
				return nil, err
			}
			freed += len(b) + 1
		}
		if drop == 0 {
			drop = 1 // 至少前进一步，保证循环终止
		}
		total -= len(encoded[0]) + 1
		// 无事件（只剩 resolve 仍超限）或事件将丢光且无 resolve：整条丢弃
		if len(head.Events) == 0 || (drop >= len(head.Events) && len(head.Resolves) == 0) {
			reports, encoded = reports[1:], encoded[1:]
			continue
		}
		trimmed := &nodev1.LogReport{ReportId: head.ReportId, Events: head.Events[drop:], Resolves: head.Resolves}
		b, err := spoolMarshal.Marshal(trimmed)
		if err != nil {
			return nil, err
		}
		reports[0], encoded[0] = trimmed, b
		total += len(b) + 1
	}

	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, b := range encoded {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(b)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// appendSpool 把未发送的报告追加到 spool（保留已有报告，同 ReportId 不重复写入）。
func (r *Reporter) appendSpool(add []*nodev1.LogReport) error {
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	existing, err := readSpool(r.spoolPath)
	if err != nil {
		existing = nil // 已损坏的旧文件无法恢复，直接覆盖
	}
	seen := make(map[string]bool, len(existing))
	for _, rep := range existing {
		seen[rep.ReportId] = true
	}
	all := existing
	for _, rep := range add {
		if !seen[rep.ReportId] {
			all = append(all, rep)
			seen[rep.ReportId] = true
		}
	}
	return writeSpool(r.spoolPath, all)
}

// ReplaySpool 启动时按顺序重放 spool 中的报告（保留原 ReportId 供面板去重）。
// 全部成功删除文件；失败时把未发送的剩余部分写回并返回错误。文件不存在返回 nil。
func (r *Reporter) ReplaySpool(ctx context.Context) error {
	if r.spoolPath == "" {
		return nil
	}
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	reports, err := readSpool(r.spoolPath)
	if err != nil {
		// 损坏的文件无法重放，删除以免每次启动都失败
		_ = os.Remove(r.spoolPath)
		return err
	}
	if len(reports) == 0 {
		return writeSpool(r.spoolPath, nil)
	}

	r.mu.Lock()
	s := r.sender
	r.mu.Unlock()
	if s == nil {
		return errNoSender
	}

	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.sem }()

	for i, rep := range reports {
		err := s.SendLogs(ctx, rep)
		if errors.Is(err, ErrRejected) {
			// 面板永久拒绝：丢弃该报告，继续重放后面的（只记 Debug，避免经 hook 回流）
			logrus.WithField("err", err).Debugf("[log] 面板拒绝 spool 中的节点日志报告 %s，已丢弃", rep.ReportId)
			continue
		}
		if err != nil {
			// 剩余部分（不含已发送和已被拒绝的）写回
			if werr := writeSpool(r.spoolPath, reports[i:]); werr != nil {
				return errors.Join(err, werr)
			}
			return err
		}
	}
	return writeSpool(r.spoolPath, nil)
}
