package panel

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-resty/resty/v2"
	"github.com/npanel-dev/npnode/common/logreport"
	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	_ logreport.Sender = LogSender{}
	_ logreport.Sender = (*GrpcClient)(nil)
	_ logreport.Sender = (*ClientV2)(nil)
)

// silentLogger 让 resty 自身不输出任何日志。
type silentLogger struct{}

func (silentLogger) Errorf(string, ...interface{}) {}
func (silentLogger) Warnf(string, ...interface{})  {}
func (silentLogger) Debugf(string, ...interface{}) {}

// logHTTPClient 返回专用于日志上报的 resty 客户端：
// 与主客户端共用 BaseURL、查询参数、请求头与超时，但不挂 OnError，
// 避免上报失败时产生 logrus 错误日志，被 logrus hook 再次捕获成新的日志上报（反馈回路）。
func (c *ClientV2) logHTTPClient() *resty.Client {
	c.logClientOnce.Do(func() {
		src := c.Client
		cl := resty.New()
		cl.SetRetryCount(0)
		cl.SetLogger(silentLogger{})
		cl.SetTimeout(src.GetClient().Timeout)
		cl.SetBaseURL(src.BaseURL)
		for k, vs := range src.QueryParam {
			for _, v := range vs {
				cl.QueryParam.Add(k, v)
			}
		}
		for k, vs := range src.Header {
			for _, v := range vs {
				cl.Header.Add(k, v)
			}
		}
		c.logClient = cl
	})
	return c.logClient
}

// ReportLogs 通过 HTTP 上报一批节点日志（gRPC 不可用时的兜底）。
func (c *ClientV2) ReportLogs(ctx context.Context, r *nodev1.LogReport) error {
	body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(r)
	if err != nil {
		return fmt.Errorf("序列化节点日志失败: %w", err)
	}
	resp, err := c.logHTTPClient().R().
		SetContext(ctx).
		SetQueryParam("server_id", strconv.Itoa(c.ServerId)).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Post("/v1/server/log")
	if err != nil {
		return fmt.Errorf("上报节点日志失败: %w", err)
	}
	return classifyLogStatus(resp.StatusCode())
}

// classifyLogStatus 按 HTTP 状态码分类上报结果：仅 2xx 成功；
// 404/405（旧面板没有该接口）-> ErrUnsupported，交给 Reporter 长时间退避；
// 其它 4xx（408/429 除外）-> ErrRejected，重发不会成功，Reporter 丢弃该批；
// 3xx、408、429、5xx 等 -> 普通可重试错误。
func classifyLogStatus(code int) error {
	switch {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusNotFound || code == http.StatusMethodNotAllowed:
		return fmt.Errorf("%w: HTTP %d", logreport.ErrUnsupported, code)
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests:
		return fmt.Errorf("%w: 上报节点日志失败: HTTP %d", logreport.ErrRejected, code)
	default:
		return fmt.Errorf("上报节点日志失败: HTTP %d", code)
	}
}

// SendLogs 实现 logreport.Sender（仅 HTTP）。
func (c *ClientV2) SendLogs(ctx context.Context, r *nodev1.LogReport) error {
	return c.ReportLogs(ctx, r)
}

// LogSender 优先经 gRPC Sync 上报日志，失败或未连接时静默回退到 HTTP。
type LogSender struct {
	Grpc *GrpcClient
	HTTP *ClientV2
}

// SendLogs 实现 logreport.Sender。不得输出 Info 及以上日志，避免反馈回路。
func (s LogSender) SendLogs(ctx context.Context, r *nodev1.LogReport) error {
	if s.Grpc != nil && s.Grpc.IsSyncConnected() && s.Grpc.logsEnabled() {
		err := s.Grpc.SendLogs(ctx, r)
		if err == nil {
			return nil
		}
		logrus.Debugf("[log] gRPC 上报节点日志失败，回退 HTTP: %v", err)
		if ctx.Err() != nil {
			return err
		}
	}
	if s.HTTP == nil {
		return fmt.Errorf("没有可用的日志上报通道")
	}
	return s.HTTP.ReportLogs(ctx, r)
}
