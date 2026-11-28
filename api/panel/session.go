package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/npanel-dev/npnode/conf"
)

// 实时准入（admission_v1）的面板接口：POST /v1/server/session/check、/v1/server/session/release。
// 字段与状态码按面板 compatLegacySessionHandler：请求体为 JSON，响应直接是对象（没有 code/msg 外壳），
// 非 200 即失败（400 参数错误、403 密钥不对、500 Redis / 数据库出错）。

// SessionTimeout 准入请求的单次超时（A5：不超过 2 秒）
const SessionTimeout = 2 * time.Second

const (
	sessionCheckPath   = "/v1/server/session/check"
	sessionReleasePath = "/v1/server/session/release"
)

// SessionRequest check / release 的请求体。server_id、secret_key、capabilities 由 SessionClient 填写。
// 面板先读查询参数、请求体只补空缺，所以全部字段都放在请求体里、不带查询参数，避免不同入站的 port 互相覆盖。
type SessionRequest struct {
	ServerID  int64  `json:"server_id"`
	SecretKey string `json:"secret_key"`
	// Protocol 面板当前不参与判断；release 可以省略
	Protocol string `json:"protocol,omitempty"`
	// Port 1～65535，面板当前不参与判断，标准要求填写
	Port         int    `json:"port,omitempty"`
	UserID       int64  `json:"user_id"`
	ClientIP     string `json:"client_ip"`
	Capabilities string `json:"capabilities"`
	// ConnectionID 每个 (id, ip) 一个，只用于排查（§7.2）
	ConnectionID string `json:"connection_id,omitempty"`
}

// SessionResult check 的响应
type SessionResult struct {
	Allowed bool   `json:"allowed"`
	Current int64  `json:"current"`
	Limit   int64  `json:"limit"`
	Reason  string `json:"reason"`
}

// SessionError 面板返回了非 200 状态码
type SessionError struct {
	Path   string
	Status int
	Body   string
}

func (e *SessionError) Error() string {
	return fmt.Sprintf("%s 返回 HTTP %d: %s", e.Path, e.Status, e.Body)
}

// Misconfigured 400（实现错误）或 403（密钥不对）：仍按面板不可用降级，但要记 Error 提醒运维
func (e *SessionError) Misconfigured() bool {
	return e.Status == http.StatusBadRequest || e.Status == http.StatusForbidden
}

// SessionClient 准入接口客户端：独立的 resty 客户端，超时 2 秒、不自动重试，与 30 秒超时的业务客户端分开
type SessionClient struct {
	client    *resty.Client
	serverID  int64
	secretKey string
}

// NewSessionClient 按服务器配置建立准入客户端；须在 SetAdmissionV1 等能力开关设置之后调用
func NewSessionClient(c *conf.ServerApiConfig) *SessionClient {
	client := resty.New()
	client.SetRetryCount(0)
	client.SetTimeout(SessionTimeout)
	client.SetBaseURL(c.ApiHost)
	client.SetHeader("Content-Type", "application/json")
	if agent := NodeAgent(); agent != "" {
		client.SetHeader("X-Node-Agent", agent)
	}
	return &SessionClient{client: client, serverID: int64(c.ServerId), secretKey: c.SecretKey}
}

// ServerID 本服务器 ID（严格模式下是计数成员的一部分）
func (s *SessionClient) ServerID() int64 { return s.serverID }

func (s *SessionClient) fill(req SessionRequest) SessionRequest {
	req.ServerID = s.serverID
	req.SecretKey = s.secretKey
	req.Capabilities = CapabilityParam()
	return req
}

func (s *SessionClient) post(ctx context.Context, path string, req SessionRequest) ([]byte, error) {
	r, err := s.client.R().
		SetContext(ctx).
		SetBody(s.fill(req)).
		Post(path)
	if err != nil {
		return nil, fmt.Errorf("访问 %s 失败: %w", path, err)
	}
	if r.StatusCode() != http.StatusOK {
		return nil, &SessionError{Path: path, Status: r.StatusCode(), Body: strings.TrimSpace(string(r.Body()))}
	}
	return r.Body(), nil
}

// Check 准入 / 保活。超时、网络错误、非 200、响应不是合法 JSON 或缺少 allowed 都返回错误（按面板不可用处理）
func (s *SessionClient) Check(ctx context.Context, req SessionRequest) (*SessionResult, error) {
	body, err := s.post(ctx, sessionCheckPath, req)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Allowed *bool  `json:"allowed"`
		Current int64  `json:"current"`
		Limit   int64  `json:"limit"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("解析 %s 响应失败: %w", sessionCheckPath, err)
	}
	if raw.Allowed == nil {
		return nil, fmt.Errorf("%s 响应缺少 allowed: %s", sessionCheckPath, strings.TrimSpace(string(body)))
	}
	return &SessionResult{Allowed: *raw.Allowed, Current: raw.Current, Limit: raw.Limit, Reason: raw.Reason}, nil
}

// Release 释放会话；非 200 返回错误，由调用方的释放队列重试
func (s *SessionClient) Release(ctx context.Context, req SessionRequest) error {
	_, err := s.post(ctx, sessionReleasePath, req)
	return err
}
