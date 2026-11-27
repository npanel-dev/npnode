package panel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync/atomic"
	"time"
)

// OnlineUser 在线用户。面板按小写键 uid / ip 解析（旧版面板读不到大写键，整批拒收）
type OnlineUser struct {
	UID int    `json:"uid"`
	IP  string `json:"ip"`
}

type UserInfo struct {
	Id          int    `json:"id"`
	Uuid        string `json:"uuid"`
	SpeedLimit  int    `json:"speed_limit"`
	DeviceLimit int    `json:"device_limit"`
}

type UserListBody struct {
	Users []UserInfo `json:"users"`
}

type UserOnlineBody struct {
	Users []OnlineUser `json:"users"`
}

type AliveMap struct {
	Alive map[int]int `json:"alive"`
}

func (c *ClientV1) GetUserList(ctx context.Context) ([]UserInfo, error) {
	const p = "/v1/server/user"
	r, err := c.Client.R().
		SetContext(ctx).
		SetHeader("If-None-Match", c.userEtag).
		ForceContentType("application/json").
		SetDoNotParseResponse(true).
		Get(p)
	if r == nil || r.RawResponse == nil {
		return nil, fmt.Errorf("服务端响应为空")
	}
	defer r.RawResponse.Body.Close()

	if r.StatusCode() == 304 {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), err)
	}
	if r.StatusCode() >= 400 {
		body := r.Body()
		return nil, fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), string(body))
	}
	// 空列表必须是非 nil 切片：调用方用 nil 表示 304 未变化，
	// 否则用户被全部移除时节点会当作“没变化”而不删除旧用户。
	userlist := &UserListBody{Users: []UserInfo{}}
	dec := json.NewDecoder(r.RawResponse.Body)
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("解码用户列表失败: %w", err)
		}
		if tok == "users" {
			break
		}
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("解码用户列表失败: %w", err)
	}
	if tok == nil {
		// "users": null 视为没有用户
		c.userEtag = r.Header().Get("ETag")
		return userlist.Users, nil
	}
	if tok != json.Delim('[') {
		return nil, fmt.Errorf(`解码用户列表失败: "users"非数组`)
	}
	for dec.More() {
		var u UserInfo
		if err := dec.Decode(&u); err != nil {
			return nil, fmt.Errorf("解码用户列表失败: 读取用户对象失败: %w", err)
		}
		userlist.Users = append(userlist.Users, u)
	}
	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("解码用户列表失败: %w", err)
	}
	c.userEtag = r.Header().Get("ETag")
	return userlist.Users, nil
}

func (c *ClientV1) GetUserAlive() (map[int]int, error) {
	c.AliveMap = &AliveMap{}
	c.AliveMap.Alive = make(map[int]int)
	/*const path = "/v1/server/alivelist"
	r, err := c.client.R().
		ForceContentType("application/json").
		Get(path)
	if err != nil || r.StatusCode() >= 399 {
		c.AliveMap.Alive = make(map[int]int)
	}
	if r == nil || r.RawResponse == nil {
		fmt.Printf("received nil response or raw response")
		c.AliveMap.Alive = make(map[int]int)
	}
	defer r.RawResponse.Body.Close()
	if err := json.Unmarshal(r.Body(), c.AliveMap); err != nil {
		//fmt.Printf("unmarshal user alive list error: %s", err)
		c.AliveMap.Alive = make(map[int]int)
	}
	*/
	return c.AliveMap.Alive, nil
}

type ServerPushUserTrafficRequest struct {
	Traffic []UserTraffic `json:"traffic"`
	// 以下为可靠回执字段（T3），只在打开 Api.TrafficIdempotent 时填写；为零值时不出现在请求体中，与旧版一致。
	// 面板 traffic_report_mode=off 时忽略；shadow / enforce 时按 boot_id + sequence 生成幂等键
	BootID     string `json:"boot_id,omitempty"`
	Sequence   uint64 `json:"sequence,omitempty"`
	ReportedAt int64  `json:"reported_at,omitempty"`
}

// HTTPTrafficBatch 一批带批次号的 HTTP 流量上报。失败后必须原样重发（同一 boot_id、sequence 与内容），
// 面板 enforce 模式下同一幂等键内容不同会被拒绝
type HTTPTrafficBatch struct {
	Traffic    []UserTraffic
	BootID     string
	Sequence   uint64
	ReportedAt int64
}

var (
	// httpBootID 本进程的启动标识（HTTP 流量上报用），进程内不变；gRPC 客户端各有自己的 bootID
	httpBootID = newBootID()
	httpSeq    atomic.Uint64
	// trafficIdempotent 是否给 HTTP 流量上报带批次号（Api.TrafficIdempotent）
	trafficIdempotent atomic.Bool
)

// SetTrafficIdempotent 设置 HTTP 流量上报是否带批次号（启动、重载时由 cmd 按配置设置）
func SetTrafficIdempotent(on bool) {
	trafficIdempotent.Store(on)
}

// TrafficIdempotentEnabled HTTP 流量上报是否带批次号
func TrafficIdempotentEnabled() bool {
	return trafficIdempotent.Load()
}

// NewHTTPTrafficBatch 冻结一批流量并分配批次号：本进程 boot_id + 自增 sequence，reported_at 为采集时间
func NewHTTPTrafficBatch(traffic []UserTraffic) *HTTPTrafficBatch {
	frozen := make([]UserTraffic, len(traffic))
	copy(frozen, traffic)
	return &HTTPTrafficBatch{
		Traffic:    frozen,
		BootID:     httpBootID,
		Sequence:   httpSeq.Add(1),
		ReportedAt: time.Now().Unix(),
	}
}

// ReportUserTrafficBatch 上报一批带批次号的流量；重试时传入同一个 batch
func (c *ClientV1) ReportUserTrafficBatch(ctx context.Context, b *HTTPTrafficBatch) error {
	return c.pushTraffic(ctx, ServerPushUserTrafficRequest{
		Traffic:    b.Traffic,
		BootID:     b.BootID,
		Sequence:   b.Sequence,
		ReportedAt: b.ReportedAt,
	})
}

type UserTraffic struct {
	UID      int   `json:"uid"`
	Upload   int64 `json:"upload"`
	Download int64 `json:"download"`
}

func (c *ClientV1) ReportUserTraffic(ctx context.Context, userTraffic *[]UserTraffic) error {
	traffic := make([]UserTraffic, 0)
	for _, t := range *userTraffic {
		traffic = append(traffic, UserTraffic{
			UID:      t.UID,
			Upload:   t.Upload,
			Download: t.Download,
		})
	}
	return c.pushTraffic(ctx, ServerPushUserTrafficRequest{
		Traffic: traffic,
	})
}

func (c *ClientV1) pushTraffic(ctx context.Context, req ServerPushUserTrafficRequest) error {
	p := "/v1/server/push"
	r, err := c.Client.R().
		SetContext(ctx).
		SetBody(req).
		ForceContentType("application/json").
		Post(p)
	if err != nil {
		return fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), err)
	}
	if r.StatusCode() >= 400 {
		body := r.Body()
		return fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), string(body))
	}
	// 面板出错时仍返回 HTTP 200，错误码放在响应体 code 字段；不检查会把失败当成功，流量丢失
	if err := checkPanelBodyCode(r.Body()); err != nil {
		return fmt.Errorf("访问 %s 失败: %w", path.Join(c.APIHost+p), err)
	}

	return nil
}

// checkPanelBodyCode 检查面板响应体里的业务状态码：空响应或 code 为 0/200 视为成功。
func checkPanelBodyCode(body []byte) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var resp struct {
		Code *int   `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("解码流量上报响应失败: %w", err)
	}
	if resp.Code == nil {
		return nil
	}
	if *resp.Code != 0 && *resp.Code != 200 {
		return fmt.Errorf("面板返回错误 code=%d msg=%s", *resp.Code, resp.Msg)
	}
	return nil
}

func (c *ClientV1) ReportNodeOnlineUsers(ctx context.Context, data *[]OnlineUser) error {
	const p = "/v1/server/online"
	users := UserOnlineBody{
		Users: *data,
	}
	r, err := c.Client.R().
		SetContext(ctx).
		SetBody(users).
		ForceContentType("application/json").
		Post(p)
	if err != nil {
		return fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), err)
	}
	if r.StatusCode() >= 400 {
		body := r.Body()
		return fmt.Errorf("访问 %s 失败: %s", path.Join(c.APIHost+p), string(body))
	}
	// 与流量上报相同：业务失败时 HTTP 仍是 200，错误码在响应体 code 里
	if err := checkPanelBodyCode(r.Body()); err != nil {
		return fmt.Errorf("访问 %s 失败: %w", path.Join(c.APIHost+p), err)
	}
	return nil
}
