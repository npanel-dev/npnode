package conf

import "testing"

// 能力声明开关默认关闭：旧配置文件没有这一项时行为与旧版一致
func TestSeatV1DefaultsOffAndCanBeEnabled(t *testing.T) {
	c := load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n")
	if c.ApiConfig.SeatV1 {
		t.Fatal("SeatV1 must default to false")
	}
	c = load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n  SeatV1: true\n")
	if !c.ApiConfig.SeatV1 {
		t.Fatal("Api.SeatV1: true was not loaded")
	}
}

// online_v1 开关默认关闭，与 SeatV1 相互独立
func TestOnlineV1DefaultsOffAndIndependent(t *testing.T) {
	c := load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n  SeatV1: true\n")
	if c.ApiConfig.OnlineV1 {
		t.Fatal("OnlineV1 must default to false")
	}
	c = load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n  OnlineV1: true\n")
	if !c.ApiConfig.OnlineV1 || c.ApiConfig.SeatV1 {
		t.Fatalf("Api.OnlineV1: true loaded as online=%v seat=%v", c.ApiConfig.OnlineV1, c.ApiConfig.SeatV1)
	}
}

// HTTP 流量幂等开关默认关闭
func TestTrafficIdempotentDefaultsOff(t *testing.T) {
	c := load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n  SeatV1: true\n  OnlineV1: true\n")
	if c.ApiConfig.TrafficIdempotent {
		t.Fatal("TrafficIdempotent must default to false")
	}
	c = load(t, "Api:\n  ApiHost: https://panel.example.com/api\n  ServerID: 6\n  SecretKey: k\n  TrafficIdempotent: true\n")
	if !c.ApiConfig.TrafficIdempotent {
		t.Fatal("Api.TrafficIdempotent: true was not loaded")
	}
}
