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
