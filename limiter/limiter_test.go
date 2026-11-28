package limiter

import (
	"testing"

	"github.com/npanel-dev/npnode/api/panel"
	"github.com/npanel-dev/npnode/common/format"
)

func checkUserBucket(t *testing.T, seatV1 bool, speedLimit int) (rate float64, capacity int64, unlimited bool) {
	t.Helper()
	panel.SetSeatV1(seatV1)
	t.Cleanup(func() { panel.SetSeatV1(false) })
	Init()
	const tag = "limiter-test"
	users := []panel.UserInfo{{Id: 1000000045, Uuid: "u-1", SpeedLimit: speedLimit}}
	l := AddLimiter(tag, users, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	bucket, reject := l.CheckLimit(format.UserTag(tag, "u-1"), "203.0.113.7", true, true)
	if reject {
		t.Fatal("user must not be rejected")
	}
	if bucket == nil {
		return 0, 0, true
	}
	return bucket.Rate(), bucket.Capacity(), false
}

// 声明 seat_v1 后 speed_limit 一律按 bit/s 解释：字节每秒 = speed_limit / 8，不做任何启发式换算
func TestSpeedLimitIsBitsPerSecondWithSeatV1(t *testing.T) {
	cases := []struct {
		bits      int
		wantBytes int64
	}{
		{524288, 65536},     // 64 KiB/s（过期分组 / 流量规则 64 KB/s）
		{4194304, 524288},   // 512 KiB/s
		{10485760, 1310720}, // 套餐 10 Mbps
		{7, 1},              // 不足 1 字节每秒按 1 处理，不能当成不限
	}
	for _, tc := range cases {
		rate, capacity, unlimited := checkUserBucket(t, true, tc.bits)
		if unlimited || int64(rate) != tc.wantBytes || capacity != tc.wantBytes {
			t.Errorf("speed_limit=%d bit/s: rate=%v capacity=%d unlimited=%v, want %d B/s", tc.bits, rate, capacity, unlimited, tc.wantBytes)
		}
	}
	if _, _, unlimited := checkUserBucket(t, true, 0); !unlimited {
		t.Error("speed_limit=0 must mean unlimited")
	}
}

// 未声明 seat_v1 时保持旧口径：按 Mbps 解释（× 1000000 / 8）
func TestSpeedLimitKeepsLegacyMbpsWithoutSeatV1(t *testing.T) {
	for _, tc := range []struct {
		mbps      int
		wantBytes int64
	}{{10, 1250000}, {1, 125000}, {524288, 524288 * 1000000 / 8}} {
		rate, capacity, unlimited := checkUserBucket(t, false, tc.mbps)
		if unlimited || int64(rate) != tc.wantBytes || capacity != tc.wantBytes {
			t.Errorf("speed_limit=%d (legacy): rate=%v capacity=%d unlimited=%v, want %d B/s", tc.mbps, rate, capacity, unlimited, tc.wantBytes)
		}
	}
	if _, _, unlimited := checkUserBucket(t, false, 0); !unlimited {
		t.Error("speed_limit=0 must mean unlimited")
	}
}

// 限速口径在建立限速器时确定：之后开关改变（重载前）不影响已建立的限速器
func TestSpeedLimitUnitIsFixedAtAddLimiter(t *testing.T) {
	panel.SetSeatV1(true)
	t.Cleanup(func() { panel.SetSeatV1(false) })
	Init()
	const tag = "limiter-fixed"
	l := AddLimiter(tag, []panel.UserInfo{{Id: 1, Uuid: "u-1", SpeedLimit: 10485760}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	panel.SetSeatV1(false)
	bucket, _ := l.CheckLimit(format.UserTag(tag, "u-1"), "203.0.113.7", true, true)
	if bucket == nil || bucket.Capacity() != 1310720 {
		t.Fatalf("bucket = %v, want 1310720 B/s fixed at AddLimiter", bucket)
	}
}

// U5：只改限速 / 设备数时原地更新 UserLimitInfo，并替换已有 bucket（已有连接持有同一个间接层，立即生效）
func TestUpdateUserLimitReplacesBucketInPlace(t *testing.T) {
	panel.SetSeatV1(true)
	t.Cleanup(func() { panel.SetSeatV1(false) })
	Init()
	const tag = "limiter-u5"
	l := AddLimiter(tag, []panel.UserInfo{{Id: 9, Uuid: "u-9", SpeedLimit: 524288}}, nil)
	t.Cleanup(func() { DeleteLimiter(tag) })
	key := format.UserTag(tag, "u-9")
	ref, reject := l.CheckLimit(key, "203.0.113.7", true, true)
	if reject || ref == nil || ref.Capacity() != 65536 {
		t.Fatalf("initial bucket = %v reject=%v", ref, reject)
	}
	l.UpdateUserLimit(key, 4194304, 1)
	if ref.Capacity() != 524288 {
		t.Fatalf("after update capacity = %d, want 524288", ref.Capacity())
	}
	v, _ := l.UserLimitInfo.Load(key)
	if u := v.(*UserLimitInfo); u.SpeedLimit != 4194304 || u.DeviceLimit != 1 || u.UID != 9 {
		t.Fatalf("UserLimitInfo = %+v", u)
	}
	again, _ := l.CheckLimit(key, "203.0.113.7", true, true)
	if again != ref {
		t.Fatal("新连接应拿到同一个间接层")
	}
	l.UpdateUserLimit(key, 0, 0)
	if ref.Load() != nil {
		t.Fatal("限速改为 0 后应不限速")
	}
	// 之后再改回限速：新连接拿到新速率
	l.UpdateUserLimit(key, 524288, 0)
	if again, _ := l.CheckLimit(key, "203.0.113.7", true, true); again == nil || again.Capacity() != 65536 {
		t.Fatalf("bucket = %v", again)
	}
	// 不在限速器里的用户：什么都不做
	l.UpdateUserLimit(format.UserTag(tag, "nobody"), 1, 1)
	if _, ok := l.UserLimitInfo.Load(format.UserTag(tag, "nobody")); ok {
		t.Fatal("UpdateUserLimit 不应新增用户")
	}
}
