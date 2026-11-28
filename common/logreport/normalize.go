package logreport

import (
	"regexp"
	"strconv"
	"strings"
)

// 归一化替换顺序是与面板后端共享的跨仓库契约，两端必须完全一致，
// testdata/normalize_cases.json 由面板后端原样复制。顺序固定为：
//  1. 双引号内内容      "..."                  -> "?"
//  2. UUID                                      -> <uuid>
//  3. IPv6（方括号形式，含可选 :port）/ IPv4（含可选 :port） -> <ip>
//  4. 8 位及以上十六进制串（含纯数字长串）      -> <hex>
//  5. 十进制数字串                              -> <n>
//  6. 合并连续空白为单个空格并 trim
//
// 注意：正则中的 \d \s \b 均为 RE2 的 ASCII 语义（\d 仅 0-9，\s 仅 ASCII 空白，\b 按 ASCII 单词边界），
// 对端实现者（面板后端）必须使用等价的 ASCII 语义，不得启用 Unicode 数字/空白。
var (
	reQuoted = regexp.MustCompile(`"[^"]*"`)
	reUUID   = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reIPv6   = regexp.MustCompile(`\[[0-9a-fA-F:.]*:[0-9a-fA-F:.]*\](?::\d+)?`)
	reIPv4   = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
	reHex    = regexp.MustCompile(`\b[0-9a-fA-F]{8,}\b`)
	reNum    = regexp.MustCompile(`\d+`)
	reSpace  = regexp.MustCompile(`\s+`)
)

// Normalize 把错误消息中的可变部分替换为占位符，使同类错误得到相同文本。
func Normalize(msg string) string {
	s := reQuoted.ReplaceAllString(msg, `"?"`)
	s = reUUID.ReplaceAllString(s, "<uuid>")
	s = reIPv6.ReplaceAllString(s, "<ip>")
	s = reIPv4.ReplaceAllString(s, "<ip>")
	s = reHex.ReplaceAllString(s, "<hex>")
	s = reNum.ReplaceAllString(s, "<n>")
	s = reSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// MergeKey 生成合并键：有明确错误码时为 code|protocol|port，
// 调用方应传入已经 Sanitize 的 message。UNKNOWN（或空码）时追加归一化后的消息：code|protocol|port|<normalized>。
func MergeKey(code, protocol string, port int, message string) string {
	if code == "" {
		code = CodeUnknown
	}
	k := code + "|" + protocol + "|" + strconv.Itoa(port)
	if code == CodeUnknown {
		k += "|" + Normalize(message)
	}
	return k
}
