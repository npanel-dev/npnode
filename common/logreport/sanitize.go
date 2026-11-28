package logreport

import (
	"regexp"
	"strings"
)

var (
	// 查询串形式 secret_key=xxx，允许前缀（client_secret=、access_token=），值到 & 或空白/引号结束，前缀保留
	reSecretQuery = regexp.MustCompile(`(?i)([A-Za-z0-9_]*(?:secret_key|secret|token|password|passwd|api_key|apikey)=)[^&\s"']+`)
	// JSON 形式 "secret_key":"xxx"
	reSecretJSON = regexp.MustCompile(`(?i)("[A-Za-z0-9_]*(?:secret_key|secret|token|password|passwd|api_key|apikey)"\s*:\s*")[^"]*(")`)
	// 43 位 base64url（x25519 密钥等），前后不得紧邻 base64url 字符
	reKey43 = regexp.MustCompile(`(^|[^A-Za-z0-9_-])[A-Za-z0-9_-]{43}($|[^A-Za-z0-9_-])`)
)

// Sanitize 对待上报文本脱敏：密钥值 -> <redacted>，43 位 base64url 密钥 -> <key>，UUID -> <uuid>，
// 并把非法 UTF-8 字节替换为 U+FFFD（proto3 string 必须是合法 UTF-8，否则整批序列化失败）。
func Sanitize(s string) string {
	s = reSecretQuery.ReplaceAllString(s, "${1}<redacted>")
	s = reSecretJSON.ReplaceAllString(s, "${1}<redacted>${2}")
	s = reUUID.ReplaceAllString(s, "<uuid>")
	// 相邻两个密钥共用分隔符时单次替换会漏掉，重复直到稳定
	for {
		n := reKey43.ReplaceAllString(s, "${1}<key>${2}")
		if n == s {
			return validUTF8(s)
		}
		s = n
	}
}

// validUTF8 把非法 UTF-8 字节替换为 U+FFFD；合法字符串原样返回（不分配）。
func validUTF8(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
}
