package logreport

import (
	"io"
	"regexp"

	"github.com/sirupsen/logrus"
)

var reLevel = regexp.MustCompile(`(?:^|\s)level=(debug|info|warning|warn|error|fatal|panic|trace)\b`)

// LevelWriter 按行内 level= 过滤输出：低于 Level 的行丢弃，无 level= 的行原样透传。
type LevelWriter struct {
	Out   io.Writer
	Level logrus.Level
}

func (w *LevelWriter) Write(p []byte) (int, error) {
	if m := reLevel.FindSubmatch(p); m != nil {
		name := string(m[1])
		if name == "warn" {
			name = "warning"
		}
		if lv, err := logrus.ParseLevel(name); err == nil && lv > w.Level {
			// logrus 级别数值越大越详细
			return len(p), nil
		}
	}
	return w.Out.Write(p)
}
