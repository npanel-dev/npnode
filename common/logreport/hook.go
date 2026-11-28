package logreport

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	nodev1 "github.com/npanel-dev/npnode/proto/node/v1"
)

// contextLines 为事件附带的最近日志行数。
const contextLines = 50

// Hook 把日志写入环形缓冲，并把符合条件的日志转成 LogEvent 交给 sink。
// 注意：Fire 在 logrus 持锁时被调用，内部绝不能再通过 logrus 打日志，也不能阻塞。
type Hook struct {
	ring *Ring
	sink func(*nodev1.LogEvent)
}

// NewHook 创建 Hook。Version 由 sink 一侧（Reporter）填充。
func NewHook(ring *Ring, sink func(*nodev1.LogEvent)) *Hook {
	return &Hook{ring: ring, sink: sink}
}

// Levels 实现 logrus.Hook。
func (h *Hook) Levels() []logrus.Level { return logrus.AllLevels }

// Fire 实现 logrus.Hook。
func (h *Hook) Fire(entry *logrus.Entry) error {
	if line, err := entry.String(); err == nil {
		h.ring.Add(Sanitize(strings.TrimRight(line, "\r\n")))
	}
	if h.sink == nil {
		return nil
	}

	var code, stage, protocol string
	var port int
	coded := false
	if e, ok := entry.Data["err"].(error); ok && e != nil {
		code, stage, protocol, port, coded = Classify(e)
	}
	if !coded {
		code = fieldString(entry.Data["code"])
		stage = fieldString(entry.Data["stage"])
		protocol = fieldString(entry.Data["protocol"])
		port = fieldInt(entry.Data["port"])
		coded = code != ""
	}

	var level string
	switch entry.Level {
	case logrus.WarnLevel:
		// warn 仅在带错误码时上报
		if !coded {
			return nil
		}
		level = "warn"
	case logrus.ErrorLevel:
		level = "error"
	case logrus.FatalLevel, logrus.PanicLevel:
		level = "fatal"
	default:
		return nil
	}
	if code == "" {
		code = CodeUnknown
	}

	msg := entry.Message
	if e, ok := entry.Data["err"].(error); ok && e != nil {
		msg += ": " + e.Error()
	}
	exiting, _ := entry.Data["exiting"].(bool)

	ev := &nodev1.LogEvent{
		Level:      level,
		Code:       code,
		Stage:      stage,
		Protocol:   protocol,
		Port:       uint32(port),
		Message:    Sanitize(msg),
		Context:    h.ring.Last(contextLines),
		OccurredAt: time.Now().UnixMilli(),
		Exiting:    exiting,
		Count:      1,
	}
	h.sink(ev)
	return nil
}

func fieldString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func fieldInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case string:
		var i int
		if _, err := fmt.Sscanf(n, "%d", &i); err == nil {
			return i
		}
	}
	return 0
}

// Install 安装到全局 logrus：级别放开到 Debug（供 ring 采集），
// 实际输出按 outLevel 过滤，并挂上 Hook。
func Install(ring *Ring, sink func(*nodev1.LogEvent), outLevel logrus.Level, out io.Writer) {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(&LevelWriter{Out: out, Level: outLevel})
	logrus.AddHook(NewHook(ring, sink))
}
