// Package logreport 提供节点错误日志上报的基础能力：错误码、消息归一化、脱敏与合并键。
// 本包不依赖 npnode 的其他包。
package logreport

// Error 是带分类信息的错误，Error() 返回值与被包装错误完全一致。
type Error struct {
	Code     string
	Stage    string
	Protocol string
	Port     int
	Err      error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// innermost 返回错误链上最内层（最后一个）带非空 Code 的 *Error。
func innermost(err error) *Error {
	var found *Error
	for err != nil {
		if e, ok := err.(*Error); ok && e.Code != "" {
			found = e
		}
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			// 多错误：取第一个含 *Error 的分支
			var next error
			for _, c := range u.Unwrap() {
				if innermost(c) != nil {
					next = c
					break
				}
			}
			err = next
		default:
			err = nil
		}
	}
	return found
}

// Wrap 给 err 附加错误码与阶段；err 为 nil 返回 nil。
// 若链上已存在带 Code 的 *Error，则保留内层分类并原样返回 err（消息不变）。
func Wrap(code, stage string, err error) error {
	if err == nil {
		return nil
	}
	if innermost(err) != nil {
		return err
	}
	return &Error{Code: code, Stage: stage, Err: err}
}

// WithInbound 给链上最内层 *Error 补充协议与端口（仅补缺失字段）；
// 链上没有 *Error 时包一层 INBOUND_START_FAILED/inbound。
func WithInbound(err error, protocol string, port int) error {
	if err == nil {
		return nil
	}
	if e := innermost(err); e != nil {
		if e.Protocol == "" {
			e.Protocol = protocol
		}
		if e.Port == 0 {
			e.Port = port
		}
		return err
	}
	return &Error{Code: CodeInboundStart, Stage: StageInbound, Protocol: protocol, Port: port, Err: err}
}

// Classify 取错误链上最内层 *Error 的分类信息。
func Classify(err error) (code, stage, protocol string, port int, ok bool) {
	e := innermost(err)
	if e == nil {
		return "", "", "", 0, false
	}
	return e.Code, e.Stage, e.Protocol, e.Port, true
}
