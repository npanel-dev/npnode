package logreport

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyThroughFmtWrap(t *testing.T) {
	err := fmt.Errorf("启动节点失败: %w", fmt.Errorf("add new node error: %w",
		Wrap(CodeVlessEncModeEmpty, StageInbound, errors.New("vless encryption mode is empty"))))
	err = WithInbound(err, "vless", 443)
	code, stage, proto, port, ok := Classify(err)
	if !ok || code != CodeVlessEncModeEmpty || stage != StageInbound || proto != "vless" || port != 443 {
		t.Fatalf("got %v %v %v %v %v", code, stage, proto, port, ok)
	}
	if err.Error() != "启动节点失败: add new node error: vless encryption mode is empty" {
		t.Fatalf("message changed: %q", err.Error())
	}
}

func TestWithInboundWithoutCode(t *testing.T) {
	err := WithInbound(errors.New("x"), "trojan", 8443)
	code, stage, proto, port, ok := Classify(err)
	if !ok || code != CodeInboundStart || stage != StageInbound || proto != "trojan" || port != 8443 {
		t.Fatalf("got %v %v %v %v %v", code, stage, proto, port, ok)
	}
}

func TestWrapNilAndKeepInner(t *testing.T) {
	if Wrap(CodePanic, StageRuntime, nil) != nil {
		t.Fatal("nil should stay nil")
	}
	inner := Wrap(CodeCoreStart, StageCore, errors.New("boom"))
	outer := Wrap(CodeReload, StageRuntime, inner)
	code, stage, _, _, _ := Classify(outer)
	if code != CodeCoreStart || stage != StageCore || outer.Error() != "boom" {
		t.Fatalf("got %v %v %q", code, stage, outer.Error())
	}
}

func TestClassifyPlainError(t *testing.T) {
	if _, _, _, _, ok := Classify(errors.New("x")); ok {
		t.Fatal("plain error should not classify")
	}
	if WithInbound(nil, "a", 1) != nil {
		t.Fatal("nil should stay nil")
	}
}
