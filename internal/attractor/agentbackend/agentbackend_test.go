// Type-shape sanity tests for the agentbackend foundational types.
// Step-1 of Block 6 is types-only; these tests just confirm the
// discriminator strings and a few zero-value invariants. Real-codec
// behavior tests come later as the codecs are extracted.
package agentbackend

import (
	"errors"
	"testing"
)

func TestTurnEventType_String(t *testing.T) {
	tests := []struct {
		in   TurnEventType
		want string
	}{
		{TurnEventText, "text"},
		{TurnEventThinking, "thinking"},
		{TurnEventToolUse, "tool_use"},
		{TurnEventToolResult, "tool_result"},
		{TurnEventTurnEnd, "turn_end"},
		{TurnEventError, "error"},
		{TurnEventType(99), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("TurnEventType(%d).String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestToolControlMode_String(t *testing.T) {
	if ToolControlKilroy.String() != "kilroy" {
		t.Errorf("ToolControlKilroy.String() = %q, want kilroy", ToolControlKilroy.String())
	}
	if ToolControlDriver.String() != "driver" {
		t.Errorf("ToolControlDriver.String() = %q, want driver", ToolControlDriver.String())
	}
	if ToolControlMode(42).String() != "unknown" {
		t.Errorf("unknown mode should stringify as 'unknown'")
	}
}

// TestTurnEvent_VariantsZeroValue confirms variant fields are nil/zero
// for unrelated types. This is a simple invariant codecs rely on when
// constructing events.
func TestTurnEvent_VariantsZeroValue(t *testing.T) {
	ev := TurnEvent{Type: TurnEventText, Text: "hi"}
	if ev.Tool != nil || ev.Result != nil || ev.End != nil || ev.Err != nil {
		t.Errorf("text event should leave variant pointers nil, got %+v", ev)
	}

	tu := TurnEvent{Type: TurnEventToolUse, Tool: &ToolCall{ID: "x", Name: "y"}}
	if tu.Text != "" || tu.Result != nil || tu.End != nil || tu.Err != nil {
		t.Errorf("tool_use event should leave non-Tool variants empty, got %+v", tu)
	}

	te := TurnEvent{Type: TurnEventTurnEnd, End: &TurnEndInfo{StopReason: "end_turn"}}
	if te.Text != "" || te.Tool != nil || te.Result != nil || te.Err != nil {
		t.Errorf("turn_end event should leave non-End variants empty, got %+v", te)
	}
}

// TestErrToolControlDriver_Identity confirms callers can match the
// sentinel error via errors.Is — important because TurnStream
// implementations may wrap it with extra context.
func TestErrToolControlDriver_Identity(t *testing.T) {
	wrapped := errors.New("wrapper: " + ErrToolControlDriver.Error())
	if errors.Is(wrapped, ErrToolControlDriver) {
		t.Error("plain string concat should not satisfy errors.Is for ErrToolControlDriver")
	}

	wrapped2 := errors.Join(ErrToolControlDriver, errors.New("extra context"))
	if !errors.Is(wrapped2, ErrToolControlDriver) {
		t.Error("errors.Join should preserve identity for ErrToolControlDriver")
	}
}

// TestBackendCapabilities_ZeroDefault confirms the zero value is "no
// capabilities" — backends opt in by setting fields true. This matters
// because new bool fields will default false on existing backends.
func TestBackendCapabilities_ZeroDefault(t *testing.T) {
	var c BackendCapabilities
	if c.Thinking || c.TokenStreaming || c.CostTracking || c.ToolInjection {
		t.Errorf("zero-value BackendCapabilities should be all-false, got %+v", c)
	}
}
