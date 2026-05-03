// Tests for ParseCodexCLIJSONL. Exercises the full range of codex --json
// wire shapes: assistant messages, command-execution tool calls and their
// results, file-change tool calls, the terminal turn.completed event, and
// codec-level tolerance properties (malformed lines, empty input, large
// lines).

package agentbackend

import (
	"strings"
	"testing"
)

// TestParseCodexCLIJSONL_AssistantMessage confirms that a single
// item.completed/agent_message line produces exactly one TurnEventText.
func TestParseCodexCLIJSONL_AssistantMessage(t *testing.T) {
	src := `{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"Hello, world!"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Type != TurnEventText {
		t.Errorf("events[0].Type = %s, want text", events[0].Type)
	}
	if events[0].Text != "Hello, world!" {
		t.Errorf("events[0].Text = %q, want 'Hello, world!'", events[0].Text)
	}
}

// TestParseCodexCLIJSONL_ToolCall confirms that an item.started/
// command_execution line produces TurnEventToolUse with the command
// name and input populated.
func TestParseCodexCLIJSONL_ToolCall(t *testing.T) {
	src := `{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"ls -la","aggregated_output":"","exit_code":null,"status":"in_progress"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", events[0].Type)
	}
	tc := events[0].Tool
	if tc == nil {
		t.Fatal("events[0].Tool is nil")
	}
	if tc.ID != "item_1" {
		t.Errorf("Tool.ID = %q, want 'item_1'", tc.ID)
	}
	if tc.Name != "command" {
		t.Errorf("Tool.Name = %q, want 'command'", tc.Name)
	}
	if tc.Input["command"] != "ls -la" {
		t.Errorf("Tool.Input[command] = %v, want 'ls -la'", tc.Input["command"])
	}
}

// TestParseCodexCLIJSONL_ToolResult confirms that an item.completed/
// command_execution line produces TurnEventToolResult whose ToolUseID
// matches the item.id from the corresponding item.started event.
func TestParseCodexCLIJSONL_ToolResult(t *testing.T) {
	src := `{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"echo hi","aggregated_output":"","exit_code":null,"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"echo hi","aggregated_output":"hi\n","exit_code":0,"status":"completed"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if events[0].Type != TurnEventToolUse {
		t.Errorf("events[0].Type = %s, want tool_use", events[0].Type)
	}
	if events[1].Type != TurnEventToolResult {
		t.Fatalf("events[1].Type = %s, want tool_result", events[1].Type)
	}
	r := events[1].Result
	if r == nil {
		t.Fatal("events[1].Result is nil")
	}
	if r.ToolUseID != "item_1" {
		t.Errorf("ToolUseID = %q, want 'item_1'", r.ToolUseID)
	}
	if r.Content != "hi\n" {
		t.Errorf("Content = %q, want 'hi\\n'", r.Content)
	}
	if r.IsError {
		t.Error("IsError should be false for exit_code=0")
	}
}

// TestParseCodexCLIJSONL_ToolResult_NonZeroExit confirms IsError is set
// when exit_code is non-zero.
func TestParseCodexCLIJSONL_ToolResult_NonZeroExit(t *testing.T) {
	src := `{"type":"item.completed","item":{"id":"item_5","type":"command_execution","command":"false","aggregated_output":"","exit_code":1,"status":"completed"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Type != TurnEventToolResult {
		t.Fatalf("events[0].Type = %s, want tool_result", events[0].Type)
	}
	if !events[0].Result.IsError {
		t.Error("IsError should be true for exit_code=1")
	}
}

// TestParseCodexCLIJSONL_Sequence confirms the codec emits events in
// document order for a realistic text → tool_use → tool_result → turn_end
// sequence — the four-event case the tests spec requires.
func TestParseCodexCLIJSONL_Sequence(t *testing.T) {
	src := `{"type":"thread.started","thread_id":"tid-1"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"Let me run a command."}}
{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"whoami","aggregated_output":"","exit_code":null,"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"whoami","aggregated_output":"root\n","exit_code":0,"status":"completed"}}
{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":50}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	wantTypes := []TurnEventType{
		TurnEventText,
		TurnEventToolUse,
		TurnEventToolResult,
		TurnEventTurnEnd,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("event count = %d, want %d\nevents:\n%+v", len(events), len(wantTypes), events)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Errorf("events[%d].Type = %s, want %s", i, events[i].Type, want)
		}
	}
	// Spot-check the text and tool pair.
	if events[0].Text != "Let me run a command." {
		t.Errorf("text = %q, want 'Let me run a command.'", events[0].Text)
	}
	if events[1].Tool == nil || events[1].Tool.ID != "item_1" {
		t.Errorf("tool_use ID mismatch: %+v", events[1].Tool)
	}
	if events[2].Result == nil || events[2].Result.ToolUseID != "item_1" {
		t.Errorf("tool_result ToolUseID mismatch: %+v", events[2].Result)
	}
}

// TestParseCodexCLIJSONL_TurnEnd confirms the terminal turn.completed
// event is correctly mapped to TurnEventTurnEnd with stop_reason, usage
// counters, and CostUSD populated when present.
func TestParseCodexCLIJSONL_TurnEnd(t *testing.T) {
	src := `{"type":"turn.completed","stop_reason":"end_turn","usage":{"input_tokens":1000,"output_tokens":200},"total_cost_usd":0.005}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Type != TurnEventTurnEnd {
		t.Fatalf("events[0].Type = %s, want turn_end", events[0].Type)
	}
	end := events[0].End
	if end == nil {
		t.Fatal("events[0].End is nil")
	}
	if end.StopReason != "end_turn" {
		t.Errorf("StopReason = %q, want 'end_turn'", end.StopReason)
	}
	if end.InputTokens != 1000 {
		t.Errorf("InputTokens = %d, want 1000", end.InputTokens)
	}
	if end.OutputTokens != 200 {
		t.Errorf("OutputTokens = %d, want 200", end.OutputTokens)
	}
	if end.CostUSD == nil || *end.CostUSD != 0.005 {
		t.Errorf("CostUSD = %v, want pointer to 0.005", end.CostUSD)
	}
}

// TestParseCodexCLIJSONL_TurnEnd_NoStopReason confirms that when
// turn.completed carries no stop_reason (the common real-corpus case),
// StopReason is the empty string and the event is still emitted.
func TestParseCodexCLIJSONL_TurnEnd_NoStopReason(t *testing.T) {
	src := `{"type":"turn.completed","usage":{"input_tokens":157742,"cached_input_tokens":123392,"output_tokens":4654}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 || events[0].Type != TurnEventTurnEnd {
		t.Fatalf("events = %+v, want [turn_end]", events)
	}
	end := events[0].End
	if end == nil {
		t.Fatal("End is nil")
	}
	if end.StopReason != "" {
		t.Errorf("StopReason = %q, want empty", end.StopReason)
	}
	if end.InputTokens != 157742 || end.OutputTokens != 4654 {
		t.Errorf("usage = %d/%d, want 157742/4654", end.InputTokens, end.OutputTokens)
	}
	if end.CostUSD != nil {
		t.Errorf("CostUSD = %v, want nil", end.CostUSD)
	}
}

// TestParseCodexCLIJSONL_MalformedLines confirms the codec tolerates
// non-JSON lines and recovers to parse the valid events that follow.
func TestParseCodexCLIJSONL_MalformedLines(t *testing.T) {
	src := `not json at all
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"ok"}}
{ broken json
{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2 (malformed lines skipped)", len(events))
	}
	if events[0].Type != TurnEventText || events[0].Text != "ok" {
		t.Errorf("events[0] = %+v, want text 'ok'", events[0])
	}
	if events[1].Type != TurnEventTurnEnd {
		t.Errorf("events[1].Type = %s, want turn_end", events[1].Type)
	}
}

// TestParseCodexCLIJSONL_EmptyInput confirms an empty reader returns an
// empty (or nil) slice and no error.
func TestParseCodexCLIJSONL_EmptyInput(t *testing.T) {
	events, err := ParseCodexCLIJSONL(strings.NewReader(""))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("empty input produced %d events, want 0: %+v", len(events), events)
	}
}

// TestParseCodexCLIJSONL_NilReader confirms a nil io.Reader doesn't panic.
func TestParseCodexCLIJSONL_NilReader(t *testing.T) {
	events, err := ParseCodexCLIJSONL(nil)
	if err != nil {
		t.Fatalf("nil reader err = %v, want nil", err)
	}
	if events != nil {
		t.Errorf("nil reader events = %+v, want nil", events)
	}
}

// TestParseCodexCLIJSONL_SkipsUnknownTypes confirms that thread.started,
// turn.started, and other unknown top-level types produce no events.
func TestParseCodexCLIJSONL_SkipsUnknownTypes(t *testing.T) {
	src := `{"type":"thread.started","thread_id":"tid-abc"}
{"type":"turn.started"}
{"type":"something.new","data":42}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("skippable types produced %d events, want 0: %+v", len(events), events)
	}
}

// TestParseCodexCLIJSONL_FileChange confirms that a file_change pair
// produces a matched TurnEventToolUse (item.started) and TurnEventToolResult
// (item.completed) with the same item.id.
func TestParseCodexCLIJSONL_FileChange(t *testing.T) {
	src := `{"type":"item.started","item":{"id":"item_8","type":"file_change","changes":[{"path":"/tmp/foo.md","kind":"add"}],"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_8","type":"file_change","changes":[{"path":"/tmp/foo.md","kind":"add"}],"status":"completed"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2 (tool_use + tool_result)", len(events))
	}
	if events[0].Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", events[0].Type)
	}
	if events[0].Tool == nil || events[0].Tool.Name != "file_change" {
		t.Errorf("events[0].Tool = %+v, want file_change", events[0].Tool)
	}
	if events[0].Tool.ID != "item_8" {
		t.Errorf("Tool.ID = %q, want 'item_8'", events[0].Tool.ID)
	}
	if events[1].Type != TurnEventToolResult {
		t.Fatalf("events[1].Type = %s, want tool_result", events[1].Type)
	}
	if events[1].Result == nil || events[1].Result.ToolUseID != "item_8" {
		t.Errorf("events[1].Result = %+v, want ToolUseID='item_8'", events[1].Result)
	}
}

// TestParseCodexCLIJSONL_LongLine confirms the per-line buffer ceiling
// is raised above the 64 KB scanner default. Real codex runs can carry
// large file contents inside command outputs.
func TestParseCodexCLIJSONL_LongLine(t *testing.T) {
	bigText := strings.Repeat("x", 512*1024)
	src := `{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"` + bigText + `"}}
`
	events, err := ParseCodexCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 || events[0].Type != TurnEventText {
		t.Fatalf("len/type wrong: %+v", events)
	}
	if len(events[0].Text) != len(bigText) {
		t.Errorf("text length = %d, want %d", len(events[0].Text), len(bigText))
	}
}
