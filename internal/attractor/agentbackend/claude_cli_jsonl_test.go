// Tests for ParseClaudeCLIJSONL. The headline tests run the parser
// against a real-shape JSONL trace (lifted from a small captured run)
// and assert event-by-event normalization. Edge-case tests exercise
// the long-line buffer, malformed lines, missing fields, and the
// list-shaped tool_result content path.

package agentbackend

import (
	"bytes"
	"strings"
	"testing"
)

// realTraceJSONL is a hand-edited representative trace lifted from
// /Users/matt/.local/state/kilroy/attractor/runs/01KQJ66ZSK3BYYXC7KR265NT7T/agent/agent_output.jsonl
// (the smallest real run in the cache). Trimmed for readability — the
// shapes here are exactly what claude-CLI emits in production. Each
// line is a single JSON object; lines are separated by \n.
const realTraceJSONL = `{"type":"system","subtype":"init","cwd":"/tmp/work","session_id":"sess-1","tools":["Bash","Read"],"model":"claude-sonnet-4-6","permissionMode":"bypassPermissions","apiKeySource":"none"}
{"type":"assistant","message":{"model":"claude-sonnet-4-6","id":"msg_1","type":"message","role":"assistant","content":[{"type":"thinking","thinking":"Let me start by reading the input."}],"stop_reason":null}}
{"type":"assistant","message":{"id":"msg_1","content":[{"type":"tool_use","id":"toolu_001","name":"Read","input":{"file_path":"/tmp/work/.kilroy/INPUT.md"}}]}}
{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_001","type":"tool_result","content":"# Input\n\n## prompt\n\nWrite a smoke test."}]}}
{"type":"assistant","message":{"id":"msg_2","content":[{"type":"tool_use","id":"toolu_002","name":"Bash","input":{"command":"echo hello > result.md"}}]}}
{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_002","type":"tool_result","content":"(Bash completed with no output)"}]}}
{"type":"assistant","message":{"id":"msg_3","content":[{"type":"text","text":"Done. Wrote result.md."}]}}
{"type":"result","subtype":"success","is_error":false,"result":"Done. Wrote result.md.","total_cost_usd":0.0042,"usage":{"input_tokens":1943,"output_tokens":788}}
`

func TestParseClaudeCLIJSONL_RealTrace_NormalizesAllEvents(t *testing.T) {
	events, err := ParseClaudeCLIJSONL(strings.NewReader(realTraceJSONL))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Expected sequence: thinking, tool_use, tool_result, tool_use,
	// tool_result, text, turn_end. (system init is skipped.)
	wantTypes := []TurnEventType{
		TurnEventThinking,
		TurnEventToolUse,
		TurnEventToolResult,
		TurnEventToolUse,
		TurnEventToolResult,
		TurnEventText,
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

	// Spot-check field-level extraction for each variant.
	if events[0].Text != "Let me start by reading the input." {
		t.Errorf("thinking text = %q, want preface", events[0].Text)
	}
	if events[1].Tool == nil ||
		events[1].Tool.ID != "toolu_001" ||
		events[1].Tool.Name != "Read" ||
		events[1].Tool.Input["file_path"] != "/tmp/work/.kilroy/INPUT.md" {
		t.Errorf("tool_use[0] = %+v, want id=toolu_001 name=Read input.file_path=...", events[1].Tool)
	}
	if events[2].Result == nil ||
		events[2].Result.ToolUseID != "toolu_001" ||
		events[2].Result.IsError ||
		!strings.Contains(events[2].Result.Content, "Write a smoke test") {
		t.Errorf("tool_result[0] = %+v, want ToolUseID=toolu_001 content has 'Write a smoke test'", events[2].Result)
	}
	if events[5].Text != "Done. Wrote result.md." {
		t.Errorf("text = %q, want 'Done. Wrote result.md.'", events[5].Text)
	}
	if events[6].End == nil {
		t.Fatalf("turn_end has nil End: %+v", events[6])
	}
	if events[6].End.StopReason != "success" {
		t.Errorf("StopReason = %q, want success", events[6].End.StopReason)
	}
	if events[6].End.InputTokens != 1943 || events[6].End.OutputTokens != 788 {
		t.Errorf("usage = %d/%d, want 1943/788", events[6].End.InputTokens, events[6].End.OutputTokens)
	}
	if events[6].End.CostUSD == nil || *events[6].End.CostUSD != 0.0042 {
		t.Errorf("CostUSD = %v, want pointer to 0.0042", events[6].End.CostUSD)
	}
}

// TestParseClaudeCLIJSONL_AssistantMultipleBlocks confirms a single
// assistant line carrying multiple content blocks (the common
// "thinking, then tool_use" pattern) expands to one event per block.
func TestParseClaudeCLIJSONL_AssistantMultipleBlocks(t *testing.T) {
	src := `{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"first"},{"type":"text","text":"and now text"},{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}
`
	events, err := ParseClaudeCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}
	if events[0].Type != TurnEventThinking || events[0].Text != "first" {
		t.Errorf("events[0] = %+v, want thinking 'first'", events[0])
	}
	if events[1].Type != TurnEventText || events[1].Text != "and now text" {
		t.Errorf("events[1] = %+v, want text 'and now text'", events[1])
	}
	if events[2].Type != TurnEventToolUse || events[2].Tool == nil || events[2].Tool.Name != "Bash" {
		t.Errorf("events[2] = %+v, want tool_use Bash", events[2])
	}
}

// TestParseClaudeCLIJSONL_ToolResultListContent confirms the
// less-common case where claude returns a list of content blocks
// inside a tool_result rather than a single string. The codec
// normalizes this by concatenating text-typed blocks; non-text
// blocks emit as a JSON fragment so the structural cue survives.
func TestParseClaudeCLIJSONL_ToolResultListContent(t *testing.T) {
	src := `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"line one"},{"type":"text","text":"line two"},{"type":"image","source":{"type":"base64","data":"..."}}]}]}}
`
	events, err := ParseClaudeCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Type != TurnEventToolResult || events[0].Result == nil {
		t.Fatalf("events[0] = %+v, want tool_result", events[0])
	}
	got := events[0].Result.Content
	for _, want := range []string{"line one", "line two"} {
		if !strings.Contains(got, want) {
			t.Errorf("content missing %q\nfull:\n%s", want, got)
		}
	}
	if !strings.Contains(got, `"image"`) {
		t.Errorf("expected non-text block to surface as JSON fragment containing 'image'\nfull:\n%s", got)
	}
}

// TestParseClaudeCLIJSONL_MalformedLines_AreSkipped confirms the
// codec is tolerant of stray non-JSON lines and missing fields. The
// stream-json output is occasionally interleaved with raw stderr.
func TestParseClaudeCLIJSONL_MalformedLines_AreSkipped(t *testing.T) {
	src := `not json at all
{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}
{ broken json
{"type":"result","subtype":"success"}
`
	events, err := ParseClaudeCLIJSONL(strings.NewReader(src))
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

// TestParseClaudeCLIJSONL_LongLine confirms the buffer ceiling is
// raised above the bufio.Scanner default (64KB). Real claude lines
// can carry full file contents inside tool inputs/outputs and easily
// exceed that.
func TestParseClaudeCLIJSONL_LongLine(t *testing.T) {
	// Build a single assistant line ~512KB long.
	bigText := strings.Repeat("x", 512*1024)
	src := `{"type":"assistant","message":{"content":[{"type":"text","text":"` + bigText + `"}]}}
`
	events, err := ParseClaudeCLIJSONL(strings.NewReader(src))
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

// TestParseClaudeCLIJSONL_NilReader_NoCrash confirms the obvious
// edge-case (nil io.Reader) doesn't blow up.
func TestParseClaudeCLIJSONL_NilReader_NoCrash(t *testing.T) {
	events, err := ParseClaudeCLIJSONL(nil)
	if err != nil {
		t.Fatalf("nil reader err = %v, want nil", err)
	}
	if events != nil {
		t.Errorf("nil reader events = %+v, want nil", events)
	}
}

// TestParseClaudeCLIJSONL_EmptyTextBlock_Skipped confirms text/thinking
// blocks with empty content are skipped (claude occasionally emits
// streaming-warmup empty blocks before real content arrives).
func TestParseClaudeCLIJSONL_EmptyTextBlock_Skipped(t *testing.T) {
	src := `{"type":"assistant","message":{"content":[{"type":"text","text":""},{"type":"text","text":"real"}]}}
`
	events, err := ParseClaudeCLIJSONL(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1 (empty skipped)", len(events))
	}
	if events[0].Text != "real" {
		t.Errorf("text = %q, want real", events[0].Text)
	}
}

// TestParseClaudeCLIJSONL_RealCapture_Smoke runs the parser against
// the actual smallest real-trace bytes (a defensive copy in
// real_trace_smoke.go would be brittle to maintain; instead we just
// run the realTraceJSONL block through io.NopCloser via bytes.Reader
// so this test crashes loudly if the parser regresses against the
// representative shape).
func TestParseClaudeCLIJSONL_RealCapture_Smoke(t *testing.T) {
	events, err := ParseClaudeCLIJSONL(bytes.NewReader([]byte(realTraceJSONL)))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("real-trace produced 0 events")
	}
	// Confirm we got at least one of every variant we support.
	seen := map[TurnEventType]bool{}
	for _, e := range events {
		seen[e.Type] = true
	}
	for _, want := range []TurnEventType{
		TurnEventText, TurnEventThinking, TurnEventToolUse, TurnEventToolResult, TurnEventTurnEnd,
	} {
		if !seen[want] {
			t.Errorf("real trace produced no %s event; full type set = %v", want, seen)
		}
	}
}
