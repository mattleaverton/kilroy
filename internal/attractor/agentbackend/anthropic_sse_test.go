// Tests for ParseAnthropicSSE. Every test uses string-literal SSE input
// (no real-capture files needed for this step). Modelled on the
// claude_cli_jsonl_test.go pattern: shape-driven, assertion-first,
// minimal boilerplate.
//
// Block 6 Step 2: codec extraction campaign.
package agentbackend

import (
	"encoding/json"
	"strings"
	"testing"
)

// sseStream builds a minimal SSE body from (event, data) pairs.
// Each pair becomes two lines + a trailing blank line.
func sseStream(pairs ...string) string {
	if len(pairs)%2 != 0 {
		panic("sseStream: pairs must be even")
	}
	var b strings.Builder
	for i := 0; i < len(pairs); i += 2 {
		b.WriteString("event: ")
		b.WriteString(pairs[i])
		b.WriteByte('\n')
		b.WriteString("data: ")
		b.WriteString(pairs[i+1])
		b.WriteByte('\n')
		b.WriteByte('\n')
	}
	return b.String()
}

// TestParseAnthropicSSE_SingleTextResponse verifies that a stream with
// one text content_block (multiple deltas) emits one TurnEventText with
// the concatenated body, then TurnEventTurnEnd.
func TestParseAnthropicSSE_SingleTextResponse(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_01","usage":{"input_tokens":10,"output_tokens":1}}}`,
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", world!"}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventText {
		t.Errorf("events[0].Type = %s, want text", events[0].Type)
	}
	if events[0].Text != "Hello, world!" {
		t.Errorf("events[0].Text = %q, want 'Hello, world!'", events[0].Text)
	}
	if events[1].Type != TurnEventTurnEnd {
		t.Errorf("events[1].Type = %s, want turn_end", events[1].Type)
	}
}

// TestParseAnthropicSSE_ThinkingBlock verifies that thinking_delta events
// accumulate into a TurnEventThinking at content_block_stop.
func TestParseAnthropicSSE_ThinkingBlock(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_02","usage":{"input_tokens":20,"output_tokens":1}}}`,
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me think"}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":" about this."}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		"content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Answer."}}`,
		"content_block_stop", `{"type":"content_block_stop","index":1}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	// Expect: thinking, text, turn_end
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventThinking {
		t.Errorf("events[0].Type = %s, want thinking", events[0].Type)
	}
	if events[0].Text != "Let me think about this." {
		t.Errorf("events[0].Text = %q, want concatenated thinking text", events[0].Text)
	}
	if events[1].Type != TurnEventText || events[1].Text != "Answer." {
		t.Errorf("events[1] = %+v, want TurnEventText 'Answer.'", events[1])
	}
}

// TestParseAnthropicSSE_ToolUse verifies that a tool_use block with
// input_json_delta chunks accumulates the JSON and emits TurnEventToolUse
// with the parsed Input map.
func TestParseAnthropicSSE_ToolUse(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_03","usage":{"input_tokens":30,"output_tokens":1}}}`,
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_001","name":"Bash","input":{}}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":"}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"echo hi\"}"}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":12}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	// Expect: tool_use, turn_end
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", events[0].Type)
	}
	if events[0].Tool == nil {
		t.Fatal("events[0].Tool is nil")
	}
	if events[0].Tool.ID != "toolu_001" {
		t.Errorf("Tool.ID = %q, want toolu_001", events[0].Tool.ID)
	}
	if events[0].Tool.Name != "Bash" {
		t.Errorf("Tool.Name = %q, want Bash", events[0].Tool.Name)
	}
	if cmd, _ := events[0].Tool.Input["command"].(string); cmd != "echo hi" {
		t.Errorf("Tool.Input[command] = %q, want 'echo hi'", cmd)
	}
}

// TestParseAnthropicSSE_MultipleContentBlocks verifies that text + tool_use
// in one message produce events in document order.
func TestParseAnthropicSSE_MultipleContentBlocks(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_04","usage":{"input_tokens":40,"output_tokens":1}}}`,
		// block 0: text
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I will call a tool."}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		// block 1: tool_use
		"content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_002","name":"Read","input":{}}}`,
		"content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/etc/hosts\"}"}}`,
		"content_block_stop", `{"type":"content_block_stop","index":1}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	// Expect: text, tool_use, turn_end
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventText || events[0].Text != "I will call a tool." {
		t.Errorf("events[0] = %+v, want text 'I will call a tool.'", events[0])
	}
	if events[1].Type != TurnEventToolUse {
		t.Errorf("events[1].Type = %s, want tool_use", events[1].Type)
	}
	if events[1].Tool == nil || events[1].Tool.Name != "Read" {
		t.Errorf("events[1].Tool = %+v, want Read", events[1].Tool)
	}
	if fp, _ := events[1].Tool.Input["file_path"].(string); fp != "/etc/hosts" {
		t.Errorf("Input[file_path] = %q, want /etc/hosts", fp)
	}
	if events[2].Type != TurnEventTurnEnd {
		t.Errorf("events[2].Type = %s, want turn_end", events[2].Type)
	}
}

// TestParseAnthropicSSE_StopReasonFromMessageDelta verifies that
// TurnEventTurnEnd carries the stop_reason from message_delta.
func TestParseAnthropicSSE_StopReasonFromMessageDelta(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_05","usage":{"input_tokens":5,"output_tokens":1}}}`,
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"max_tokens","stop_sequence":null},"usage":{"output_tokens":3}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	var end *TurnEndInfo
	for _, e := range events {
		if e.Type == TurnEventTurnEnd {
			end = e.End
			break
		}
	}
	if end == nil {
		t.Fatal("no TurnEventTurnEnd in events")
	}
	if end.StopReason != "max_tokens" {
		t.Errorf("StopReason = %q, want max_tokens", end.StopReason)
	}
}

// TestParseAnthropicSSE_TokenUsageFromMessageDelta verifies that
// TurnEndInfo carries InputTokens (from message_start) and OutputTokens
// (from message_delta).
func TestParseAnthropicSSE_TokenUsageFromMessageDelta(t *testing.T) {
	src := sseStream(
		"message_start", `{"type":"message_start","message":{"id":"msg_06","usage":{"input_tokens":1234,"output_tokens":1}}}`,
		"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		"content_block_stop", `{"type":"content_block_stop","index":0}`,
		"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":567}}`,
		"message_stop", `{"type":"message_stop"}`,
	)

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	var end *TurnEndInfo
	for _, e := range events {
		if e.Type == TurnEventTurnEnd {
			end = e.End
			break
		}
	}
	if end == nil {
		t.Fatal("no TurnEventTurnEnd / nil End")
	}
	if end.InputTokens != 1234 {
		t.Errorf("InputTokens = %d, want 1234", end.InputTokens)
	}
	if end.OutputTokens != 567 {
		t.Errorf("OutputTokens = %d, want 567", end.OutputTokens)
	}
	// CostUSD is not present in the streaming API.
	if end.CostUSD != nil {
		t.Errorf("CostUSD = %v, want nil (not available in streaming API)", end.CostUSD)
	}
}

// TestParseAnthropicSSE_MalformedLinesTolerated verifies that garbage
// lines between valid SSE events do not cause errors or corrupt output.
func TestParseAnthropicSSE_MalformedLinesTolerated(t *testing.T) {
	src := "not valid SSE at all\n" +
		// broken data line — malformed JSON
		"event: content_block_start\n" +
		"data: {BROKEN JSON\n" +
		"\n" +
		// a valid block start (reuses same event name on the next round)
		"event: content_block_start\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n" +
		"\n" +
		"garbage line without prefix\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"recovered"}}` + "\n" +
		"\n" +
		"event: content_block_stop\n" +
		`data: {"type":"content_block_stop","index":0}` + "\n" +
		"\n" +
		"event: message_delta\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}` + "\n" +
		"\n" +
		"event: message_stop\n" +
		`data: {"type":"message_stop"}` + "\n" +
		"\n"

	events, err := ParseAnthropicSSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventText || events[0].Text != "recovered" {
		t.Errorf("events[0] = %+v, want text 'recovered'", events[0])
	}
	if events[1].Type != TurnEventTurnEnd {
		t.Errorf("events[1].Type = %s, want turn_end", events[1].Type)
	}
}

// TestParseAnthropicSSE_EmptyInput verifies that an empty reader returns
// an empty (non-nil) slice and no error.
func TestParseAnthropicSSE_EmptyInput(t *testing.T) {
	events, err := ParseAnthropicSSE(strings.NewReader(""))
	if err != nil {
		t.Fatalf("empty input err = %v, want nil", err)
	}
	if len(events) != 0 {
		t.Errorf("empty input events = %+v, want empty", events)
	}
}

// TestParseAnthropicSSE_NilReader verifies the nil-reader edge case does
// not panic and returns nil, nil.
func TestParseAnthropicSSE_NilReader(t *testing.T) {
	events, err := ParseAnthropicSSE(nil)
	if err != nil {
		t.Fatalf("nil reader err = %v, want nil", err)
	}
	if events != nil {
		t.Errorf("nil reader events = %+v, want nil", events)
	}
}

// TestParseAnthropicSSE_LargeInputJSONDelta verifies that the scanner
// buffer ceiling (16 MB) is sufficient to parse a tool_use block whose
// input_json_delta data line exceeds 100 KB.
func TestParseAnthropicSSE_LargeInputJSONDelta(t *testing.T) {
	bigVal := strings.Repeat("z", 110*1024)
	// partial_json value is a JSON object string: {"data":"zzz..."}
	partialJSONStr := `{"data":"` + bigVal + `"}`

	// Build the delta JSON object with the large partial_json string.
	// Use encoding/json so the string is correctly escaped.
	deltaObj := map[string]any{
		"type":  "content_block_delta",
		"index": 0,
		"delta": map[string]any{
			"type":         "input_json_delta",
			"partial_json": partialJSONStr,
		},
	}
	deltaJSON, err := json.Marshal(deltaObj)
	if err != nil {
		t.Fatalf("json.Marshal delta: %v", err)
	}

	var b strings.Builder
	writeLine := func(event, data string) {
		b.WriteString("event: ")
		b.WriteString(event)
		b.WriteByte('\n')
		b.WriteString("data: ")
		b.WriteString(data)
		b.WriteByte('\n')
		b.WriteByte('\n')
	}
	writeLine("message_start", `{"type":"message_start","message":{"id":"msg_large","usage":{"input_tokens":5,"output_tokens":1}}}`)
	writeLine("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_big","name":"BigTool","input":{}}}`)
	b.WriteString("event: content_block_delta\n")
	b.WriteString("data: ")
	b.Write(deltaJSON)
	b.WriteByte('\n')
	b.WriteByte('\n')
	writeLine("content_block_stop", `{"type":"content_block_stop","index":0}`)
	writeLine("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":10}}`)
	writeLine("message_stop", `{"type":"message_stop"}`)

	events, err := ParseAnthropicSSE(strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("ParseAnthropicSSE large delta: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2; events=%+v", len(events), events)
	}
	if events[0].Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", events[0].Type)
	}
	if events[0].Tool == nil {
		t.Fatal("Tool is nil")
	}
	got, _ := events[0].Tool.Input["data"].(string)
	if len(got) != len(bigVal) {
		t.Errorf("Input[data] length = %d, want %d", len(got), len(bigVal))
	}
}
