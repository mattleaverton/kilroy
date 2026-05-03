// Tests for ParseOpenAISSE.  Each test exercises one of the eight
// requirements listed in the Block 6 Step 2 spec.  SSE input is written
// inline as a raw string so the shapes are easy to read and compare against
// the real OpenAI Responses API wire format.

package agentbackend

import (
	"encoding/json"
	"strings"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// mustParseSSE calls ParseOpenAISSE and fails the test on any error.
func mustParseSSE(t *testing.T, src string) []TurnEvent {
	t.Helper()
	events, err := ParseOpenAISSE(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseOpenAISSE error: %v", err)
	}
	return events
}

// jsonEncode returns the JSON encoding of v as a string.  Used to embed
// large strings inside inline SSE fixture payloads without hand-quoting.
func jsonEncode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// ─── test 1: single message with one output_text part ────────────────────────

// TestParseOpenAISSE_SingleTextMessage confirms the typical "assistant says
// hello" response is normalised to one TurnEventText followed by
// TurnEventTurnEnd.
func TestParseOpenAISSE_SingleTextMessage(t *testing.T) {
	const src = `event: response.created
data: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}

event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}

event: response.content_part.added
data: {"type":"response.content_part.added","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"Hello"}

event: response.output_text.delta
data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":", world!"}

event: response.output_text.done
data: {"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":0,"text":"Hello, world!"}

event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":5,"output_tokens":3}}}

`
	events := mustParseSSE(t, src)

	var textEvents, endEvents int
	for _, e := range events {
		switch e.Type {
		case TurnEventText:
			textEvents++
			if e.Text != "Hello, world!" {
				t.Errorf("TurnEventText.Text = %q, want %q", e.Text, "Hello, world!")
			}
		case TurnEventTurnEnd:
			endEvents++
		}
	}
	if textEvents != 1 {
		t.Errorf("TurnEventText count = %d, want 1", textEvents)
	}
	if endEvents != 1 {
		t.Errorf("TurnEventTurnEnd count = %d, want 1", endEvents)
	}
}

// ─── test 2: reasoning summary block → TurnEventThinking ─────────────────────

// TestParseOpenAISSE_ReasoningBlock confirms that a reasoning summary
// stream (response.reasoning_summary.delta + .done) emits exactly one
// TurnEventThinking with the accumulated text.
func TestParseOpenAISSE_ReasoningBlock(t *testing.T) {
	const src = `event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}

event: response.reasoning_summary.delta
data: {"type":"response.reasoning_summary.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":"I'm thinking"}

event: response.reasoning_summary.delta
data: {"type":"response.reasoning_summary.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":" about this."}

event: response.reasoning_summary.done
data: {"type":"response.reasoning_summary.done","item_id":"rs_1","output_index":0,"summary_index":0,"text":"I'm thinking about this."}

`
	events := mustParseSSE(t, src)

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1; events = %+v", len(events), events)
	}
	if events[0].Type != TurnEventThinking {
		t.Fatalf("events[0].Type = %s, want thinking", events[0].Type)
	}
	if events[0].Text != "I'm thinking about this." {
		t.Errorf("thinking text = %q, want %q", events[0].Text, "I'm thinking about this.")
	}
}

// ─── test 3: function call with argument streaming ────────────────────────────

// TestParseOpenAISSE_FunctionCallWithStreaming verifies that accumulated
// function-call argument chunks are assembled, parsed as JSON, and emitted
// as TurnEventToolUse with the correct ID, name, and Input map.
func TestParseOpenAISSE_FunctionCallWithStreaming(t *testing.T) {
	const src = `event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_abc","name":"read_file","arguments":""}}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":"{\"file_path\":"}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":"\"/tmp/test.txt\"}"}

event: response.function_call_arguments.done
data: {"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":0,"arguments":"{\"file_path\":\"/tmp/test.txt\"}"}

`
	events := mustParseSSE(t, src)

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1; events = %+v", len(events), events)
	}
	e := events[0]
	if e.Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", e.Type)
	}
	if e.Tool == nil {
		t.Fatal("Tool is nil")
	}
	if e.Tool.ID != "call_abc" {
		t.Errorf("Tool.ID = %q, want %q", e.Tool.ID, "call_abc")
	}
	if e.Tool.Name != "read_file" {
		t.Errorf("Tool.Name = %q, want %q", e.Tool.Name, "read_file")
	}
	if e.Tool.Input == nil {
		t.Fatal("Tool.Input is nil")
	}
	if got, ok := e.Tool.Input["file_path"]; !ok || got != "/tmp/test.txt" {
		t.Errorf("Tool.Input[file_path] = %v, want /tmp/test.txt", got)
	}
}

// ─── test 4: multiple output items in document order ─────────────────────────

// TestParseOpenAISSE_MultipleOutputItemsInOrder verifies that a response
// containing both a text part and a function call emits events in SSE stream
// order (text first, then tool_use).
func TestParseOpenAISSE_MultipleOutputItemsInOrder(t *testing.T) {
	const src = `event: response.output_item.added
data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}

event: response.content_part.added
data: {"type":"response.content_part.added","item_id":"msg_1","content_index":0,"part":{"type":"output_text","text":""}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","item_id":"msg_1","content_index":0,"delta":"Sure, I will run that."}

event: response.output_text.done
data: {"type":"response.output_text.done","item_id":"msg_1","content_index":0,"text":"Sure, I will run that."}

event: response.output_item.added
data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_1","call_id":"call_xyz","name":"bash","arguments":""}}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"command\":\"ls\"}"}

event: response.function_call_arguments.done
data: {"type":"response.function_call_arguments.done","item_id":"fc_1","arguments":"{\"command\":\"ls\"}"}

`
	events := mustParseSSE(t, src)

	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2; events = %+v", len(events), events)
	}
	if events[0].Type != TurnEventText {
		t.Errorf("events[0].Type = %s, want text", events[0].Type)
	}
	if events[0].Text != "Sure, I will run that." {
		t.Errorf("text = %q, want %q", events[0].Text, "Sure, I will run that.")
	}
	if events[1].Type != TurnEventToolUse {
		t.Errorf("events[1].Type = %s, want tool_use", events[1].Type)
	}
	if events[1].Tool == nil || events[1].Tool.Name != "bash" {
		t.Errorf("events[1].Tool = %+v, want name=bash", events[1].Tool)
	}
	if cmd, _ := events[1].Tool.Input["command"].(string); cmd != "ls" {
		t.Errorf("Tool.Input[command] = %q, want ls", cmd)
	}
}

// ─── test 5: response.completed → TurnEventTurnEnd with usage ────────────────

// TestParseOpenAISSE_CompletedWithUsage verifies that response.completed
// produces TurnEventTurnEnd whose End field carries the status as StopReason
// and the usage counts from usage.input_tokens / output_tokens.
func TestParseOpenAISSE_CompletedWithUsage(t *testing.T) {
	const src = `event: response.completed
data: {"type":"response.completed","response":{"id":"resp_2","status":"completed","output":[],"usage":{"input_tokens":42,"output_tokens":17}}}

`
	events := mustParseSSE(t, src)

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1; events = %+v", len(events), events)
	}
	e := events[0]
	if e.Type != TurnEventTurnEnd {
		t.Fatalf("events[0].Type = %s, want turn_end", e.Type)
	}
	if e.End == nil {
		t.Fatal("End is nil")
	}
	if e.End.StopReason != "completed" {
		t.Errorf("StopReason = %q, want %q", e.End.StopReason, "completed")
	}
	if e.End.InputTokens != 42 {
		t.Errorf("InputTokens = %d, want 42", e.End.InputTokens)
	}
	if e.End.OutputTokens != 17 {
		t.Errorf("OutputTokens = %d, want 17", e.End.OutputTokens)
	}
	// Cost stays nil — OpenAI Responses API does not report cost.
	if e.End.CostUSD != nil {
		t.Errorf("CostUSD = %v, want nil", e.End.CostUSD)
	}
}

// ─── test 6: malformed lines are tolerated ───────────────────────────────────

// TestParseOpenAISSE_MalformedLinesTolerated verifies that stray non-SSE
// bytes, invalid JSON payloads, and the [DONE] sentinel do not crash the
// parser or prevent it from processing subsequent valid events.
func TestParseOpenAISSE_MalformedLinesTolerated(t *testing.T) {
	const src = `this is not an SSE line at all
event: response.output_text.done
data: not valid json

event: response.completed
data: {"type":"response.completed","response":{"id":"r1","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}

data: [DONE]

`
	events := mustParseSSE(t, src)

	// The malformed payload should be skipped; only the completed event fires.
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1 (malformed skipped); events = %+v", len(events), events)
	}
	if events[0].Type != TurnEventTurnEnd {
		t.Errorf("events[0].Type = %s, want turn_end", events[0].Type)
	}
}

// ─── test 7: empty input ─────────────────────────────────────────────────────

// TestParseOpenAISSE_EmptyInput confirms a zero-byte reader produces an
// empty (not nil) slice with no error.
func TestParseOpenAISSE_EmptyInput(t *testing.T) {
	events, err := ParseOpenAISSE(strings.NewReader(""))
	if err != nil {
		t.Fatalf("error on empty input: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("events = %+v, want empty slice", events)
	}
}

// TestParseOpenAISSE_NilReader confirms a nil reader is handled gracefully.
func TestParseOpenAISSE_NilReader(t *testing.T) {
	events, err := ParseOpenAISSE(nil)
	if err != nil {
		t.Fatalf("nil reader err = %v, want nil", err)
	}
	if events != nil {
		t.Errorf("nil reader events = %+v, want nil", events)
	}
}

// ─── test 8: large function_call_arguments delta ─────────────────────────────

// TestParseOpenAISSE_LargeArgumentsDelta confirms the 16 MB buffer ceiling
// lets the codec parse a single function-call argument blob well above the
// bufio.Scanner default of 64 KB (~120 KB in this test).
func TestParseOpenAISSE_LargeArgumentsDelta(t *testing.T) {
	// Build a JSON object {"content":"xxx..."} with a 120 KB value.
	bigVal := strings.Repeat("x", 120*1024)
	argsJSON := `{"content":` + jsonEncode(bigVal) + `}`

	// Build the SSE fixture by concatenating segments so we avoid raw-string
	// quoting headaches with the large embedded value.
	addedLine := `{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_big","call_id":"call_big","name":"write_file","arguments":""}}`
	deltaLine := `{"type":"response.function_call_arguments.delta","item_id":"fc_big","delta":` + jsonEncode(argsJSON) + `}`
	doneLine := `{"type":"response.function_call_arguments.done","item_id":"fc_big","arguments":` + jsonEncode(argsJSON) + `}`

	src := "event: response.output_item.added\ndata: " + addedLine + "\n\n" +
		"event: response.function_call_arguments.delta\ndata: " + deltaLine + "\n\n" +
		"event: response.function_call_arguments.done\ndata: " + doneLine + "\n\n"

	events := mustParseSSE(t, src)

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1; events = %+v", len(events), events)
	}
	e := events[0]
	if e.Type != TurnEventToolUse {
		t.Fatalf("events[0].Type = %s, want tool_use", e.Type)
	}
	if e.Tool == nil {
		t.Fatal("Tool is nil")
	}
	if e.Tool.Name != "write_file" {
		t.Errorf("Tool.Name = %q, want write_file", e.Tool.Name)
	}
	if e.Tool.Input == nil {
		t.Fatal("Tool.Input is nil")
	}
	got, _ := e.Tool.Input["content"].(string)
	if len(got) != len(bigVal) {
		t.Errorf("Input[content] length = %d, want %d", len(got), len(bigVal))
	}
}
