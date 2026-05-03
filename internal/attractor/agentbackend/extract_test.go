// Tests for cross-codec text extraction. The four codecs all emit
// TurnEventText for assistant text; this helper drains them uniformly.

package agentbackend

import (
	"strings"
	"testing"
)

func TestExtractText_OnlyTextEvents(t *testing.T) {
	events := []TurnEvent{
		{Type: TurnEventText, Text: "hello"},
		{Type: TurnEventThinking, Text: "internal pondering — should be excluded"},
		{Type: TurnEventText, Text: "world"},
		{Type: TurnEventToolUse, Tool: &ToolCall{Name: "Read"}},
		{Type: TurnEventTurnEnd, End: &TurnEndInfo{StopReason: "end_turn"}},
	}
	got := ExtractText(events)
	want := "hello\n\nworld"
	if got != want {
		t.Errorf("ExtractText = %q, want %q", got, want)
	}
}

func TestExtractText_EmptyTextSkipped(t *testing.T) {
	events := []TurnEvent{
		{Type: TurnEventText, Text: ""},
		{Type: TurnEventText, Text: "actual content"},
		{Type: TurnEventText, Text: ""},
	}
	got := ExtractText(events)
	want := "actual content"
	if got != want {
		t.Errorf("ExtractText = %q, want %q", got, want)
	}
}

func TestExtractText_NoEvents(t *testing.T) {
	if got := ExtractText(nil); got != "" {
		t.Errorf("ExtractText(nil) = %q, want empty", got)
	}
	if got := ExtractText([]TurnEvent{}); got != "" {
		t.Errorf("ExtractText([]) = %q, want empty", got)
	}
}

func TestParseAndExtractText_Claude(t *testing.T) {
	jsonl := `{"type":"system","subtype":"init","cwd":"/tmp"}
{"type":"assistant","message":{"content":[{"type":"text","text":"Done."}]}}
{"type":"result","subtype":"success","result":"Done."}
`
	got := ParseAndExtractText("claude", []byte(jsonl))
	if !strings.Contains(got, "Done.") {
		t.Errorf("ParseAndExtractText(claude) = %q, want to contain 'Done.'", got)
	}
}

func TestParseAndExtractText_UnknownTool(t *testing.T) {
	if got := ParseAndExtractText("unknown_tool", []byte(`{"type":"x"}`)); got != "" {
		t.Errorf("ParseAndExtractText(unknown) = %q, want empty", got)
	}
}

func TestParseForTool_Codex(t *testing.T) {
	// Use the corpus shape codex_cli_jsonl.go expects: item.completed +
	// agent_message yields a text event.
	jsonl := `{"type":"thread.started","thread_id":"t1"}
{"type":"item.completed","item":{"type":"agent_message","text":"All done"}}
{"type":"turn.completed"}
`
	events, err := ParseForTool("codex", []byte(jsonl))
	if err != nil {
		t.Fatalf("ParseForTool(codex): %v", err)
	}
	got := ExtractText(events)
	if !strings.Contains(got, "All done") {
		t.Errorf("got %q, want to contain 'All done'", got)
	}
}
