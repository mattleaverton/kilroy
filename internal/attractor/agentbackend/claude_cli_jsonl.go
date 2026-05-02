// claude-CLI JSONL codec — parses the wire format `claude --print
// --output-format stream-json --verbose` emits, normalizing it to
// the agentbackend.TurnEvent stream every codec produces.
//
// This is the first concrete codec landed under Block 6 step 2. The
// extraction pressure-tests the agentbackend types: every shape that
// appears in real captures (in ~/.local/state/kilroy/attractor/runs/*/
// agent/agent_output.jsonl) maps cleanly to an existing TurnEvent
// variant — no type changes were required.

package agentbackend

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// ParseClaudeCLIJSONL reads the JSONL stream a claude-CLI subprocess
// emits and returns the corresponding TurnEvents. Malformed lines are
// skipped (the wire format occasionally sees stray output); a non-nil
// error is returned only for unrecoverable I/O failures on the reader.
//
// The wire format is one JSON object per line. Recognized top-level
// `type` values:
//
//   - `system` (subtype="init") — connection / session metadata.
//     Skipped: not a turn event.
//   - `assistant` — one assistant message containing one or more
//     content blocks. Each block becomes a TurnEvent: text →
//     TurnEventText, thinking → TurnEventThinking, tool_use →
//     TurnEventToolUse.
//   - `user` — driver-relayed tool results from the previous turn's
//     tool_use blocks. Each tool_result block becomes a
//     TurnEventToolResult.
//   - `result` — terminal event with stop_reason, token usage, cost.
//     Becomes TurnEventTurnEnd.
//
// Unknown types are skipped silently. Each `assistant` line with N
// content blocks expands to N events in document order.
func ParseClaudeCLIJSONL(r io.Reader) ([]TurnEvent, error) {
	if r == nil {
		return nil, nil
	}
	var events []TurnEvent
	sc := bufio.NewScanner(r)
	// claude lines can be very large (full tool inputs/outputs). Bump the
	// per-line buffer ceiling well above the 64KB scanner default.
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			// Tolerate malformed lines — they happen with mixed stdout/stderr.
			continue
		}
		t, _ := raw["type"].(string)
		switch t {
		case "system":
			// init metadata only — no turn-event meaning.
			continue
		case "assistant":
			events = append(events, parseClaudeAssistantLine(raw)...)
		case "user":
			events = append(events, parseClaudeUserLine(raw)...)
		case "result":
			events = append(events, parseClaudeResultLine(raw))
		}
	}
	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("claude jsonl scan: %w", err)
	}
	return events, nil
}

// parseClaudeAssistantLine extracts one event per content block in an
// assistant message. Block types we recognize: text, thinking, tool_use.
// Unknown block types are silently skipped.
func parseClaudeAssistantLine(raw map[string]any) []TurnEvent {
	msg, _ := raw["message"].(map[string]any)
	if msg == nil {
		return nil
	}
	content, _ := msg["content"].([]any)
	out := make([]TurnEvent, 0, len(content))
	for _, item := range content {
		block, _ := item.(map[string]any)
		if block == nil {
			continue
		}
		switch t, _ := block["type"].(string); t {
		case "text":
			text, _ := block["text"].(string)
			if text == "" {
				continue
			}
			out = append(out, TurnEvent{Type: TurnEventText, Text: text})
		case "thinking":
			text, _ := block["thinking"].(string)
			if text == "" {
				continue
			}
			out = append(out, TurnEvent{Type: TurnEventThinking, Text: text})
		case "tool_use":
			id, _ := block["id"].(string)
			name, _ := block["name"].(string)
			input, _ := block["input"].(map[string]any)
			if input == nil {
				input = map[string]any{}
			}
			out = append(out, TurnEvent{
				Type: TurnEventToolUse,
				Tool: &ToolCall{ID: id, Name: name, Input: input},
			})
		}
	}
	return out
}

// parseClaudeUserLine extracts one TurnEventToolResult per tool_result
// block. Claude's tool_result.content is either a plain string or a
// list of content blocks (typically text); we normalize to a string by
// concatenating any text-shaped blocks. Non-text content (image, etc.)
// is preserved as a JSON-stringified placeholder so callers can
// observe its shape without claiming to render it.
func parseClaudeUserLine(raw map[string]any) []TurnEvent {
	msg, _ := raw["message"].(map[string]any)
	if msg == nil {
		return nil
	}
	content, _ := msg["content"].([]any)
	out := make([]TurnEvent, 0, len(content))
	for _, item := range content {
		block, _ := item.(map[string]any)
		if block == nil {
			continue
		}
		if t, _ := block["type"].(string); t != "tool_result" {
			continue
		}
		toolUseID, _ := block["tool_use_id"].(string)
		isError, _ := block["is_error"].(bool)
		out = append(out, TurnEvent{
			Type: TurnEventToolResult,
			Result: &ToolResult{
				ToolUseID: toolUseID,
				Content:   stringifyClaudeToolResultContent(block["content"]),
				IsError:   isError,
			},
		})
	}
	return out
}

// stringifyClaudeToolResultContent normalizes claude's tool_result
// content (string | []contentBlock) to a single string. Text blocks
// are concatenated with newlines; non-text blocks render as a JSON
// fragment so the structural cue survives without a rendering claim.
func stringifyClaudeToolResultContent(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case []any:
		var out []byte
		for i, item := range c {
			if i > 0 {
				out = append(out, '\n')
			}
			block, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := block["type"].(string); t == "text" {
				if text, _ := block["text"].(string); text != "" {
					out = append(out, text...)
					continue
				}
			}
			// Non-text block: emit a JSON fragment so callers can see what
			// shape the model returned without us pretending to render it.
			if encoded, err := json.Marshal(block); err == nil {
				out = append(out, encoded...)
			}
		}
		return string(out)
	}
	return ""
}

// parseClaudeResultLine maps the terminal `result` event to TurnEventTurnEnd.
// Pulls stop_reason from the canonical subtype field (claude-CLI emits
// "success" / "error_max_turns" / etc.) and usage/cost from the
// well-known top-level fields. CostUSD stays nil when cost wasn't
// reported, which is distinguishable from $0.00.
func parseClaudeResultLine(raw map[string]any) TurnEvent {
	subtype, _ := raw["subtype"].(string)
	end := &TurnEndInfo{StopReason: subtype}

	if usage, ok := raw["usage"].(map[string]any); ok {
		end.InputTokens = intFromAny(usage["input_tokens"])
		end.OutputTokens = intFromAny(usage["output_tokens"])
	}
	if cost, ok := raw["total_cost_usd"].(float64); ok {
		end.CostUSD = &cost
	}
	return TurnEvent{Type: TurnEventTurnEnd, End: end}
}

// intFromAny coerces a JSON number (always float64 after Unmarshal into
// any) to an int. Returns 0 for absent / non-numeric values.
func intFromAny(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}
