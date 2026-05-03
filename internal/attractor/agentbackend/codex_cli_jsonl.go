// codex-CLI JSONL codec — parses the wire format `codex exec --json`
// emits, normalizing it to the agentbackend.TurnEvent stream every
// codec produces.
//
// Codex emits one JSON object per line. The top-level `type` field
// discriminates the event kind:
//
//   - `item.completed` with item.type="agent_message" — assistant text.
//     Becomes TurnEventText.
//   - `item.started` with item.type="command_execution" — a shell
//     command beginning. Becomes TurnEventToolUse (name="command").
//   - `item.completed` with item.type="command_execution" — a shell
//     command finished. Becomes TurnEventToolResult (ToolUseID = item.id).
//   - `item.started` with item.type="file_change" — a file operation
//     beginning. Becomes TurnEventToolUse (name="file_change").
//   - `item.completed` with item.type="file_change" — a file operation
//     finished. Becomes TurnEventToolResult (ToolUseID = item.id).
//   - `turn.completed` — terminal event with usage counters and optional
//     stop_reason / cost. Becomes TurnEventTurnEnd.
//
// All other types (thread.started, turn.started, …) are skipped silently.
// Malformed lines are tolerated — the wire format occasionally interleaves
// stray stderr with the JSONL stream.
package agentbackend

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// ParseCodexCLIJSONL reads `codex exec --json` output (one JSON object
// per line) and returns the corresponding TurnEvents. Mirrors
// ParseClaudeCLIJSONL's contract: malformed lines tolerated, errors
// returned only for unrecoverable reader failures.
func ParseCodexCLIJSONL(r io.Reader) ([]TurnEvent, error) {
	if r == nil {
		return nil, nil
	}
	var events []TurnEvent
	sc := bufio.NewScanner(r)
	// codex lines can be very large (full file contents, long command
	// outputs). Bump the per-line buffer ceiling above the 64 KB default.
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			// Tolerate malformed lines — mixed stdout/stderr is common.
			continue
		}
		t, _ := raw["type"].(string)
		switch t {
		case "item.started":
			events = append(events, parseCodexItemStarted(raw)...)
		case "item.completed":
			events = append(events, parseCodexItemCompleted(raw)...)
		case "turn.completed":
			events = append(events, parseCodexTurnCompleted(raw))
		}
		// thread.started, turn.started, and unknown types are skipped.
	}
	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("codex jsonl scan: %w", err)
	}
	return events, nil
}

// parseCodexItemStarted handles item.started events:
//   - command_execution → TurnEventToolUse (name="command")
//   - file_change       → TurnEventToolUse (name="file_change")
//
// Unknown item types are skipped.
func parseCodexItemStarted(raw map[string]any) []TurnEvent {
	item, _ := raw["item"].(map[string]any)
	if item == nil {
		return nil
	}
	id, _ := item["id"].(string)
	itemType, _ := item["type"].(string)
	switch itemType {
	case "command_execution":
		cmd, _ := item["command"].(string)
		return []TurnEvent{{
			Type: TurnEventToolUse,
			Tool: &ToolCall{
				ID:    id,
				Name:  "command",
				Input: map[string]any{"command": cmd},
			},
		}}
	case "file_change":
		input := map[string]any{}
		// Real wire format carries a "changes" array; older/alternative
		// formats may carry top-level "path" and "action" fields.
		if changes, ok := item["changes"]; ok {
			input["changes"] = changes
		}
		if path, _ := item["path"].(string); path != "" {
			input["path"] = path
		}
		if action, _ := item["action"].(string); action != "" {
			input["action"] = action
		}
		return []TurnEvent{{
			Type: TurnEventToolUse,
			Tool: &ToolCall{
				ID:    id,
				Name:  "file_change",
				Input: input,
			},
		}}
	}
	return nil
}

// parseCodexItemCompleted handles item.completed events:
//   - agent_message     → TurnEventText
//   - command_execution → TurnEventToolResult (ToolUseID = item.id)
//   - file_change       → TurnEventToolResult (ToolUseID = item.id)
//
// Unknown item types are skipped.
func parseCodexItemCompleted(raw map[string]any) []TurnEvent {
	item, _ := raw["item"].(map[string]any)
	if item == nil {
		return nil
	}
	id, _ := item["id"].(string)
	itemType, _ := item["type"].(string)
	switch itemType {
	case "agent_message":
		text, _ := item["text"].(string)
		if text == "" {
			return nil
		}
		return []TurnEvent{{Type: TurnEventText, Text: text}}
	case "command_execution":
		output, _ := item["aggregated_output"].(string)
		exitCode := intFromAny(item["exit_code"])
		return []TurnEvent{{
			Type: TurnEventToolResult,
			Result: &ToolResult{
				ToolUseID: id,
				Content:   output,
				IsError:   exitCode != 0,
			},
		}}
	case "file_change":
		// Report the completed file change as a tool result so callers
		// see a matched ToolUse → ToolResult pair (same item.id).
		status, _ := item["status"].(string)
		return []TurnEvent{{
			Type: TurnEventToolResult,
			Result: &ToolResult{
				ToolUseID: id,
				Content:   status,
			},
		}}
	}
	return nil
}

// parseCodexTurnCompleted maps the terminal turn.completed event to
// TurnEventTurnEnd. Pulls stop_reason, usage counters, and optional
// cost from the well-known top-level fields. CostUSD stays nil when
// not reported — distinguishable from $0.00.
func parseCodexTurnCompleted(raw map[string]any) TurnEvent {
	end := &TurnEndInfo{}
	if stopReason, ok := raw["stop_reason"].(string); ok {
		end.StopReason = stopReason
	}
	if usage, ok := raw["usage"].(map[string]any); ok {
		end.InputTokens = intFromAny(usage["input_tokens"])
		end.OutputTokens = intFromAny(usage["output_tokens"])
	}
	if cost, ok := raw["total_cost_usd"].(float64); ok {
		end.CostUSD = &cost
	}
	return TurnEvent{Type: TurnEventTurnEnd, End: end}
}
