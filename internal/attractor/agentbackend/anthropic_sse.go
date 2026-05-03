// Anthropic Messages API SSE codec — parses the Server-Sent Events
// stream that Anthropic's streaming endpoint emits and normalizes it
// into the agentbackend.TurnEvent stream every codec produces.
//
// Block 6 Step 2: codec extraction campaign.
//
// Wire format summary:
//
//	event: <type>
//	data: <json object>
//	<blank line>
//
// We track per-block state (type, accumulated text, tool metadata)
// keyed by the integer block index Anthropic sends in every event.
// At content_block_stop we flush the accumulated state to a TurnEvent.
// At message_stop we emit TurnEventTurnEnd with the stop_reason and
// token usage gathered from message_start / message_delta.
package agentbackend

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// ParseAnthropicSSE reads Anthropic Messages API SSE-format streaming
// output (Server-Sent Events with event: + data: lines) and returns
// the corresponding TurnEvents. Mirrors ParseClaudeCLIJSONL's contract:
// malformed lines tolerated, errors returned only for unrecoverable
// reader failures.
func ParseAnthropicSSE(r io.Reader) ([]TurnEvent, error) {
	if r == nil {
		return nil, nil
	}

	var events []TurnEvent

	sc := bufio.NewScanner(r)
	// Tool inputs / large text blocks can exceed the 64 KB default.
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	// per-block accumulated state
	type blockState struct {
		typ      string          // "text", "thinking", "tool_use"
		text     strings.Builder // for text + thinking blocks
		toolID   string
		toolName string
		toolArgs strings.Builder // for tool_use blocks (partial_json)
	}
	blocks := map[int]*blockState{}

	getBlock := func(idx int) *blockState {
		st := blocks[idx]
		if st == nil {
			st = &blockState{}
			blocks[idx] = st
		}
		return st
	}

	// end metadata accumulated across message_start / message_delta
	var pendingEnd TurnEndInfo

	var curEvent string // last seen "event:" value

	for sc.Scan() {
		line := sc.Text()

		switch {
		case strings.HasPrefix(line, "event:"):
			curEvent = strings.TrimSpace(line[len("event:"):])

		case strings.HasPrefix(line, "data:"):
			data := strings.TrimSpace(line[len("data:"):])
			if data == "" {
				continue
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(data), &payload); err != nil {
				// Tolerate malformed data lines.
				continue
			}
			switch curEvent {
			case "message_start":
				// Capture input-token usage reported at message open.
				if msg, ok := payload["message"].(map[string]any); ok {
					if u, ok := msg["usage"].(map[string]any); ok {
						if n := intFromAny(u["input_tokens"]); n > 0 {
							pendingEnd.InputTokens = n
						}
					}
				}

			case "content_block_start":
				idx := intFromAny(payload["index"])
				cb, _ := payload["content_block"].(map[string]any)
				if cb == nil {
					continue
				}
				st := getBlock(idx)
				st.typ, _ = cb["type"].(string)
				if st.typ == "tool_use" {
					st.toolID, _ = cb["id"].(string)
					st.toolName, _ = cb["name"].(string)
				}

			case "content_block_delta":
				idx := intFromAny(payload["index"])
				st := getBlock(idx)
				delta, _ := payload["delta"].(map[string]any)
				if delta == nil {
					continue
				}
				deltaType, _ := delta["type"].(string)
				switch deltaType {
				case "text_delta":
					text, _ := delta["text"].(string)
					st.text.WriteString(text)
				case "thinking_delta":
					// Anthropic uses "thinking" as the field name inside
					// thinking_delta; fall back to "text" for compatibility.
					text, _ := delta["thinking"].(string)
					if text == "" {
						text, _ = delta["text"].(string)
					}
					st.text.WriteString(text)
				case "input_json_delta":
					partial, _ := delta["partial_json"].(string)
					st.toolArgs.WriteString(partial)
				}

			case "content_block_stop":
				idx := intFromAny(payload["index"])
				st := blocks[idx]
				if st == nil {
					continue
				}
				switch st.typ {
				case "text":
					if st.text.Len() > 0 {
						events = append(events, TurnEvent{
							Type: TurnEventText,
							Text: st.text.String(),
						})
					}
				case "thinking":
					if st.text.Len() > 0 {
						events = append(events, TurnEvent{
							Type: TurnEventThinking,
							Text: st.text.String(),
						})
					}
				case "tool_use":
					var input map[string]any
					if s := st.toolArgs.String(); s != "" {
						// Best-effort parse; nil input becomes empty map.
						_ = json.Unmarshal([]byte(s), &input)
					}
					if input == nil {
						input = map[string]any{}
					}
					events = append(events, TurnEvent{
						Type: TurnEventToolUse,
						Tool: &ToolCall{
							ID:    st.toolID,
							Name:  st.toolName,
							Input: input,
						},
					})
				}

			case "message_delta":
				// stop_reason lives inside delta.delta; usage is top-level.
				if d, ok := payload["delta"].(map[string]any); ok {
					if sr, _ := d["stop_reason"].(string); sr != "" {
						pendingEnd.StopReason = sr
					}
				}
				if u, ok := payload["usage"].(map[string]any); ok {
					if n := intFromAny(u["output_tokens"]); n > 0 {
						pendingEnd.OutputTokens = n
					}
					if n := intFromAny(u["input_tokens"]); n > 0 {
						pendingEnd.InputTokens = n
					}
				}

			case "message_stop":
				// Copy the accumulated end info so mutation after this
				// point doesn't affect the emitted event.
				end := pendingEnd
				events = append(events, TurnEvent{
					Type: TurnEventTurnEnd,
					End:  &end,
				})
				// Cost is not present in the streaming API; CostUSD stays nil.
			}

			// blank lines and any other line type (e.g. ": comment") are ignored
		}
	}
	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("anthropic sse scan: %w", err)
	}
	return events, nil
}
