// OpenAI Responses API SSE codec — parses the Server-Sent Events wire
// format that the OpenAI Responses streaming endpoint emits, normalising
// it to the agentbackend.TurnEvent stream every codec produces.
//
// Wire format: event:<type>\ndata:<json>\n\n (blank line terminates each
// event).  Unknown events and malformed JSON payloads are silently skipped
// so the codec tolerates mixed stdout/stderr in captured recordings.

package agentbackend

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// ParseOpenAISSE reads OpenAI Responses API SSE-format streaming output
// (Server-Sent Events with event: + data: lines) and returns the
// corresponding TurnEvents. Mirrors ParseClaudeCLIJSONL's contract:
// malformed lines tolerated, errors returned only for unrecoverable
// reader failures.
func ParseOpenAISSE(r io.Reader) ([]TurnEvent, error) {
	if r == nil {
		return nil, nil
	}

	var events []TurnEvent

	// Per-item accumulators keyed by item_id (= item["id"] from
	// response.output_item.added).
	type textAcc struct{ buf strings.Builder }
	type thinkAcc struct{ buf strings.Builder }
	type toolAcc struct {
		// callID is item.call_id — what the model exposes in its output and
		// what callers must echo back in tool_result messages.
		callID string
		name   string
		buf    strings.Builder
	}

	texts := map[string]*textAcc{}        // key: itemID:contentIndex
	thinks := map[string]*thinkAcc{}      // key: itemID
	tools := map[string]*toolAcc{}        // key: itemID (item["id"])
	toolByCallID := map[string]*toolAcc{} // secondary: item.call_id

	// lookupToolAcc finds toolAcc by itemID first, then callID.
	lookupToolAcc := func(itemID, callID string) *toolAcc {
		if itemID != "" {
			if st, ok := tools[itemID]; ok {
				return st
			}
		}
		if callID != "" {
			if st, ok := toolByCallID[callID]; ok {
				return st
			}
		}
		return nil
	}

	var curEvent string
	var curData []byte

	// dispatch processes the current accumulated SSE event.  Called on every
	// blank separator line and once more at EOF for trailing events.
	dispatch := func() {
		if len(curData) == 0 {
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(curData, &payload); err != nil {
			// Tolerate malformed JSON — captures occasionally mix stderr.
			return
		}

		// Prefer the `type` field embedded in the JSON; fall back to the SSE
		// event: header so both API styles work.
		typ, _ := payload["type"].(string)
		if typ == "" {
			typ = curEvent
		}

		switch typ {

		// ── lifecycle events with no turn-event meaning ────────────────────
		case "response.created", "response.in_progress":
			// session metadata only; skip.

		// ── output item bookkeeping ────────────────────────────────────────
		case "response.output_item.added":
			item, _ := payload["item"].(map[string]any)
			if item == nil {
				return
			}
			itemID, _ := item["id"].(string)
			itemType, _ := item["type"].(string)
			switch itemType {
			case "message":
				// Text content parts are registered when
				// response.content_part.added fires; nothing to do here.
			case "reasoning":
				if itemID != "" {
					thinks[itemID] = &thinkAcc{}
				}
			case "function_call":
				if itemID == "" {
					return
				}
				callID, _ := item["call_id"].(string)
				name, _ := item["name"].(string)
				st := &toolAcc{name: name, callID: callID}
				if st.callID == "" {
					st.callID = itemID
				}
				tools[itemID] = st
				if callID != "" {
					toolByCallID[callID] = st
				}
			}

		// ── text content part opened ───────────────────────────────────────
		case "response.content_part.added":
			part, _ := payload["part"].(map[string]any)
			if part == nil {
				return
			}
			if partType, _ := part["type"].(string); partType != "output_text" {
				return
			}
			itemID, _ := payload["item_id"].(string)
			contentIdx := int(floatFromAny(payload["content_index"]))
			texts[sseTextKey(itemID, contentIdx)] = &textAcc{}

		// ── text streaming ─────────────────────────────────────────────────
		case "response.output_text.delta":
			itemID, _ := payload["item_id"].(string)
			contentIdx := int(floatFromAny(payload["content_index"]))
			delta, _ := payload["delta"].(string)
			if delta == "" {
				delta, _ = payload["text"].(string)
			}
			key := sseTextKey(itemID, contentIdx)
			st := texts[key]
			if st == nil {
				st = &textAcc{}
				texts[key] = st
			}
			st.buf.WriteString(delta)

		case "response.output_text.done":
			itemID, _ := payload["item_id"].(string)
			contentIdx := int(floatFromAny(payload["content_index"]))
			// Prefer the final text in the done event; fall back to accumulated
			// deltas for API shapes that omit it.
			text, _ := payload["text"].(string)
			if text == "" {
				if st := texts[sseTextKey(itemID, contentIdx)]; st != nil {
					text = st.buf.String()
				}
			}
			if text != "" {
				events = append(events, TurnEvent{Type: TurnEventText, Text: text})
			}

		// ── reasoning / thinking streaming ────────────────────────────────
		case "response.reasoning_summary.delta":
			itemID, _ := payload["item_id"].(string)
			delta, _ := payload["delta"].(string)
			if itemID == "" {
				return
			}
			st := thinks[itemID]
			if st == nil {
				st = &thinkAcc{}
				thinks[itemID] = st
			}
			st.buf.WriteString(delta)

		case "response.reasoning_summary.done":
			itemID, _ := payload["item_id"].(string)
			text, _ := payload["text"].(string)
			if text == "" {
				if st := thinks[itemID]; st != nil {
					text = st.buf.String()
				}
			}
			if text != "" {
				events = append(events, TurnEvent{Type: TurnEventThinking, Text: text})
			}

		// ── function-call argument streaming ──────────────────────────────
		case "response.function_call_arguments.delta":
			itemID, _ := payload["item_id"].(string)
			callID, _ := payload["call_id"].(string)
			delta, _ := payload["delta"].(string)
			if delta == "" {
				delta, _ = payload["arguments"].(string)
			}

			st := lookupToolAcc(itemID, callID)
			if st == nil {
				// First delta before output_item.added; synthesise state.
				key := itemID
				if key == "" {
					key = callID
				}
				if key == "" {
					return
				}
				name, _ := payload["name"].(string)
				st = &toolAcc{callID: callID, name: name}
				if st.callID == "" {
					st.callID = key
				}
				tools[key] = st
				if callID != "" {
					toolByCallID[callID] = st
				}
			}
			st.buf.WriteString(delta)

		case "response.function_call_arguments.done":
			itemID, _ := payload["item_id"].(string)
			callID, _ := payload["call_id"].(string)
			argsStr, _ := payload["arguments"].(string)

			st := lookupToolAcc(itemID, callID)
			if st == nil {
				key := itemID
				if key == "" {
					key = callID
				}
				if key == "" {
					return
				}
				name, _ := payload["name"].(string)
				st = &toolAcc{callID: callID, name: name}
				if st.callID == "" {
					st.callID = key
				}
				tools[key] = st
			}

			// Done event carries the final accumulated string; prefer it over
			// what we buffered (both should be identical in practice).
			if argsStr == "" {
				argsStr = st.buf.String()
			}

			var input map[string]any
			if argsStr != "" {
				_ = json.Unmarshal([]byte(argsStr), &input)
			}
			if input == nil {
				input = map[string]any{}
			}

			id := st.callID
			if id == "" {
				id = itemID
			}
			events = append(events, TurnEvent{
				Type: TurnEventToolUse,
				Tool: &ToolCall{ID: id, Name: st.name, Input: input},
			})

		// ── turn completion ────────────────────────────────────────────────
		case "response.completed":
			rawResp, _ := payload["response"].(map[string]any)
			if rawResp == nil {
				rawResp = payload
			}
			status, _ := rawResp["status"].(string)
			end := &TurnEndInfo{StopReason: status}
			if usage, ok := rawResp["usage"].(map[string]any); ok {
				end.InputTokens = intFromAny(usage["input_tokens"])
				end.OutputTokens = intFromAny(usage["output_tokens"])
			}
			events = append(events, TurnEvent{Type: TurnEventTurnEnd, End: end})
		}
	}

	sc := bufio.NewScanner(r)
	// Function-call argument blobs can be very large; raise the per-line
	// ceiling well above the 64KB bufio default (mirrors ParseClaudeCLIJSONL).
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	for sc.Scan() {
		line := sc.Text()

		switch {
		case line == "":
			// Blank line: end of SSE event — dispatch and reset state.
			dispatch()
			curEvent = ""
			curData = nil

		case strings.HasPrefix(line, "event:"):
			curEvent = strings.TrimSpace(line[6:])

		case strings.HasPrefix(line, "data:"):
			data := line[5:]
			if len(data) > 0 && data[0] == ' ' {
				data = data[1:]
			}
			// Multiple data: lines in one event are joined by newline per the
			// SSE specification (rare in practice for OpenAI Responses API).
			if curData == nil {
				curData = []byte(data)
			} else {
				curData = append(curData, '\n')
				curData = append(curData, data...)
			}

		case strings.HasPrefix(line, ":"):
			// SSE comment line; ignore.

			// All other lines are stray bytes (mixed stderr, etc.); tolerate.
		}
	}

	// Handle any trailing event not followed by a terminal blank line.
	dispatch()

	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("openai sse scan: %w", err)
	}
	return events, nil
}

// sseTextKey builds the accumulator map key for an output_text part,
// combining item ID and content index so multiple text parts inside a
// single message item each get their own buffer.
func sseTextKey(itemID string, contentIndex int) string {
	return fmt.Sprintf("%s:%d", itemID, contentIndex)
}

// floatFromAny extracts a float64 from a JSON-decoded value (json.Unmarshal
// into any always produces float64 for numbers).  Returns 0 for absent or
// non-numeric values.
func floatFromAny(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}
