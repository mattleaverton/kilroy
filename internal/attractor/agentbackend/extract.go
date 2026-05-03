// Cross-codec helpers for extracting human-readable text from a parsed
// TurnEvent stream. The four codecs (claude_cli_jsonl, codex_cli_jsonl,
// anthropic_sse, openai_sse) all emit TurnEventText for assistant text;
// callers downstream — response.md generation, summary stages, UI
// renderers — can drain a stream uniformly via these helpers without
// caring which codec produced it.
//
// This is the seam where Block 6 Step 3 starts paying off in production:
// instead of parallel parsers in agents/agentlog/extract.go and the new
// codecs both reading the same JSONL, callers route through the new
// codec + this helper.

package agentbackend

import (
	"bytes"
	"strings"
)

// ExtractText returns the concatenated text from every TurnEventText in
// the stream, joined with double newlines (paragraph break). Thinking
// blocks, tool calls, and tool results are intentionally excluded —
// callers wanting structured output should iterate the events directly.
//
// This is the agentbackend equivalent of agents/agentlog.ExtractResponseText
// but driven by the unified TurnEvent surface, so all four codecs feed
// the same downstream extraction.
func ExtractText(events []TurnEvent) string {
	if len(events) == 0 {
		return ""
	}
	var parts []string
	for _, ev := range events {
		if ev.Type == TurnEventText && ev.Text != "" {
			parts = append(parts, ev.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// ParseAndExtractText is the convenience entry point for callers that
// have a JSONL/SSE byte buffer keyed by tool name. Dispatches to the
// matching codec and returns the extracted assistant text.
//
// Tool names match the existing agentlog ParserForTool keys ("claude",
// "codex"); SSE-based codecs are reserved for API-path callers that
// route differently.
func ParseAndExtractText(tool string, data []byte) string {
	events, err := ParseForTool(tool, data)
	if err != nil || len(events) == 0 {
		return ""
	}
	return ExtractText(events)
}

// ParseForTool dispatches to the appropriate JSONL codec by tool name.
// Returns (nil, nil) for unknown tools — callers fall back gracefully
// to a per-tool extractor or accept empty events. Errors are reserved
// for unrecoverable I/O on the byte buffer (rare; these codecs tolerate
// malformed lines internally).
func ParseForTool(tool string, data []byte) ([]TurnEvent, error) {
	switch tool {
	case "claude":
		return ParseClaudeCLIJSONL(bytes.NewReader(data))
	case "codex":
		return ParseCodexCLIJSONL(bytes.NewReader(data))
	}
	return nil, nil
}
