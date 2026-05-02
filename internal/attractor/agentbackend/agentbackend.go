// Package agentbackend defines the unified interface every agent backend
// implements. Plan §9 describes the six-axis tuple (model, driver,
// transport, auth, turn codec, tool control); this package is where the
// turn-codec axis materializes as Go types.
//
// This file is the foundational types-only step (Inv5 §7 step 1) — no
// implementation. Subsequent steps extract the real codecs (Anthropic
// SSE, OpenAI SSE, claude-CLI JSONL, codex-CLI JSONL) as backends that
// emit TurnEvent values through TurnStream, then wrap the existing
// AgentHandler / TmuxAgentHandler as adapters that consume this surface.
//
// Design notes:
//
//   - Cancellation is via context.Context; no separate Cancel() method
//     on the interfaces (per plan §9.3).
//   - SendToolResult lives on TurnStream, not on AgentBackend, because
//     tool-result injection is stateful to a specific in-flight turn.
//   - TurnEvent is a tagged union (Type discriminator + variant fields),
//     not a sealed interface, because Go's type-switch ergonomics for
//     deeply-nested kinds are awkward and the codec layer cares more
//     about cheap construction than open extensibility.
package agentbackend

import (
	"context"
	"errors"
)

// ToolControlMode is the §9 axis that captures who owns the tool-call
// dispatch loop for a given backend.
//
//   - ToolControlKilroy: the API path (and Ollama) — kilroy receives a
//     tool_use event, runs the tool locally, and injects the
//     corresponding tool_result back into the conversation.
//   - ToolControlDriver: the CLI/tmux path — the driver binary
//     (claude/codex/opencode) owns the loop internally; tool-call
//     events arriving at kilroy are informational only, and
//     SendToolResult returns ErrToolControlDriver.
type ToolControlMode int

const (
	ToolControlKilroy ToolControlMode = iota
	ToolControlDriver
)

func (m ToolControlMode) String() string {
	switch m {
	case ToolControlKilroy:
		return "kilroy"
	case ToolControlDriver:
		return "driver"
	default:
		return "unknown"
	}
}

// AgentBackend is the per-(driver, transport, model) handle a caller uses
// to start agent turns. Implementations wrap one of: an LLM HTTP client
// (Anthropic SSE, OpenAI SSE, …), a CLI subprocess (claude --print,
// codex), a tmux pty session, or — eventually — Ollama.
type AgentBackend interface {
	// StartTurn begins a single conversation turn. The returned
	// TurnStream emits events for that turn until Recv returns io.EOF
	// (normal completion) or a non-nil error (failure / cancellation).
	StartTurn(ctx context.Context, msg UserMessage, opts TurnOptions) (TurnStream, error)

	// ToolControl reports who owns the tool-call dispatch loop. Callers
	// branch on this to decide whether to run tools and inject results
	// (ToolControlKilroy) or just observe events (ToolControlDriver).
	ToolControl() ToolControlMode

	// Capabilities surfaces optional features so callers can degrade
	// gracefully (e.g., skip thinking blocks when Thinking is false).
	Capabilities() BackendCapabilities

	// Close releases any backend-held resources (HTTP keepalive
	// connections, tmux sessions, etc.). Calling Close while a
	// TurnStream is still in flight cancels it.
	Close() error
}

// TurnStream is the per-turn handle returned by StartTurn. Recv returns
// the next event in the stream; on normal turn completion Recv returns
// io.EOF. SendToolResult feeds a tool's output back into the
// conversation; in ToolControlDriver mode it returns ErrToolControlDriver
// because the driver owns the loop.
type TurnStream interface {
	Recv() (TurnEvent, error)
	SendToolResult(ctx context.Context, r ToolResult) error
	Close() error
}

// UserMessage is the input that initiates a turn. Kept narrow on
// purpose — backends differ wildly in what additional inputs they
// accept (system prompts, conversation history, attachments); those
// are passed through TurnOptions or pre-configured on the backend.
type UserMessage struct {
	// Text is the user's natural-language prompt for this turn.
	Text string
}

// TurnOptions controls a single turn's invocation. Tools is ignored when
// ToolControl returns ToolControlDriver — the driver binary owns its
// own tool surface.
type TurnOptions struct {
	// Model identifies the concrete model for this turn. Required.
	// Backends may reject mismatches against their configured driver.
	Model string

	// Tools is the JSON-Schema tool surface kilroy will dispatch when
	// ToolControl == ToolControlKilroy. Ignored otherwise.
	Tools []ToolSchema

	// ThinkingBudget caps the model's extended-thinking token budget.
	// Zero means thinking is disabled.
	ThinkingBudget int

	// Extra is a backend-specific escape hatch for options that aren't
	// general enough to surface on this struct. Implementations
	// document their accepted keys in their own godoc.
	Extra map[string]any
}

// ToolSchema describes a single tool kilroy will dispatch when the
// model asks for it. The Parameters shape is JSON Schema; backends
// translate to their wire format (Anthropic input_schema, OpenAI
// parameters, etc).
type ToolSchema struct {
	Name        string
	Description string
	Parameters  map[string]any
}

// BackendCapabilities lets callers ask "can this backend do X?" before
// constructing a request that would otherwise fail at the wire. New
// capabilities should be additive (new bool fields default to false on
// older backends).
type BackendCapabilities struct {
	// Thinking is true when the backend exposes extended-thinking blocks.
	Thinking bool
	// TokenStreaming is true when text events arrive as deltas during a turn.
	TokenStreaming bool
	// CostTracking is true when TurnEndInfo.CostUSD is populated.
	CostTracking bool
	// ToolInjection is true when SendToolResult actually feeds the
	// conversation back to the model. False for ToolControlDriver
	// backends and for codecs that don't support mid-turn tool I/O.
	ToolInjection bool
}

// TurnEventType discriminates TurnEvent's variant fields. New types
// must be appended (existing values are persisted in run logs).
type TurnEventType int

const (
	// TurnEventText is a chunk of natural-language assistant output.
	// In streaming codecs each chunk is a delta; non-streaming codecs
	// emit one Text event per turn with the whole response.
	TurnEventText TurnEventType = iota

	// TurnEventThinking is a chunk of extended-thinking content.
	// Same delta semantics as Text. Backends without Thinking
	// capability never emit this.
	TurnEventThinking

	// TurnEventToolUse signals that the model is requesting a tool
	// invocation. In ToolControlKilroy mode the caller dispatches the
	// tool and replies via SendToolResult. In ToolControlDriver mode
	// it's informational.
	TurnEventToolUse

	// TurnEventToolResult is the driver's report of a tool call's
	// result (only emitted by ToolControlDriver backends; in Kilroy
	// mode the caller knows the result before injecting it).
	TurnEventToolResult

	// TurnEventTurnEnd marks the end of an assistant's turn. After
	// this, Recv returns io.EOF on the next call. End carries
	// stop-reason and usage metadata.
	TurnEventTurnEnd

	// TurnEventError carries a non-fatal anomaly the codec wants
	// callers to know about (rate-limit warning, truncated event
	// stream, decode hiccup that the codec recovered from). Fatal
	// errors come back from Recv as a non-nil error instead.
	TurnEventError
)

func (t TurnEventType) String() string {
	switch t {
	case TurnEventText:
		return "text"
	case TurnEventThinking:
		return "thinking"
	case TurnEventToolUse:
		return "tool_use"
	case TurnEventToolResult:
		return "tool_result"
	case TurnEventTurnEnd:
		return "turn_end"
	case TurnEventError:
		return "error"
	default:
		return "unknown"
	}
}

// TurnEvent is the unified event type every codec emits. Variant fields
// are populated based on Type; consumers should branch on Type and read
// the matching field (others are zero-valued).
type TurnEvent struct {
	Type TurnEventType

	// Text is set for TurnEventText and TurnEventThinking.
	Text string

	// Tool is set for TurnEventToolUse.
	Tool *ToolCall

	// Result is set for TurnEventToolResult.
	Result *ToolResult

	// End is set for TurnEventTurnEnd.
	End *TurnEndInfo

	// Err is set for TurnEventError. (For fatal stream errors, Recv
	// returns the error directly instead of wrapping it in an event.)
	Err error
}

// ToolCall describes a model-requested tool invocation.
type ToolCall struct {
	// ID is the codec-assigned identifier the matching ToolResult
	// must reference (Anthropic's tool_use_id, etc).
	ID string

	// Name is the tool's name as declared in TurnOptions.Tools.
	Name string

	// Input is the JSON-decoded argument blob the model produced.
	Input map[string]any
}

// ToolResult is the value sent back to the model after kilroy runs
// a ToolCall. In ToolControlDriver backends, the same shape is also
// emitted as a TurnEventToolResult so observers can audit the loop.
type ToolResult struct {
	// ToolUseID matches ToolCall.ID.
	ToolUseID string

	// Content is the tool's stringified output. Codecs translate
	// to their wire format (Anthropic content blocks, OpenAI string).
	Content string

	// IsError is true when the tool failed; the model treats this
	// as a recoverable failure to be reported back, not a stream-level
	// error.
	IsError bool
}

// TurnEndInfo carries metadata about how a turn finished. CostUSD is a
// pointer so unset (no cost-tracking capability) is distinguishable
// from $0.00.
type TurnEndInfo struct {
	// StopReason is the codec's normalized end signal. Common values:
	// "end_turn", "max_tokens", "stop_sequence", "tool_use".
	StopReason string

	// InputTokens / OutputTokens are usage counts when reported by
	// the backend. Zero means "not reported."
	InputTokens  int
	OutputTokens int

	// CostUSD is populated when Capabilities.CostTracking is true.
	CostUSD *float64
}

// ErrToolControlDriver is returned from TurnStream.SendToolResult when
// the backend's ToolControl is ToolControlDriver — the driver owns the
// tool loop, so kilroy cannot inject results into it.
var ErrToolControlDriver = errors.New("agentbackend: tool control belongs to the driver; SendToolResult is unavailable")
