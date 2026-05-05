// Package loop provides the ToolControlKilroy orchestration loop.
//
// The RunTurn function drives a single agent turn from start to terminal
// status, handling tool-call routing, iteration limits, and cancellation.
// This is a reusable primitive for agentic execution on top of the
// AgentBackend abstraction.
package loop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/auth"
)

// TurnEventType represents lifecycle events emitted by the orchestration loop.
type TurnEventType string

const (
	// TurnEventTurnStart is emitted when a turn begins.
	TurnEventTurnStart TurnEventType = "turn_start"
	// TurnEventToolCall is emitted when a tool call is dispatched.
	TurnEventToolCall TurnEventType = "tool_call"
	// TurnEventToolResult is emitted when a tool result is received.
	TurnEventToolResult TurnEventType = "tool_result"
	// TurnEventTurnEnd is emitted when a turn completes successfully.
	TurnEventTurnEnd TurnEventType = "turn_end"
	// TurnEventTurnError is emitted when a turn fails.
	TurnEventTurnError TurnEventType = "turn_error"
)

// LoopEvent represents a lifecycle event emitted during turn execution.
type LoopEvent struct {
	// Type is the kind of event (turn_start, tool_call, tool_result, turn_end, turn_error).
	Type TurnEventType

	// Timestamp is when the event occurred.
	Timestamp time.Time

	// ToolCall is set for tool_call events.
	ToolCall *agentbackend.ToolCall

	// ToolResult is set for tool_result events.
	ToolResult *agentbackend.ToolResult

	// Error is set for turn_error events.
	Error error
}

// ToolHandler dispatches a tool call and returns the result.
// Implementations execute the named tool with the provided input.
type ToolHandler interface {
	// HandleTool executes a tool call and returns its result.
	// The ctx carries the turn's cancellation and deadline.
	HandleTool(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error)
}

// ToolHandlerFunc is an adapter to allow ordinary functions as ToolHandlers.
type ToolHandlerFunc func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error)

// HandleTool implements ToolHandler.
func (f ToolHandlerFunc) HandleTool(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
	return f(ctx, call)
}

// TurnConfig configures the orchestration loop for a single turn.
type TurnConfig struct {
	// MaxIterations caps the number of agent→tool→agent round trips.
	// Zero means unlimited (not recommended for production).
	MaxIterations int

	// StallTimeout is the maximum idle time between events before the
	// turn is considered stalled and cancelled. Zero disables stall detection.
	StallTimeout time.Duration

	// AuthResolver provides credential resolution for the turn.
	// Optional; when nil, the loop assumes credentials are pre-configured
	// on the backend.
	AuthResolver auth.AuthResolver

	// EventSink receives lifecycle events during turn execution.
	// Optional; when nil, no events are emitted.
	// Events are emitted at: turn start, tool call dispatch, tool result
	// received, turn end (success), and turn error.
	EventSink func(LoopEvent)
}

// TurnResult captures the outcome of a completed turn.
type TurnResult struct {
	// Text is the accumulated natural-language response from the agent.
	Text string

	// Thinking is the accumulated extended-thinking content (if any).
	Thinking string

	// ToolCalls is the list of all tool calls requested by the agent.
	ToolCalls []agentbackend.ToolCall

	// StopReason indicates how the turn ended.
	// Common values: "end_turn", "max_tokens", "stop_sequence", "tool_use",
	// "max_iterations", "context_cancelled", "stall_timeout", "error".
	StopReason string

	// Error is non-nil if the turn failed (not just tool errors).
	Error error

	// Usage tracks token consumption if reported by the backend.
	Usage TokenUsage
}

// TokenUsage tracks token consumption for a turn.
type TokenUsage struct {
	InputTokens  int
	OutputTokens int
}

// emitEvent sends an event to the configured EventSink if one is set.
func emitEvent(sink func(LoopEvent), evt LoopEvent) {
	if sink != nil {
		sink(evt)
	}
}

// RunTurn drives a single agent turn from start to completion.
//
// The function:
//  1. Starts a turn via backend.StartTurn
//  2. Receives events from the stream (text, thinking, tool_use, turn_end, error)
//  3. For ToolControlKilroy backends: dispatches tool calls via handler,
//     injects results back via SendToolResult
//  4. Honors MaxIterations, context cancellation, and stall timeout
//  5. Returns when the turn reaches a terminal state
//
// The loop is designed to be backend-agnostic. It branches on
// backend.ToolControl() to decide whether to run the tool dispatch loop
// (ToolControlKilroy) or just observe events (ToolControlDriver).
func RunTurn(ctx context.Context, backend agentbackend.AgentBackend, msg agentbackend.UserMessage, opts agentbackend.TurnOptions, handler ToolHandler, cfg TurnConfig) (TurnResult, error) {
	result := TurnResult{
		ToolCalls: make([]agentbackend.ToolCall, 0),
	}

	// Emit turn start event.
	emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnStart, Timestamp: time.Now()})

	// Apply credential resolution if an auth resolver is configured.
	// This is a hook for Step 5 auth integration.
	if cfg.AuthResolver != nil {
		if err := resolveAndApplyAuth(ctx, cfg.AuthResolver, opts); err != nil {
			result.StopReason = "error"
			result.Error = fmt.Errorf("auth resolution failed: %w", err)
			emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
			return result, result.Error
		}
	}

	// Start the turn.
	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		result.StopReason = "error"
		result.Error = fmt.Errorf("start turn failed: %w", err)
		emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
		return result, result.Error
	}
	defer stream.Close()

	// Determine if we own the tool loop or just observe.
	ownsToolLoop := backend.ToolControl() == agentbackend.ToolControlKilroy

	// Run the event loop with a goroutine for receiving events.
	// This allows the main loop to check context cancellation and stall timeout
	// even when Recv() would block.
	iterations := 0
	stallTimer := newStallTimer(cfg.StallTimeout)

	// Create a channel for events from the receive goroutine.
	eventCh := make(chan recvResult)

	// Start the receive goroutine.
	go func() {
		defer close(eventCh)
		for {
			event, err := stream.Recv()
			select {
			case eventCh <- recvResult{event: event, err: err}:
				if err != nil {
					// EOF or error - the main loop will handle it.
					return
				}
			case <-ctx.Done():
				// Context cancelled while we were blocked in Recv.
				// Return to allow cleanup.
				return
			}
		}
	}()

	for {
		// Check iteration limit before waiting for events.
		if cfg.MaxIterations > 0 && iterations >= cfg.MaxIterations {
			result.StopReason = "max_iterations"
			result.Error = fmt.Errorf("turn exceeded max iterations (%d)", cfg.MaxIterations)
			emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
			return result, result.Error
		}

		// Compute stall deadline if configured.
		var stallCh <-chan time.Time
		if cfg.StallTimeout > 0 {
			stallCh = time.After(cfg.StallTimeout)
		}

		// Wait for an event, context cancellation, or stall timeout.
		select {
		case <-ctx.Done():
			result.StopReason = "context_cancelled"
			result.Error = context.Cause(ctx)
			if result.Error == nil {
				result.Error = ctx.Err()
			}
			emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
			return result, result.Error

		case <-stallCh:
			result.StopReason = "stall_timeout"
			result.Error = fmt.Errorf("stall timeout after %s", cfg.StallTimeout)
			emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
			return result, result.Error

		case recv, ok := <-eventCh:
			if !ok {
				// Channel closed - receive goroutine exited.
				if result.StopReason == "" {
					result.StopReason = "end_turn"
				}
				emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnEnd, Timestamp: time.Now()})
				return result, nil
			}

			event, err := recv.event, recv.err

			if err != nil {
				if errors.Is(err, io.EOF) {
					// Normal end of stream.
					if result.StopReason == "" {
						result.StopReason = "end_turn"
					}
					emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnEnd, Timestamp: time.Now()})
					return result, nil
				}
				result.StopReason = "error"
				result.Error = fmt.Errorf("stream recv failed: %w", err)
				emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
				return result, result.Error
			}

			// Reset stall timer on any event.
			stallTimer.reset()

			// Process the event.
			switch event.Type {
			case agentbackend.TurnEventText:
				result.Text += event.Text

			case agentbackend.TurnEventThinking:
				result.Thinking += event.Text

			case agentbackend.TurnEventToolUse:
				if event.Tool == nil {
					continue
				}
				result.ToolCalls = append(result.ToolCalls, *event.Tool)

				// Emit tool_call event for both Kilroy and Driver control modes.
				emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventToolCall, Timestamp: time.Now(), ToolCall: event.Tool})

				if ownsToolLoop {
					if handler == nil {
						result.StopReason = "error"
						result.Error = fmt.Errorf("tool handler is nil for ToolControlKilroy mode")
						emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
						return result, result.Error
					}
					// Dispatch the tool and send result back.
					toolResult, toolErr := handler.HandleTool(ctx, *event.Tool)
					if toolErr != nil {
						// Tool errors are recoverable; report back to agent.
						toolResult = agentbackend.ToolResult{
							ToolUseID: event.Tool.ID,
							Content:   toolErr.Error(),
							IsError:   true,
						}
					}

					// Emit tool_result event after receiving result.
					emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventToolResult, Timestamp: time.Now(), ToolResult: &toolResult})

					if err := stream.SendToolResult(ctx, toolResult); err != nil {
						result.StopReason = "error"
						result.Error = fmt.Errorf("send tool result failed: %w", err)
						emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventTurnError, Timestamp: time.Now(), Error: result.Error})
						return result, result.Error
					}
					iterations++
				}

			case agentbackend.TurnEventTurnEnd:
				if event.End != nil {
					result.StopReason = event.End.StopReason
					result.Usage.InputTokens = event.End.InputTokens
					result.Usage.OutputTokens = event.End.OutputTokens
				}
				// Turn is complete; next Recv will return EOF and emit turn_end.

			case agentbackend.TurnEventError:
				// Non-fatal error event; record but continue.
				if event.Err != nil {
					// Log but don't terminate; the backend may recover.
					// We'll accumulate the error if the turn ultimately fails.
					if result.Error == nil {
						result.Error = event.Err
					}
				}

			case agentbackend.TurnEventToolResult:
				// Only emitted by ToolControlDriver backends for observability.
				// We don't need to act on it since we don't control the loop,
				// but we forward it to the EventSink for symmetric visibility.
				if event.Result != nil {
					emitEvent(cfg.EventSink, LoopEvent{Type: TurnEventToolResult, Timestamp: time.Now(), ToolResult: event.Result})
				}
			}
		}
	}
}

// recvResult captures the result of a Recv() call for channel communication.
type recvResult struct {
	event agentbackend.TurnEvent
	err   error
}

// stallTimer tracks idle time for stall detection.
type stallTimer struct {
	timeout   time.Duration
	lastReset time.Time
}

func newStallTimer(timeout time.Duration) *stallTimer {
	return &stallTimer{
		timeout:   timeout,
		lastReset: time.Now(),
	}
}

func (t *stallTimer) reset() {
	t.lastReset = time.Now()
}

func (t *stallTimer) timedOut() bool {
	if t.timeout <= 0 {
		return false
	}
	return time.Since(t.lastReset) > t.timeout
}

// resolveAndApplyAuth resolves credentials via the auth resolver and
// applies them to the turn options. This is a placeholder for Step 5
// integration; currently it validates that auth resolution works but
// doesn't modify opts (the backend is expected to use the resolver
// directly or have pre-configured credentials).
func resolveAndApplyAuth(ctx context.Context, resolver auth.AuthResolver, opts agentbackend.TurnOptions) error {
	// Build a route from options for auth resolution.
	// The route identifies the provider/driver being used.
	route := auth.AgentRoute{
		Provider: extractProvider(opts.Extra),
		Driver:   extractDriver(opts.Extra),
	}

	// Attempt to resolve credentials. This validates that the auth chain
	// is properly configured, even if we don't use the result directly.
	// Future iterations will inject resolved credentials into the backend.
	_, err := resolver.ResolveCredential(ctx, route)
	if err != nil {
		// Auth resolution failure is decisive to preserve the freeze invariant.
		return fmt.Errorf("auth resolution failed: %w", err)
	}
	return nil
}

func extractProvider(extra map[string]any) string {
	if extra == nil {
		return ""
	}
	if v, ok := extra["provider"].(string); ok {
		return v
	}
	return ""
}

func extractDriver(extra map[string]any) string {
	if extra == nil {
		return ""
	}
	if v, ok := extra["driver"].(string); ok {
		return v
	}
	return ""
}
