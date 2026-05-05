// Package agents provides the unified agent dispatcher and backend adapters.
//
// This file contains the AgentBackend adapters that wrap the existing handler
// implementations (CodergenHandler for SDK/API path, TmuxAgentHandler for CLI
// path) to satisfy the agentbackend.AgentBackend interface.
//
// These adapters are a passive wrapper layer (Block 6 Step 3) — they delegate
// entirely to the existing handlers with no behavior change. Future steps will
// extract the transport layer and implement the full TurnStream event model.
package agents

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// extractProvider safely extracts the provider string from TurnOptions.Extra.
// Returns an error if Extra is nil, the key is missing, or the value is not a string.
func extractProvider(extra map[string]any) (string, error) {
	if extra == nil {
		return "", errors.New("TurnOptions.Extra is nil: provider is required")
	}
	val, ok := extra["provider"]
	if !ok {
		return "", errors.New("TurnOptions.Extra[" + "provider" + "] is missing: provider is required")
	}
	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("TurnOptions.Extra[%q] has type %T, expected string", "provider", val)
	}
	return str, nil
}

// Compile-time assertions: both adapters satisfy AgentBackend interface.
var _ agentbackend.AgentBackend = (*SDKBackend)(nil)
var _ agentbackend.AgentBackend = (*TmuxBackend)(nil)

// agentHandler defines the minimal interface the SDKBackend needs from
// the underlying handler. Both engine.CodergenHandler and test doubles
// satisfy this interface.
type agentHandler interface {
	ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error)
}

// SDKBackend wraps the existing CodergenHandler (SDK/API path) as an
// AgentBackend adapter. It delegates execution to the handler while
// presenting the unified backend interface.
//
// This is Step 3 of Block 6: the adapter is passive, with no behavior
// change. The full TurnStream event model will be implemented in Step 4.
type SDKBackend struct {
	handler agentHandler
}

// NewSDKBackend creates an AgentBackend adapter wrapping the given
// handler. The handler must satisfy the agentHandler interface (which
// *engine.CodergenHandler does). If handler is nil, a zero-value
// CodergenHandler is used.
func NewSDKBackend(handler agentHandler) *SDKBackend {
	if handler == nil {
		handler = &engine.CodergenHandler{}
	}
	return &SDKBackend{handler: handler}
}

// StartTurn begins a single conversation turn. This adapter delegates to
// the underlying CodergenHandler's ExecuteAgent method.
//
// Note: The full TurnStream event model is not yet implemented. This
// adapter returns a minimal stream that yields a single TurnEnd event
// containing the handler's result. Step 4 will extract the transport
// layer and emit proper streaming events.
func (b *SDKBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	// Create a minimal execution context for the handler.
	// The handler expects an engine.Execution with LogsRoot set.
	// For the adapter pattern, we construct a minimal context.
	exec := &engine.Execution{
		LogsRoot: ".", // Minimal default; caller should set via context or options
	}

	// Build a minimal node from the user message.
	node := &model.Node{
		ID: "turn",
	}
	if msg.Text != "" {
		// Store the prompt in the node's attributes for retrieval by the handler.
		// The handler reads node.Prompt() which looks at the "prompt" attribute.
		if node.Attrs == nil {
			node.Attrs = make(map[string]string)
		}
		node.Attrs["prompt"] = msg.Text
	}

	// Build an AgentRoute from TurnOptions.
	provider, err := extractProvider(opts.Extra)
	if err != nil {
		return nil, err
	}
	route := engine.AgentRoute{
		Provider: provider,
		Model:    opts.Model,
		Backend:  engine.BackendAPI,
		Driver:   "anthropic_sdk", // Default; should be overridden via Extra
	}
	if driver, ok := opts.Extra["driver"].(string); ok && driver != "" {
		route.Driver = driver
	}

	// Execute via the handler. This blocks until completion.
	outcome, err := b.handler.ExecuteAgent(ctx, exec, node, route)
	if err != nil {
		return nil, err
	}

	// Create a TurnStream that yields the result as events.
	// For Step 3, we return a minimal implementation that yields:
	// 1. A text event with the response (if any)
	// 2. A turn_end event with status metadata
	return &sdkTurnStream{
		outcome: outcome,
		notes:   outcome.Notes,
	}, nil
}

// ToolControl reports that the SDK backend uses kilroy-side tool control.
// The API path (CodergenHandler) manages tool calls within the agent loop.
func (b *SDKBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

// Capabilities surfaces optional features for the SDK backend.
// These values reflect the current CodergenHandler capabilities.
func (b *SDKBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,
		TokenStreaming: true,
		CostTracking:   false, // Not currently implemented
		ToolInjection:  true,
	}
}

// Close releases backend-held resources. For the SDKBackend, this is a
// no-op because the underlying CodergenHandler is stateless.
func (b *SDKBackend) Close() error {
	return nil
}

// NativeExecuteAgent delegates directly to the underlying handler with
// zero information loss. This is used by Dispatcher.ExecuteAgent to route
// through the adapter without using the TurnStream abstraction.
func (b *SDKBackend) NativeExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	return b.handler.ExecuteAgent(ctx, exec, node, route)
}

// sdkTurnStream is a minimal TurnStream implementation for SDKBackend.
// It yields a text event (if any) followed by a turn_end event.
// This is a placeholder for the full streaming implementation in Step 4.
type sdkTurnStream struct {
	outcome  runtime.Outcome
	notes    string
	sentText bool
	sentEnd  bool
}

// Recv returns the next event in the stream.
// First call returns a text event (if notes not empty); then returns turn_end; subsequent calls return io.EOF.
func (s *sdkTurnStream) Recv() (agentbackend.TurnEvent, error) {
	// Return text event first (if we have notes and haven't sent it yet).
	if !s.sentText && s.notes != "" {
		s.sentText = true
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventText,
			Text: s.notes,
		}, nil
	}

	// Then return turn_end (if we haven't sent it yet).
	if !s.sentEnd {
		s.sentEnd = true
		stopReason := "end_turn"
		if s.outcome.Status == runtime.StatusFail {
			stopReason = "error"
		}
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventTurnEnd,
			End: &agentbackend.TurnEndInfo{
				StopReason: stopReason,
			},
		}, nil
	}

	// All events have been sent.
	return agentbackend.TurnEvent{}, io.EOF
}

// SendToolResult feeds a tool result back into the conversation.
// For SDKBackend, this is a no-op placeholder; the actual tool handling
// is managed internally by the agent loop in CodergenHandler.
func (s *sdkTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	// Tool control is handled internally by the CodergenHandler's agent loop.
	// Step 4 will implement proper tool result injection.
	return nil
}

// Close releases the stream resources.
func (s *sdkTurnStream) Close() error {
	return nil
}

// tmuxAgentHandler defines the minimal interface the TmuxBackend needs from
// the underlying handler. Both *TmuxAgentHandler and test doubles satisfy
// this interface.
type tmuxAgentHandler interface {
	ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error)
}

// TmuxBackend wraps the existing TmuxAgentHandler (CLI path) as an
// AgentBackend adapter. It delegates execution to the handler while
// presenting the unified backend interface.
//
// This is Step 3 of Block 6: the adapter is passive, with no behavior
// change. The full TurnStream event model will be implemented in Step 4.
type TmuxBackend struct {
	handler tmuxAgentHandler
}

// NewTmuxBackend creates an AgentBackend adapter wrapping the given
// handler. The handler must satisfy the tmuxAgentHandler interface
// (which *TmuxAgentHandler does). If handler is nil, a default
// TmuxAgentHandler is created.
func NewTmuxBackend(handler tmuxAgentHandler) *TmuxBackend {
	if handler == nil {
		handler = NewTmuxAgentHandler()
	}
	return &TmuxBackend{handler: handler}
}

// StartTurn begins a single conversation turn. This adapter delegates to
// the underlying TmuxAgentHandler's ExecuteAgent method.
//
// Note: The full TurnStream event model is not yet implemented. This
// adapter returns a minimal stream that yields a single TurnEnd event
// containing the handler's result. Step 4 will extract the transport
// layer and emit proper streaming events.
func (b *TmuxBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	// Create a minimal execution context for the handler.
	exec := &engine.Execution{
		LogsRoot: ".", // Minimal default; caller should set via context or options
	}

	// Build a minimal node from the user message.
	node := &model.Node{
		ID: "turn",
	}
	if msg.Text != "" {
		if node.Attrs == nil {
			node.Attrs = make(map[string]string)
		}
		node.Attrs["prompt"] = msg.Text
	}

	// Build an AgentRoute from TurnOptions.
	provider, err := extractProvider(opts.Extra)
	if err != nil {
		return nil, err
	}
	route := engine.AgentRoute{
		Provider: provider,
		Model:    opts.Model,
		Backend:  engine.BackendCLI,
		Driver:   "claude_cli", // Default; should be overridden via Extra
	}
	if driver, ok := opts.Extra["driver"].(string); ok && driver != "" {
		route.Driver = driver
	}

	// Execute via the handler. This blocks until completion.
	outcome, err := b.handler.ExecuteAgent(ctx, exec, node, route)
	if err != nil {
		return nil, err
	}

	// Create a TurnStream that yields the result as events.
	return &tmuxTurnStream{
		outcome: outcome,
		notes:   outcome.Notes,
	}, nil
}

// ToolControl reports that the tmux backend uses driver-side tool control.
// The CLI path (TmuxAgentHandler) runs driver binaries that own their own
// tool dispatch loop internally.
func (b *TmuxBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlDriver
}

// Capabilities surfaces optional features for the tmux backend.
// These values reflect the current TmuxAgentHandler capabilities.
func (b *TmuxBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,  // Claude CLI supports thinking
		TokenStreaming: false, // Streaming not yet implemented for CLI path
		CostTracking:   false, // Not currently implemented
		ToolInjection:  false, // Driver owns tool loop; kilroy cannot inject
	}
}

// Close releases backend-held resources. For the TmuxBackend, this
// delegates to the underlying handler's cleanup if any.
func (b *TmuxBackend) Close() error {
	// The TmuxAgentHandler doesn't have a Close method currently,
	// but we may add resource cleanup in the future.
	return nil
}

// NativeExecuteAgent delegates directly to the underlying handler with
// zero information loss. This is used by Dispatcher.ExecuteAgent to route
// through the adapter without using the TurnStream abstraction.
func (b *TmuxBackend) NativeExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	return b.handler.ExecuteAgent(ctx, exec, node, route)
}

// tmuxTurnStream is a minimal TurnStream implementation for TmuxBackend.
// It yields a text event (if any) followed by a turn_end event.
// This is a placeholder for the full streaming implementation in Step 4.
type tmuxTurnStream struct {
	outcome  runtime.Outcome
	notes    string
	sentText bool
	sentEnd  bool
}

// Recv returns the next event in the stream.
// First call returns a text event (if notes not empty); then returns turn_end; subsequent calls return io.EOF.
func (s *tmuxTurnStream) Recv() (agentbackend.TurnEvent, error) {
	// Return text event first (if we have notes and haven't sent it yet).
	if !s.sentText && s.notes != "" {
		s.sentText = true
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventText,
			Text: s.notes,
		}, nil
	}

	// Then return turn_end (if we haven't sent it yet).
	if !s.sentEnd {
		s.sentEnd = true
		stopReason := "end_turn"
		if s.outcome.Status == runtime.StatusFail {
			stopReason = "error"
		}
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventTurnEnd,
			End: &agentbackend.TurnEndInfo{
				StopReason: stopReason,
			},
		}, nil
	}

	// All events have been sent.
	return agentbackend.TurnEvent{}, io.EOF
}

// SendToolResult feeds a tool result back into the conversation.
// For TmuxBackend, this returns ErrToolControlDriver because the driver
// owns the tool loop; kilroy cannot inject tool results.
func (s *tmuxTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	return agentbackend.ErrToolControlDriver
}

// Close releases the stream resources.
func (s *tmuxTurnStream) Close() error {
	return nil
}
