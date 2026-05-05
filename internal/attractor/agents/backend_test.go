// Tests for the AgentBackend adapter layer (SDKBackend + TmuxBackend).
// These are sanity tests verifying that the adapters delegate to the
// underlying handlers without behavior change.
package agents

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// TestAgentBackend_SDKAdapter_DelegatesToHandler verifies that SDKBackend
// wraps a CodergenHandler and delegates execution to it.
func TestAgentBackend_SDKAdapter_DelegatesToHandler(t *testing.T) {
	// Create a test handler that we can observe.
	testHandler := &testCodergenHandler{
		outcome: runtime.Outcome{
			Status: runtime.StatusSuccess,
			Notes:  "test response from SDK handler",
		},
	}

	// Wrap it in the adapter.
	backend := NewSDKBackend(testHandler)

	// Verify the backend satisfies the interface compile-time assertion.
	var _ agentbackend.AgentBackend = backend

	// Test ToolControl returns Kilroy mode.
	if got := backend.ToolControl(); got != agentbackend.ToolControlKilroy {
		t.Errorf("ToolControl() = %v, want ToolControlKilroy", got)
	}

	// Test Capabilities returns expected values.
	caps := backend.Capabilities()
	if !caps.Thinking {
		t.Error("Capabilities().Thinking = false, want true")
	}
	if !caps.TokenStreaming {
		t.Error("Capabilities().TokenStreaming = false, want true")
	}
	if caps.CostTracking {
		t.Error("Capabilities().CostTracking = true, want false")
	}
	if !caps.ToolInjection {
		t.Error("Capabilities().ToolInjection = false, want true")
	}

	// Test StartTurn delegates to the handler.
	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test prompt"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": "anthropic",
			"driver":   "anthropic_sdk",
		},
	}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	defer stream.Close()

	// Verify the handler was called with the expected parameters.
	if !testHandler.called {
		t.Error("handler.ExecuteAgent was not called")
	}
	if testHandler.node == nil {
		t.Fatal("handler.ExecuteAgent node is nil")
	}
	if got := testHandler.node.Attr("prompt", ""); got != "test prompt" {
		t.Errorf("handler.ExecuteAgent node prompt = %q, want %q", got, "test prompt")
	}
	if testHandler.route.Model != "test-model" {
		t.Errorf("handler.ExecuteAgent route.Model = %q, want %q", testHandler.route.Model, "test-model")
	}
	if testHandler.route.Driver != "anthropic_sdk" {
		t.Errorf("handler.ExecuteAgent route.Driver = %q, want %q", testHandler.route.Driver, "anthropic_sdk")
	}

	// Read events from the stream.
	events := []agentbackend.TurnEvent{}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv() error = %v", err)
		}
		events = append(events, ev)
	}

	// Should get at least a turn_end event.
	if len(events) == 0 {
		t.Fatal("expected at least one event from stream")
	}

	// Last event should be turn_end.
	last := events[len(events)-1]
	if last.Type != agentbackend.TurnEventTurnEnd {
		t.Errorf("last event.Type = %v, want TurnEventTurnEnd", last.Type)
	}

	// Test Close is no-op and returns nil.
	if err := backend.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

// TestAgentBackend_TmuxAdapter_DelegatesToHandler verifies that TmuxBackend
// wraps a TmuxAgentHandler and delegates execution to it.
func TestAgentBackend_TmuxAdapter_DelegatesToHandler(t *testing.T) {
	// Create a test handler that we can observe.
	testHandler := &testTmuxAgentHandler{
		outcome: runtime.Outcome{
			Status: runtime.StatusSuccess,
			Notes:  "test response from tmux handler",
		},
	}

	// Wrap it in the adapter.
	backend := NewTmuxBackend(testHandler)

	// Verify the backend satisfies the interface compile-time assertion.
	var _ agentbackend.AgentBackend = backend

	// Test ToolControl returns Driver mode.
	if got := backend.ToolControl(); got != agentbackend.ToolControlDriver {
		t.Errorf("ToolControl() = %v, want ToolControlDriver", got)
	}

	// Test Capabilities returns expected values.
	caps := backend.Capabilities()
	if !caps.Thinking {
		t.Error("Capabilities().Thinking = false, want true")
	}
	if caps.TokenStreaming {
		t.Error("Capabilities().TokenStreaming = true, want false")
	}
	if caps.CostTracking {
		t.Error("Capabilities().CostTracking = true, want false")
	}
	if caps.ToolInjection {
		t.Error("Capabilities().ToolInjection = true, want false")
	}

	// Test StartTurn delegates to the handler.
	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test prompt"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": "anthropic",
			"driver":   "claude_cli",
		},
	}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	defer stream.Close()

	// Verify the handler was called with the expected parameters.
	if !testHandler.called {
		t.Error("handler.ExecuteAgent was not called")
	}
	if testHandler.node == nil {
		t.Fatal("handler.ExecuteAgent node is nil")
	}
	if got := testHandler.node.Attr("prompt", ""); got != "test prompt" {
		t.Errorf("handler.ExecuteAgent node prompt = %q, want %q", got, "test prompt")
	}
	if testHandler.route.Model != "test-model" {
		t.Errorf("handler.ExecuteAgent route.Model = %q, want %q", testHandler.route.Model, "test-model")
	}
	if testHandler.route.Driver != "claude_cli" {
		t.Errorf("handler.ExecuteAgent route.Driver = %q, want %q", testHandler.route.Driver, "claude_cli")
	}

	// Read events from the stream.
	events := []agentbackend.TurnEvent{}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv() error = %v", err)
		}
		events = append(events, ev)
	}

	// Should get at least a turn_end event.
	if len(events) == 0 {
		t.Fatal("expected at least one event from stream")
	}

	// Test SendToolResult returns ErrToolControlDriver.
	toolResult := agentbackend.ToolResult{
		ToolUseID: "test-tool-id",
		Content:   "test result",
	}
	if err := stream.SendToolResult(ctx, toolResult); !errors.Is(err, agentbackend.ErrToolControlDriver) {
		t.Errorf("SendToolResult() error = %v, want ErrToolControlDriver", err)
	}

	// Test Close is no-op and returns nil.
	if err := backend.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

// TestAgentBackend_SDKAdapter_WithNilHandler verifies that NewSDKBackend
// creates a default handler when nil is passed.
func TestAgentBackend_SDKAdapter_WithNilHandler(t *testing.T) {
	// Create backend with nil handler.
	backend := NewSDKBackend(nil)

	if backend == nil {
		t.Fatal("NewSDKBackend(nil) returned nil")
	}
	if backend.handler == nil {
		t.Error("backend.handler is nil, expected default handler")
	}
}

// TestAgentBackend_TmuxAdapter_WithNilHandler verifies that NewTmuxBackend
// creates a default handler when nil is passed.
func TestAgentBackend_TmuxAdapter_WithNilHandler(t *testing.T) {
	// Create backend with nil handler.
	backend := NewTmuxBackend(nil)

	if backend == nil {
		t.Fatal("NewTmuxBackend(nil) returned nil")
	}
	if backend.handler == nil {
		t.Error("backend.handler is nil, expected default handler")
	}
}

// TestAgentBackend_SDKAdapter_HandlerError verifies error propagation.
func TestAgentBackend_SDKAdapter_HandlerError(t *testing.T) {
	testHandler := &testCodergenHandler{
		err: errors.New("handler error"),
	}
	backend := NewSDKBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": "anthropic",
			"driver":   "anthropic_sdk",
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with handler error = nil, want error")
	}
	if !errors.Is(err, testHandler.err) && err.Error() != testHandler.err.Error() {
		t.Errorf("StartTurn() error = %v, want %v", err, testHandler.err)
	}
}

// TestAgentBackend_TmuxAdapter_HandlerError verifies error propagation.
func TestAgentBackend_TmuxAdapter_HandlerError(t *testing.T) {
	testHandler := &testTmuxAgentHandler{
		err: errors.New("handler error"),
	}
	backend := NewTmuxBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": "anthropic",
			"driver":   "claude_cli",
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with handler error = nil, want error")
	}
}

// TestAgentBackend_SDKAdapter_NilExtra verifies that SDKBackend returns an error
// when opts.Extra is nil (regression test for panic).
func TestAgentBackend_SDKAdapter_NilExtra(t *testing.T) {
	testHandler := &testCodergenHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewSDKBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		// Extra is nil - should return error, not panic
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with nil Extra = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when Extra is nil")
	}
}

// TestAgentBackend_TmuxAdapter_NilExtra verifies that TmuxBackend returns an error
// when opts.Extra is nil (regression test for panic).
func TestAgentBackend_TmuxAdapter_NilExtra(t *testing.T) {
	testHandler := &testTmuxAgentHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewTmuxBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		// Extra is nil - should return error, not panic
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with nil Extra = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when Extra is nil")
	}
}

// TestAgentBackend_SDKAdapter_MissingProvider verifies that SDKBackend returns an error
// when opts.Extra["provider"] is missing (regression test for panic).
func TestAgentBackend_SDKAdapter_MissingProvider(t *testing.T) {
	testHandler := &testCodergenHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewSDKBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"driver": "anthropic_sdk",
			// "provider" is missing - should return error, not panic
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with missing provider = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when provider is missing")
	}
}

// TestAgentBackend_TmuxAdapter_MissingProvider verifies that TmuxBackend returns an error
// when opts.Extra["provider"] is missing (regression test for panic).
func TestAgentBackend_TmuxAdapter_MissingProvider(t *testing.T) {
	testHandler := &testTmuxAgentHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewTmuxBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"driver": "claude_cli",
			// "provider" is missing - should return error, not panic
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with missing provider = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when provider is missing")
	}
}

// TestAgentBackend_SDKAdapter_WrongTypeProvider verifies that SDKBackend returns an error
// when opts.Extra["provider"] is not a string (regression test for panic).
func TestAgentBackend_SDKAdapter_WrongTypeProvider(t *testing.T) {
	testHandler := &testCodergenHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewSDKBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": 123, // Wrong type: int instead of string
			"driver":   "anthropic_sdk",
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with wrong type provider = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when provider has wrong type")
	}
}

// TestAgentBackend_TmuxAdapter_WrongTypeProvider verifies that TmuxBackend returns an error
// when opts.Extra["provider"] is not a string (regression test for panic).
func TestAgentBackend_TmuxAdapter_WrongTypeProvider(t *testing.T) {
	testHandler := &testTmuxAgentHandler{
		outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "test"},
	}
	backend := NewTmuxBackend(testHandler)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "test"}
	opts := agentbackend.TurnOptions{
		Model: "test-model",
		Extra: map[string]any{
			"provider": 123, // Wrong type: int instead of string
			"driver":   "claude_cli",
		},
	}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Error("StartTurn() with wrong type provider = nil, want error")
	}
	if testHandler.called {
		t.Error("handler.ExecuteAgent was called when provider has wrong type")
	}
}

// testCodergenHandler is a test double for engine.CodergenHandler.
type testCodergenHandler struct {
	outcome runtime.Outcome
	err     error
	called  bool
	ctx     context.Context
	exec    *engine.Execution
	node    *model.Node
	route   engine.AgentRoute
}

func (h *testCodergenHandler) UsesFidelity() bool     { return true }
func (h *testCodergenHandler) RequiresProvider() bool { return true }

func (h *testCodergenHandler) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	h.called = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	return h.outcome, h.err
}

func (h *testCodergenHandler) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	h.called = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	h.route = route
	return h.outcome, h.err
}

// testTmuxAgentHandler is a test double for TmuxAgentHandler.
type testTmuxAgentHandler struct {
	outcome runtime.Outcome
	err     error
	called  bool
	ctx     context.Context
	exec    *engine.Execution
	node    *model.Node
	route   engine.AgentRoute
}

func (h *testTmuxAgentHandler) UsesFidelity() bool     { return true }
func (h *testTmuxAgentHandler) RequiresProvider() bool { return true }

func (h *testTmuxAgentHandler) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	h.called = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	return h.outcome, h.err
}

func (h *testTmuxAgentHandler) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	h.called = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	h.route = route
	return h.outcome, h.err
}
