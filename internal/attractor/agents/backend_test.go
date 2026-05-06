// Tests for the AgentBackend adapter layer (SDKBackend + TmuxBackend).
// These lock in the post-alpha contract: StartTurn is the load-bearing
// execution path and must not call the legacy ExecuteAgent backdoor.
package agents

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestAgentBackend_SDKAdapter_StartTurnUsesRunner(t *testing.T) {
	runner := &testEngineAgentBackend{
		response: "test response from SDK runner",
		outcome: &runtime.Outcome{
			Status: runtime.StatusSuccess,
			Notes:  "runner fired",
		},
	}
	backend := NewSDKBackend(runner)

	var _ agentbackend.AgentBackend = backend

	if got := backend.ToolControl(); got != agentbackend.ToolControlKilroy {
		t.Errorf("ToolControl() = %v, want ToolControlKilroy", got)
	}
	caps := backend.Capabilities()
	if !caps.Thinking || !caps.TokenStreaming || caps.CostTracking || !caps.ToolInjection {
		t.Fatalf("unexpected SDK capabilities: %+v", caps)
	}

	route := engine.AgentRoute{
		Provider: "anthropic",
		Model:    "test-model",
		Backend:  engine.BackendAPI,
		Driver:   "anthropic_sdk",
	}
	stream, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test prompt"}, agentbackend.TurnOptions{
		Model: "test-model",
		Extra: testBackendExtra(t, route),
	})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	defer stream.Close()

	if !runner.called {
		t.Fatal("runner.Run was not called")
	}
	if runner.prompt != "test prompt" {
		t.Errorf("runner prompt = %q, want %q", runner.prompt, "test prompt")
	}
	if runner.route.Driver != "anthropic_sdk" {
		t.Errorf("runner route.Driver = %q, want anthropic_sdk", runner.route.Driver)
	}

	events := collectTurnEvents(t, stream)
	if len(events) < 2 {
		t.Fatalf("expected text and turn_end events, got %+v", events)
	}
	if events[0].Type != agentbackend.TurnEventText || events[0].Text != "test response from SDK runner" {
		t.Fatalf("first event = %+v, want SDK response text", events[0])
	}
	if last := events[len(events)-1]; last.Type != agentbackend.TurnEventTurnEnd {
		t.Fatalf("last event = %+v, want turn_end", last)
	}
	if got := backend.LastOutcome(); got == nil || got.Notes != "runner fired" {
		t.Fatalf("LastOutcome() = %+v, want runner outcome", got)
	}
}

func TestAgentBackend_TmuxAdapter_StartTurnUsesSessionPath(t *testing.T) {
	handler := &testTmuxAgentHandler{
		outcome: runtime.Outcome{
			Status: runtime.StatusSuccess,
			Notes:  "test response from tmux handler",
		},
	}
	backend := NewTmuxBackend(handler)

	var _ agentbackend.AgentBackend = backend

	if got := backend.ToolControl(); got != agentbackend.ToolControlDriver {
		t.Errorf("ToolControl() = %v, want ToolControlDriver", got)
	}
	caps := backend.Capabilities()
	if !caps.Thinking || caps.TokenStreaming || caps.CostTracking || caps.ToolInjection {
		t.Fatalf("unexpected tmux capabilities: %+v", caps)
	}

	route := engine.AgentRoute{
		Provider: "anthropic",
		Model:    "test-model",
		Backend:  engine.BackendCLI,
		Driver:   "claude_cli",
	}
	stream, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test prompt"}, agentbackend.TurnOptions{
		Model: "test-model",
		Extra: testBackendExtra(t, route),
	})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	defer stream.Close()

	if handler.called {
		t.Fatal("legacy ExecuteAgent must not be called")
	}
	if !handler.sessionCalled {
		t.Fatal("ExecuteAgentWithSession was not called")
	}
	if handler.prompt != "test prompt" {
		t.Errorf("ExecuteAgentWithSession prompt = %q, want %q", handler.prompt, "test prompt")
	}
	if handler.route.Driver != "claude_cli" {
		t.Errorf("ExecuteAgentWithSession route.Driver = %q, want claude_cli", handler.route.Driver)
	}

	events := collectTurnEvents(t, stream)
	if len(events) == 0 {
		t.Fatal("expected at least one event from stream")
	}

	toolResult := agentbackend.ToolResult{ToolUseID: "test-tool-id", Content: "test result"}
	if err := stream.SendToolResult(context.Background(), toolResult); !errors.Is(err, agentbackend.ErrToolControlDriver) {
		t.Errorf("SendToolResult() error = %v, want ErrToolControlDriver", err)
	}
	if got := backend.LastOutcome(); got == nil || got.Notes != "test response from tmux handler" {
		t.Fatalf("LastOutcome() = %+v, want tmux outcome", got)
	}
}

func TestAgentBackend_SDKAdapter_WithNilRunner(t *testing.T) {
	backend := NewSDKBackend(nil)
	if backend == nil {
		t.Fatal("NewSDKBackend(nil) returned nil")
	}
	if backend.runner == nil {
		t.Error("backend.runner is nil, expected default runner")
	}
}

func TestAgentBackend_TmuxAdapter_WithNilHandler(t *testing.T) {
	backend := NewTmuxBackend(nil)
	if backend == nil {
		t.Fatal("NewTmuxBackend(nil) returned nil")
	}
	if backend.handler == nil {
		t.Error("backend.handler is nil, expected default handler")
	}
}

func TestAgentBackend_SDKAdapter_RunnerError(t *testing.T) {
	runner := &testEngineAgentBackend{err: errors.New("runner error")}
	backend := NewSDKBackend(runner)
	route := engine.AgentRoute{
		Provider: "anthropic",
		Model:    "test-model",
		Backend:  engine.BackendAPI,
		Driver:   "anthropic_sdk",
	}

	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{
		Model: "test-model",
		Extra: testBackendExtra(t, route),
	})
	if err == nil {
		t.Fatal("StartTurn() with runner error = nil, want error")
	}
	if !errors.Is(err, runner.err) {
		t.Errorf("StartTurn() error = %v, want %v", err, runner.err)
	}
}

func TestAgentBackend_TmuxAdapter_HandlerError(t *testing.T) {
	handler := &testTmuxAgentHandler{err: errors.New("handler error")}
	backend := NewTmuxBackend(handler)
	route := engine.AgentRoute{
		Provider: "anthropic",
		Model:    "test-model",
		Backend:  engine.BackendCLI,
		Driver:   "claude_cli",
	}

	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{
		Model: "test-model",
		Extra: testBackendExtra(t, route),
	})
	if err == nil {
		t.Fatal("StartTurn() with handler error = nil, want error")
	}
	if handler.called {
		t.Fatal("legacy ExecuteAgent must not be called on handler error")
	}
}

func TestAgentBackend_SDKAdapter_NilExtra(t *testing.T) {
	runner := &testEngineAgentBackend{}
	backend := NewSDKBackend(runner)
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model"})
	if err == nil {
		t.Error("StartTurn() with nil Extra = nil, want error")
	}
	if runner.called {
		t.Error("runner.Run was called when Extra is nil")
	}
}

func TestAgentBackend_TmuxAdapter_NilExtra(t *testing.T) {
	handler := &testTmuxAgentHandler{}
	backend := NewTmuxBackend(handler)
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model"})
	if err == nil {
		t.Error("StartTurn() with nil Extra = nil, want error")
	}
	if handler.called || handler.sessionCalled {
		t.Error("tmux handler was called when Extra is nil")
	}
}

func TestAgentBackend_SDKAdapter_MissingProvider(t *testing.T) {
	runner := &testEngineAgentBackend{}
	backend := NewSDKBackend(runner)
	extra := testBackendExtra(t, engine.AgentRoute{Model: "test-model", Backend: engine.BackendAPI, Driver: "anthropic_sdk"})
	delete(extra, "provider")
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model", Extra: extra})
	if err == nil {
		t.Error("StartTurn() with missing provider = nil, want error")
	}
	if runner.called {
		t.Error("runner.Run was called when provider is missing")
	}
}

func TestAgentBackend_TmuxAdapter_MissingProvider(t *testing.T) {
	handler := &testTmuxAgentHandler{}
	backend := NewTmuxBackend(handler)
	extra := testBackendExtra(t, engine.AgentRoute{Model: "test-model", Backend: engine.BackendCLI, Driver: "claude_cli"})
	delete(extra, "provider")
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model", Extra: extra})
	if err == nil {
		t.Error("StartTurn() with missing provider = nil, want error")
	}
	if handler.called || handler.sessionCalled {
		t.Error("tmux handler was called when provider is missing")
	}
}

func TestAgentBackend_SDKAdapter_WrongTypeProvider(t *testing.T) {
	runner := &testEngineAgentBackend{}
	backend := NewSDKBackend(runner)
	extra := testBackendExtra(t, engine.AgentRoute{Provider: "anthropic", Model: "test-model", Backend: engine.BackendAPI, Driver: "anthropic_sdk"})
	extra["provider"] = 123
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model", Extra: extra})
	if err == nil {
		t.Error("StartTurn() with wrong type provider = nil, want error")
	}
	if runner.called {
		t.Error("runner.Run was called when provider has wrong type")
	}
}

func TestAgentBackend_TmuxAdapter_WrongTypeProvider(t *testing.T) {
	handler := &testTmuxAgentHandler{}
	backend := NewTmuxBackend(handler)
	extra := testBackendExtra(t, engine.AgentRoute{Provider: "anthropic", Model: "test-model", Backend: engine.BackendCLI, Driver: "claude_cli"})
	extra["provider"] = 123
	_, err := backend.StartTurn(context.Background(), agentbackend.UserMessage{Text: "test"}, agentbackend.TurnOptions{Model: "test-model", Extra: extra})
	if err == nil {
		t.Error("StartTurn() with wrong type provider = nil, want error")
	}
	if handler.called || handler.sessionCalled {
		t.Error("tmux handler was called when provider has wrong type")
	}
}

type testTmuxAgentHandler struct {
	outcome       runtime.Outcome
	err           error
	called        bool
	sessionCalled bool
	ctx           context.Context
	exec          *engine.Execution
	node          *model.Node
	route         engine.AgentRoute
	prompt        string
}

func (h *testTmuxAgentHandler) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	h.called = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	h.route = route
	return h.outcome, h.err
}

func (h *testTmuxAgentHandler) ExecuteAgentWithSession(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute, tmpl *templates.Template, toolName string, prompt string, modelID string, cfg transport.SessionConfig) (runtime.Outcome, error) {
	h.sessionCalled = true
	h.ctx = ctx
	h.exec = exec
	h.node = node
	h.route = route
	h.prompt = prompt
	return h.outcome, h.err
}

type testEngineAgentBackend struct {
	called   bool
	exec     *engine.Execution
	node     *model.Node
	prompt   string
	route    engine.AgentRoute
	response string
	outcome  *runtime.Outcome
	err      error
}

func (b *testEngineAgentBackend) Run(ctx context.Context, exec *engine.Execution, node *model.Node, prompt string, route engine.AgentRoute) (string, *runtime.Outcome, error) {
	b.called = true
	b.exec = exec
	b.node = node
	b.prompt = prompt
	b.route = route
	return b.response, b.outcome, b.err
}

func collectTurnEvents(t *testing.T, stream agentbackend.TurnStream) []agentbackend.TurnEvent {
	t.Helper()
	events := []agentbackend.TurnEvent{}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("stream.Recv() error = %v", err)
		}
		events = append(events, ev)
	}
}

func testBackendExtra(t *testing.T, route engine.AgentRoute) map[string]any {
	t.Helper()
	logsRoot := t.TempDir()
	worktree := t.TempDir()
	node := &model.Node{ID: "turn", Attrs: map[string]string{"prompt": "node prompt", "auto_status": "true"}}
	eng := &engine.Engine{
		Options:     engine.RunOptions{RunID: "test-run"},
		LogsRoot:    logsRoot,
		WorktreeDir: worktree,
	}
	execCtx := &engine.Execution{
		LogsRoot:    logsRoot,
		WorktreeDir: worktree,
		Engine:      eng,
		Context:     runtime.NewContext(),
	}
	return map[string]any{
		"provider": route.Provider,
		"driver":   route.Driver,
		"exec":     execCtx,
		"node":     node,
		"route":    route,
	}
}
