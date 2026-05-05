// Tests for the ToolControlKilroy orchestration loop.
//
// These tests exercise RunTurn against stub backends to verify the
// loop handles text events, tool calls, cancellation, and guards.
package loop

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/auth"
)

// stubBackend is a test double for AgentBackend.
type stubBackend struct {
	toolControlMode agentbackend.ToolControlMode
	events          []agentbackend.TurnEvent
	toolResults     []agentbackend.ToolResult
	startErr        error
}

func (b *stubBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	if b.startErr != nil {
		return nil, b.startErr
	}
	return &stubTurnStream{backend: b, events: b.events}, nil
}

func (b *stubBackend) ToolControl() agentbackend.ToolControlMode {
	return b.toolControlMode
}

func (b *stubBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       true,
		TokenStreaming: true,
		ToolInjection:  b.toolControlMode == agentbackend.ToolControlKilroy,
	}
}

func (b *stubBackend) Close() error {
	return nil
}

// stubTurnStream is a test double for TurnStream.
type stubTurnStream struct {
	backend  *stubBackend
	events   []agentbackend.TurnEvent
	position int
	closed   bool
}

func (s *stubTurnStream) Recv() (agentbackend.TurnEvent, error) {
	if s.closed {
		return agentbackend.TurnEvent{}, errors.New("stream closed")
	}
	if s.position >= len(s.events) {
		return agentbackend.TurnEvent{}, io.EOF
	}
	ev := s.events[s.position]
	s.position++
	return ev, nil
}

func (s *stubTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	if s.closed {
		return errors.New("stream closed")
	}
	s.backend.toolResults = append(s.backend.toolResults, r)
	return nil
}

func (s *stubTurnStream) Close() error {
	s.closed = true
	return nil
}

// TestControl_TextOnlyTurn verifies a simple text response without tools.
func TestControl_TextOnlyTurn(t *testing.T) {
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventText, Text: "Hello, "},
			{Type: agentbackend.TurnEventText, Text: "world!"},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Say hello"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	if result.Text != "Hello, world!" {
		t.Errorf("Text = %q, want %q", result.Text, "Hello, world!")
	}
	if result.StopReason != "end_turn" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "end_turn")
	}
	if len(result.ToolCalls) != 0 {
		t.Errorf("ToolCalls = %d, want 0", len(result.ToolCalls))
	}
}

// TestControl_OneToolRoundTrip verifies a single tool call and result cycle.
func TestControl_OneToolRoundTrip(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:   "call_1",
		Name: "get_weather",
		Input: map[string]any{
			"location": "Seattle",
		},
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
			{Type: agentbackend.TurnEventText, Text: "The weather is sunny."},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		if call.Name != "get_weather" {
			t.Errorf("Tool name = %q, want %q", call.Name, "get_weather")
		}
		return agentbackend.ToolResult{
			ToolUseID: call.ID,
			Content:   `{"temp": 72, "condition": "sunny"}`,
		}, nil
	})

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "What's the weather?"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, handler, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	if result.Text != "The weather is sunny." {
		t.Errorf("Text = %q, want %q", result.Text, "The weather is sunny.")
	}
	if len(result.ToolCalls) != 1 {
		t.Errorf("ToolCalls = %d, want 1", len(result.ToolCalls))
	}
	if result.ToolCalls[0].Name != "get_weather" {
		t.Errorf("ToolCalls[0].Name = %q, want %q", result.ToolCalls[0].Name, "get_weather")
	}

	// Verify tool result was sent.
	if len(backend.toolResults) != 1 {
		t.Fatalf("toolResults = %d, want 1", len(backend.toolResults))
	}
	if backend.toolResults[0].ToolUseID != "call_1" {
		t.Errorf("toolResults[0].ToolUseID = %q, want %q", backend.toolResults[0].ToolUseID, "call_1")
	}
}

// TestControl_ContextCancellation verifies the loop respects context cancellation.
func TestControl_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a stream that blocks until context is cancelled.
	stream := &blockingTurnStream{
		ctx:        ctx,
		blockUntil: make(chan struct{}),
	}

	// Start the turn in a goroutine and cancel quickly.
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	blockingBackend := &blockingBackend{stream: stream}

	done := make(chan struct{})
	var result TurnResult
	var runErr error
	go func() {
		result, runErr = RunTurn(ctx, blockingBackend, msg, opts, nil, cfg)
		close(done)
	}()

	// Cancel after a short delay.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for completion or timeout.
	select {
	case <-done:
		// Expected
	case <-time.After(2 * time.Second):
		t.Fatal("RunTurn did not return after context cancellation")
	}

	if runErr == nil {
		t.Error("RunTurn error = nil, want context cancellation error")
	}
	if result.StopReason != "context_cancelled" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "context_cancelled")
	}
}

// blockingTurnStream blocks on Recv until the context is cancelled or blockUntil is closed.
type blockingTurnStream struct {
	ctx        context.Context
	blockUntil chan struct{}
	closed     bool
	mu         chan struct{} // Used for synchronization in tests
}

func (s *blockingTurnStream) Recv() (agentbackend.TurnEvent, error) {
	select {
	case <-s.ctx.Done():
		// Return an error when context is cancelled, simulating connection close.
		return agentbackend.TurnEvent{}, s.ctx.Err()
	case <-s.blockUntil:
		return agentbackend.TurnEvent{}, io.EOF
	}
}

func (s *blockingTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	return nil
}

func (s *blockingTurnStream) Close() error {
	if !s.closed {
		s.closed = true
		close(s.blockUntil)
	}
	return nil
}

type blockingBackend struct {
	stream *blockingTurnStream
}

func (b *blockingBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	return b.stream, nil
}

func (b *blockingBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

func (b *blockingBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{}
}

func (b *blockingBackend) Close() error {
	return nil
}

// TestControl_MaxIterationsGuard verifies the max iterations guard stops the loop.
func TestControl_MaxIterationsGuard(t *testing.T) {
	// Backend that always requests a tool call (infinite loop without guard).
	toolCall := agentbackend.ToolCall{
		ID:    "call_1",
		Name:  "infinite_tool",
		Input: map[string]any{},
	}

	// The stream returns the same tool_use event repeatedly.
	infiniteStream := &infiniteTurnStream{toolCall: toolCall}
	infiniteBackend := &infiniteBackend{stream: infiniteStream}

	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		return agentbackend.ToolResult{
			ToolUseID: call.ID,
			Content:   "result",
		}, nil
	})

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 3} // Stop after 3 iterations

	_, err := RunTurn(ctx, infiniteBackend, msg, opts, handler, cfg)

	if err == nil {
		t.Error("RunTurn error = nil, want max iterations error")
	}
	if infiniteStream.toolCallsReceived < 3 {
		t.Errorf("Tool calls received = %d, want at least 3", infiniteStream.toolCallsReceived)
	}
}

// infiniteTurnStream always returns a tool_use event.
type infiniteTurnStream struct {
	toolCall          agentbackend.ToolCall
	toolCallsReceived int
}

func (s *infiniteTurnStream) Recv() (agentbackend.TurnEvent, error) {
	// Small delay to prevent tight loop.
	time.Sleep(1 * time.Millisecond)
	return agentbackend.TurnEvent{
		Type: agentbackend.TurnEventToolUse,
		Tool: &s.toolCall,
	}, nil
}

func (s *infiniteTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	s.toolCallsReceived++
	return nil
}

func (s *infiniteTurnStream) Close() error {
	return nil
}

type infiniteBackend struct {
	stream *infiniteTurnStream
}

func (b *infiniteBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	return b.stream, nil
}

func (b *infiniteBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

func (b *infiniteBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{ToolInjection: true}
}

func (b *infiniteBackend) Close() error {
	return nil
}

// TestControl_ToolErrorHandling verifies tool errors are reported back to the agent.
func TestControl_ToolErrorHandling(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:    "call_1",
		Name:  "failing_tool",
		Input: map[string]any{},
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	toolErr := errors.New("tool execution failed")
	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		return agentbackend.ToolResult{}, toolErr
	})

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, handler, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}

	// Verify the error was sent as a tool result.
	if len(backend.toolResults) != 1 {
		t.Fatalf("toolResults = %d, want 1", len(backend.toolResults))
	}
	if !backend.toolResults[0].IsError {
		t.Error("toolResults[0].IsError = false, want true")
	}
	if backend.toolResults[0].Content != toolErr.Error() {
		t.Errorf("toolResults[0].Content = %q, want %q", backend.toolResults[0].Content, toolErr.Error())
	}
	if result.StopReason != "end_turn" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "end_turn")
	}
}

// TestControl_ToolControlDriverMode verifies the loop observes but doesn't control tools in driver mode.
func TestControl_ToolControlDriverMode(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:    "call_1",
		Name:  "driver_tool",
		Input: map[string]any{},
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlDriver,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
			{Type: agentbackend.TurnEventToolResult, Result: &agentbackend.ToolResult{
				ToolUseID: "call_1",
				Content:   "driver result",
			}},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	// Handler should not be called in driver mode.
	handlerCalled := false
	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		handlerCalled = true
		return agentbackend.ToolResult{}, nil
	})

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, handler, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	if handlerCalled {
		t.Error("Handler was called in driver mode, should not be")
	}
	if len(backend.toolResults) != 0 {
		t.Errorf("toolResults = %d, want 0 (driver mode)", len(backend.toolResults))
	}
	if len(result.ToolCalls) != 1 {
		t.Errorf("ToolCalls = %d, want 1", len(result.ToolCalls))
	}
}

// TestControl_StartTurnError verifies errors from StartTurn are propagated.
func TestControl_StartTurnError(t *testing.T) {
	startErr := errors.New("backend connection failed")
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		startErr:        startErr,
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err == nil {
		t.Fatal("RunTurn error = nil, want error")
	}
	if result.StopReason != "error" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "error")
	}
	if !errors.Is(err, startErr) {
		t.Errorf("error = %v, want %v", err, startErr)
	}
}

// TestControl_ThinkingAccumulation verifies thinking content is accumulated.
func TestControl_ThinkingAccumulation(t *testing.T) {
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventThinking, Text: "Step 1: "},
			{Type: agentbackend.TurnEventThinking, Text: "Analyze...\n"},
			{Type: agentbackend.TurnEventText, Text: "The answer is 42."},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Think step by step"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	wantThinking := "Step 1: Analyze...\n"
	if result.Thinking != wantThinking {
		t.Errorf("Thinking = %q, want %q", result.Thinking, wantThinking)
	}
	if result.Text != "The answer is 42." {
		t.Errorf("Text = %q, want %q", result.Text, "The answer is 42.")
	}
}

// TestControl_UsageTracking verifies token usage is captured.
func TestControl_UsageTracking(t *testing.T) {
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventText, Text: "Response."},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{
				StopReason:   "end_turn",
				InputTokens:  100,
				OutputTokens: 50,
			}},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	if result.Usage.InputTokens != 100 {
		t.Errorf("Usage.InputTokens = %d, want 100", result.Usage.InputTokens)
	}
	if result.Usage.OutputTokens != 50 {
		t.Errorf("Usage.OutputTokens = %d, want 50", result.Usage.OutputTokens)
	}
}

// TestControl_StallTimeout verifies the stall timeout triggers when no events arrive.
func TestControl_StallTimeout(t *testing.T) {
	// Create a backend that blocks on Recv but will eventually return EOF
	// if not cancelled first.
	stream := &slowTurnStream{delay: 5 * time.Second} // Will block longer than stall timeout
	backend := &slowBackend{stream: stream}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		StallTimeout:  100 * time.Millisecond,
	}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err == nil {
		t.Error("RunTurn error = nil, want stall timeout error")
	}
	if result.StopReason != "stall_timeout" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "stall_timeout")
	}
}

// slowTurnStream delays before returning EOF.
type slowTurnStream struct {
	delay time.Duration
}

func (s *slowTurnStream) Recv() (agentbackend.TurnEvent, error) {
	time.Sleep(s.delay)
	return agentbackend.TurnEvent{}, io.EOF
}

func (s *slowTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	return nil
}

func (s *slowTurnStream) Close() error {
	return nil
}

type slowBackend struct {
	stream *slowTurnStream
}

func (b *slowBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	return b.stream, nil
}

func (b *slowBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

func (b *slowBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{}
}

func (b *slowBackend) Close() error {
	return nil
}

// TestControl_NilHandlerError verifies that a nil handler in ToolControlKilroy mode returns an error.
func TestControl_NilHandlerError(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:    "call_1",
		Name:  "some_tool",
		Input: map[string]any{},
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{MaxIterations: 10}

	// Pass nil handler with ToolControlKilroy mode - should return error
	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err == nil {
		t.Fatal("RunTurn error = nil, want error for nil handler")
	}
	if result.StopReason != "error" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "error")
	}
	if len(backend.toolResults) != 0 {
		t.Errorf("toolResults = %d, want 0 (no tool result should be sent)", len(backend.toolResults))
	}
}

// TestControl_AuthResolutionError verifies that auth resolution failures are propagated.
func TestControl_AuthResolutionError(t *testing.T) {
	authErr := errors.New("credential resolution failed")
	mockResolver := &stubAuthResolver{resolveErr: authErr}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventText, Text: "Hello"},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		AuthResolver:  mockResolver,
	}

	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)

	if err == nil {
		t.Fatal("RunTurn error = nil, want auth error")
	}
	if result.StopReason != "error" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "error")
	}
	if !errors.Is(err, authErr) {
		t.Errorf("error = %v, want auth error wrapping %v", err, authErr)
	}
}

// TestControl_EventSinkHappyPath verifies the event sequence for a successful turn with tool calls.
func TestControl_EventSinkHappyPath(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:   "call_1",
		Name: "get_weather",
		Input: map[string]any{
			"location": "Seattle",
		},
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
			{Type: agentbackend.TurnEventText, Text: "The weather is sunny."},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		return agentbackend.ToolResult{
			ToolUseID: call.ID,
			Content:   `{"temp": 72}`,
		}, nil
	})

	// Collect events via EventSink.
	var events []LoopEvent
	sink := func(evt LoopEvent) {
		events = append(events, evt)
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "What's the weather?"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		EventSink:     sink,
	}

	_, err := RunTurn(ctx, backend, msg, opts, handler, cfg)
	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}

	// Verify event sequence: turn_start, tool_call, tool_result, turn_end.
	if len(events) != 4 {
		t.Fatalf("Expected 4 events, got %d: %+v", len(events), events)
	}

	if events[0].Type != TurnEventTurnStart {
		t.Errorf("Event[0].Type = %q, want %q", events[0].Type, TurnEventTurnStart)
	}

	if events[1].Type != TurnEventToolCall {
		t.Errorf("Event[1].Type = %q, want %q", events[1].Type, TurnEventToolCall)
	}
	if events[1].ToolCall == nil || events[1].ToolCall.Name != "get_weather" {
		t.Errorf("Event[1].ToolCall.Name = %q, want %q", events[1].ToolCall.Name, "get_weather")
	}

	if events[2].Type != TurnEventToolResult {
		t.Errorf("Event[2].Type = %q, want %q", events[2].Type, TurnEventToolResult)
	}
	if events[2].ToolResult == nil || events[2].ToolResult.Content != `{"temp": 72}` {
		t.Errorf("Event[2].ToolResult.Content = %q, want %q", events[2].ToolResult.Content, `{"temp": 72}`)
	}

	if events[3].Type != TurnEventTurnEnd {
		t.Errorf("Event[3].Type = %q, want %q", events[3].Type, TurnEventTurnEnd)
	}
}

// TestControl_EventSinkErrorPath verifies the event sequence for an error turn.
func TestControl_EventSinkErrorPath(t *testing.T) {
	startErr := errors.New("backend connection failed")
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		startErr:        startErr,
	}

	// Collect events via EventSink.
	var events []LoopEvent
	sink := func(evt LoopEvent) {
		events = append(events, evt)
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		EventSink:     sink,
	}

	_, err := RunTurn(ctx, backend, msg, opts, nil, cfg)
	if err == nil {
		t.Fatal("RunTurn error = nil, want error")
	}

	// Verify event sequence: turn_start, turn_error.
	if len(events) != 2 {
		t.Fatalf("Expected 2 events, got %d: %+v", len(events), events)
	}

	if events[0].Type != TurnEventTurnStart {
		t.Errorf("Event[0].Type = %q, want %q", events[0].Type, TurnEventTurnStart)
	}

	if events[1].Type != TurnEventTurnError {
		t.Errorf("Event[1].Type = %q, want %q", events[1].Type, TurnEventTurnError)
	}
	if events[1].Error == nil {
		t.Error("Event[1].Error = nil, want non-nil error")
	}
}

// TestControl_EventSinkNil verifies that nil EventSink does not panic.
func TestControl_EventSinkNil(t *testing.T) {
	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlKilroy,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventText, Text: "Hello"},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		EventSink:     nil, // Explicitly nil
	}

	// Should not panic.
	result, err := RunTurn(ctx, backend, msg, opts, nil, cfg)
	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}
	if result.StopReason != "end_turn" {
		t.Errorf("StopReason = %q, want %q", result.StopReason, "end_turn")
	}
}

// TestControl_EventSinkDriverMode verifies the event sequence for ToolControlDriver mode.
func TestControl_EventSinkDriverMode(t *testing.T) {
	toolCall := agentbackend.ToolCall{
		ID:    "call_1",
		Name:  "driver_tool",
		Input: map[string]any{"param": "value"},
	}
	toolResult := agentbackend.ToolResult{
		ToolUseID: "call_1",
		Content:   "driver result",
	}

	backend := &stubBackend{
		toolControlMode: agentbackend.ToolControlDriver,
		events: []agentbackend.TurnEvent{
			{Type: agentbackend.TurnEventToolUse, Tool: &toolCall},
			{Type: agentbackend.TurnEventToolResult, Result: &toolResult},
			{Type: agentbackend.TurnEventText, Text: "Done."},
			{Type: agentbackend.TurnEventTurnEnd, End: &agentbackend.TurnEndInfo{StopReason: "end_turn"}},
		},
	}

	// Collect events via EventSink.
	var events []LoopEvent
	sink := func(evt LoopEvent) {
		events = append(events, evt)
	}

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Test"}
	opts := agentbackend.TurnOptions{Model: "test-model"}
	cfg := TurnConfig{
		MaxIterations: 10,
		EventSink:     sink,
	}

	// Handler should not be called in driver mode.
	handlerCalled := false
	handler := ToolHandlerFunc(func(ctx context.Context, call agentbackend.ToolCall) (agentbackend.ToolResult, error) {
		handlerCalled = true
		return agentbackend.ToolResult{}, nil
	})

	_, err := RunTurn(ctx, backend, msg, opts, handler, cfg)
	if err != nil {
		t.Fatalf("RunTurn error = %v", err)
	}

	if handlerCalled {
		t.Error("Handler was called in driver mode, should not be")
	}

	// Verify event sequence: turn_start, tool_call, tool_result, turn_end.
	if len(events) != 4 {
		t.Fatalf("Expected 4 events, got %d: %+v", len(events), events)
	}

	if events[0].Type != TurnEventTurnStart {
		t.Errorf("Event[0].Type = %q, want %q", events[0].Type, TurnEventTurnStart)
	}

	if events[1].Type != TurnEventToolCall {
		t.Errorf("Event[1].Type = %q, want %q", events[1].Type, TurnEventToolCall)
	}
	if events[1].ToolCall == nil || events[1].ToolCall.Name != "driver_tool" {
		t.Errorf("Event[1].ToolCall.Name = %q, want %q", events[1].ToolCall.Name, "driver_tool")
	}

	if events[2].Type != TurnEventToolResult {
		t.Errorf("Event[2].Type = %q, want %q", events[2].Type, TurnEventToolResult)
	}
	if events[2].ToolResult == nil || events[2].ToolResult.Content != "driver result" {
		t.Errorf("Event[2].ToolResult.Content = %q, want %q", events[2].ToolResult.Content, "driver result")
	}

	if events[3].Type != TurnEventTurnEnd {
		t.Errorf("Event[3].Type = %q, want %q", events[3].Type, TurnEventTurnEnd)
	}
}

// stubAuthResolver is a test double for AuthResolver.
type stubAuthResolver struct {
	resolveErr error
}

func (r *stubAuthResolver) ResolveCredential(ctx context.Context, route auth.AgentRoute) (auth.Credential, error) {
	if r.resolveErr != nil {
		return auth.Credential{}, r.resolveErr
	}
	return auth.Credential{}, nil
}
