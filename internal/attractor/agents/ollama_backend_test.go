package agents

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/auth"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
)

// TestOllamaBackend_StartTurn tests that OllamaBackend can start a turn
// against an httptest.Server faking a minimal /api/chat response.
func TestOllamaBackend_StartTurn(t *testing.T) {
	// Create a fake Ollama server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request path and method.
		if r.URL.Path != "/api/chat" {
			t.Errorf("expected path /api/chat, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}

		// Verify Content-Type header.
		contentType := r.Header.Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", contentType)
		}

		// Parse the request to verify structure.
		var reqBody ollamaChatRequest
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify request fields.
		if reqBody.Model != "llama2" {
			t.Errorf("expected model llama2, got %s", reqBody.Model)
		}
		if len(reqBody.Messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(reqBody.Messages))
		}
		if reqBody.Messages[0].Role != "user" {
			t.Errorf("expected role user, got %s", reqBody.Messages[0].Role)
		}
		if reqBody.Messages[0].Content != "Hello, Ollama!" {
			t.Errorf("expected content 'Hello, Ollama!', got %s", reqBody.Messages[0].Content)
		}

		// Send a minimal response.
		resp := ollamaChatResponse{
			Message: ollamaResponseMessage{
				Role:    "assistant",
				Content: "Hello! How can I help you today?",
			},
			Done: true,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create backend pointing to the test server.
	backend := NewOllamaBackend(WithBaseURL(server.URL))

	// Start a turn.
	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Hello, Ollama!"}
	opts := agentbackend.TurnOptions{Model: "llama2"}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	defer stream.Close()

	// Receive events from the stream.
	var gotText bool
	var gotEnd bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		switch event.Type {
		case agentbackend.TurnEventText:
			gotText = true
			if event.Text != "Hello! How can I help you today?" {
				t.Errorf("expected text 'Hello! How can I help you today?', got %s", event.Text)
			}
		case agentbackend.TurnEventTurnEnd:
			gotEnd = true
			if event.End == nil || event.End.StopReason != "end_turn" {
				t.Errorf("expected stop_reason end_turn, got %+v", event.End)
			}
		}
	}

	if !gotText {
		t.Error("expected to receive a text event")
	}
	if !gotEnd {
		t.Error("expected to receive a turn_end event")
	}
}

// TestOllamaBackend_HonorsAuthResolver tests that the auth resolver is
// called even when Ollama doesn't need credentials.
func TestOllamaBackend_HonorsAuthResolver(t *testing.T) {
	// Track whether the resolver was called.
	resolverCalled := false
	var capturedRoute auth.AgentRoute

	// Create a fake resolver that records the call.
	resolver := &fakeAuthResolver{
		onResolve: func(route auth.AgentRoute) (auth.Credential, error) {
			resolverCalled = true
			capturedRoute = route
			return auth.Credential{}, nil // Return empty credential as expected for Ollama
		},
	}

	// Create a fake Ollama server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := ollamaChatResponse{
			Message: ollamaResponseMessage{
				Role:    "assistant",
				Content: "Hello!",
			},
			Done: true,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create backend with the resolver.
	backend := NewOllamaBackend(
		WithBaseURL(server.URL),
		WithAuthResolver(resolver),
	)

	// Start a turn.
	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Hello!"}
	opts := agentbackend.TurnOptions{Model: "llama2"}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	stream.Close()

	// Verify the resolver was called.
	if !resolverCalled {
		t.Error("expected auth resolver to be called, but it wasn't")
	}

	// Verify the route was correctly populated.
	if capturedRoute.Provider != "ollama" {
		t.Errorf("expected provider ollama, got %s", capturedRoute.Provider)
	}
	if capturedRoute.Driver != "ollama" {
		t.Errorf("expected driver ollama, got %s", capturedRoute.Driver)
	}
}

// TestOllamaBackend_ToolControlKilroy verifies that OllamaBackend reports
// ToolControlKilroy as required.
func TestOllamaBackend_ToolControlKilroy(t *testing.T) {
	backend := NewOllamaBackend()

	if backend.ToolControl() != agentbackend.ToolControlKilroy {
		t.Errorf("expected ToolControlKilroy, got %v", backend.ToolControl())
	}
}

// TestOllamaBackend_WithToolCalls tests that tool calls in the response
// are properly emitted as TurnEventToolUse events.
func TestOllamaBackend_WithToolCalls(t *testing.T) {
	// Create a fake Ollama server that returns a tool call.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := ollamaChatResponse{
			Message: ollamaResponseMessage{
				Role:    "assistant",
				Content: "",
				ToolCalls: []ollamaToolCall{
					{
						Function: ollamaToolCallFunction{
							Name: "get_weather",
							Arguments: map[string]any{
								"location": "San Francisco",
							},
						},
					},
				},
			},
			Done: true,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	backend := NewOllamaBackend(WithBaseURL(server.URL))

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "What's the weather?"}
	opts := agentbackend.TurnOptions{
		Model: "llama2",
		Tools: []agentbackend.ToolSchema{
			{
				Name:        "get_weather",
				Description: "Get weather for a location",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"location": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	defer stream.Close()

	// Receive events from the stream.
	var gotTool bool
	var gotEnd bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		switch event.Type {
		case agentbackend.TurnEventToolUse:
			gotTool = true
			if event.Tool == nil {
				t.Fatal("expected non-nil Tool in tool_use event")
			}
			if event.Tool.Name != "get_weather" {
				t.Errorf("expected tool name get_weather, got %s", event.Tool.Name)
			}
			if event.Tool.Input["location"] != "San Francisco" {
				t.Errorf("expected location San Francisco, got %v", event.Tool.Input["location"])
			}
		case agentbackend.TurnEventTurnEnd:
			gotEnd = true
		}
	}

	if !gotTool {
		t.Error("expected to receive a tool_use event")
	}
	if !gotEnd {
		t.Error("expected to receive a turn_end event")
	}
}

// TestOllamaBackend_DefaultBaseURL verifies the default base URL.
func TestOllamaBackend_DefaultBaseURL(t *testing.T) {
	backend := NewOllamaBackend()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := ollamaChatResponse{
			Message: ollamaResponseMessage{
				Role:    "assistant",
				Content: "Hello!",
			},
			Done: true,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// We can't test the default URL directly without a real Ollama server,
	// but we can verify the struct field is set correctly.
	if backend.baseURL != "http://localhost:11434" {
		t.Errorf("expected default base URL http://localhost:11434, got %s", backend.baseURL)
	}
}

// TestOllamaBackend_HTTPError tests handling of HTTP error responses.
func TestOllamaBackend_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	backend := NewOllamaBackend(WithBaseURL(server.URL))

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Hello!"}
	opts := agentbackend.TurnOptions{Model: "llama2"}

	_, err := backend.StartTurn(ctx, msg, opts)
	if err == nil {
		t.Fatal("expected error for HTTP 500 response, got nil")
	}
}

// TestOllamaBackend_Capabilities verifies the backend capabilities.
func TestOllamaBackend_Capabilities(t *testing.T) {
	backend := NewOllamaBackend()
	caps := backend.Capabilities()

	if caps.Thinking {
		t.Error("expected Thinking to be false")
	}
	if caps.TokenStreaming {
		t.Error("expected TokenStreaming to be false")
	}
	if caps.CostTracking {
		t.Error("expected CostTracking to be false")
	}
	if !caps.ToolInjection {
		t.Error("expected ToolInjection to be true")
	}
}

// TestOllamaBackend_Close verifies Close returns nil without error.
func TestOllamaBackend_Close(t *testing.T) {
	backend := NewOllamaBackend()
	if err := backend.Close(); err != nil {
		t.Errorf("expected Close to return nil, got %v", err)
	}
}

// TestOllamaBackend_ToolCall_Roundtrip tests the full tool call roundtrip:
// 1. StartTurn makes first /api/chat call
// 2. Stream yields TurnEventToolUse
// 3. SendToolResult makes second /api/chat call with tool result
// 4. Stream yields TurnEventText + TurnEventTurnEnd
func TestOllamaBackend_ToolCall_Roundtrip(t *testing.T) {
	requestCount := 0

	// Create a fake Ollama server that returns different responses.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		if r.URL.Path != "/api/chat" {
			t.Errorf("expected path /api/chat, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}

		// Parse the request to verify structure.
		var reqBody ollamaChatRequest
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if requestCount == 1 {
			// First request: user message only.
			if len(reqBody.Messages) != 1 {
				t.Errorf("expected 1 message in first request, got %d", len(reqBody.Messages))
			}
			if reqBody.Messages[0].Content != "What's the weather?" {
				t.Errorf("expected user message, got %s", reqBody.Messages[0].Content)
			}

			// Return a tool call response.
			resp := ollamaChatResponse{
				Message: ollamaResponseMessage{
					Role:    "assistant",
					Content: "",
					ToolCalls: []ollamaToolCall{
						{
							Function: ollamaToolCallFunction{
								Name: "get_weather",
								Arguments: map[string]any{
									"location": "San Francisco",
								},
							},
						},
					},
				},
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		} else if requestCount == 2 {
			// Second request: should have assistant message + tool result.
			if len(reqBody.Messages) != 3 {
				t.Errorf("expected 3 messages in second request, got %d", len(reqBody.Messages))
			}
			if reqBody.Messages[0].Content != "What's the weather?" {
				t.Errorf("expected original user message, got %s", reqBody.Messages[0].Content)
			}
			// Verify message[1] is the assistant message with tool_calls.
			if reqBody.Messages[1].Role != "assistant" {
				t.Errorf("expected assistant role for message 1, got %s", reqBody.Messages[1].Role)
			}
			if len(reqBody.Messages[1].ToolCalls) == 0 {
				t.Errorf("expected message[1] to carry tool_calls, got none")
			}
			if reqBody.Messages[1].ToolCalls[0].Function.Name != "get_weather" {
				t.Errorf("expected tool call get_weather, got %s", reqBody.Messages[1].ToolCalls[0].Function.Name)
			}
			// Verify message[2] is the tool result with role=tool (not user).
			if reqBody.Messages[2].Role != "tool" {
				t.Errorf("expected tool role for message 2, got %s", reqBody.Messages[2].Role)
			}

			// Return a text response.
			resp := ollamaChatResponse{
				Message: ollamaResponseMessage{
					Role:    "assistant",
					Content: "The weather in San Francisco is sunny and 72°F.",
				},
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		} else {
			t.Errorf("unexpected request count: %d", requestCount)
		}
	}))
	defer server.Close()

	backend := NewOllamaBackend(WithBaseURL(server.URL))

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "What's the weather?"}
	opts := agentbackend.TurnOptions{
		Model: "llama2",
		Tools: []agentbackend.ToolSchema{
			{
				Name:        "get_weather",
				Description: "Get weather for a location",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"location": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	defer stream.Close()

	// Receive events from the first turn.
	var gotTool bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			t.Fatal("expected tool_use event before EOF")
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		if event.Type == agentbackend.TurnEventToolUse {
			gotTool = true
			if event.Tool == nil {
				t.Fatal("expected non-nil Tool in tool_use event")
			}
			if event.Tool.Name != "get_weather" {
				t.Errorf("expected tool name get_weather, got %s", event.Tool.Name)
			}
			break
		}
	}

	if !gotTool {
		t.Fatal("expected to receive a tool_use event")
	}

	// Send the tool result.
	result := agentbackend.ToolResult{
		ToolUseID: "ollama_tool_1",
		Content:   "Sunny, 72°F",
	}
	if err := stream.SendToolResult(ctx, result); err != nil {
		t.Fatalf("SendToolResult failed: %v", err)
	}

	// Receive events from the second turn.
	var gotText bool
	var gotEnd bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		switch event.Type {
		case agentbackend.TurnEventText:
			gotText = true
			if event.Text != "The weather in San Francisco is sunny and 72°F." {
				t.Errorf("expected weather response, got %s", event.Text)
			}
		case agentbackend.TurnEventTurnEnd:
			gotEnd = true
		}
	}

	if !gotText {
		t.Error("expected to receive a text event after tool result")
	}
	if !gotEnd {
		t.Error("expected to receive a turn_end event after tool result")
	}

	// Verify we made exactly 2 /api/chat calls.
	if requestCount != 2 {
		t.Errorf("expected 2 /api/chat calls, got %d", requestCount)
	}
}

// TestOllamaBackend_ComposesHTTPTransport tests that OllamaBackend composes
// the transport package rather than duplicating HTTP client construction.
// It verifies both:
// 1. NewOllamaBackend (without override) uses transport.NewRawHTTPClient()
// 2. WithHTTPClient accepts a transport-vended client and uses it
func TestOllamaBackend_ComposesHTTPTransport(t *testing.T) {
	// Verify that transport.NewRawHTTPClient returns a non-nil client.
	// This is the composition point - OllamaBackend should call this.
	transportClient := transport.NewRawHTTPClient()
	if transportClient == nil {
		t.Fatal("transport.NewRawHTTPClient() returned nil")
	}

	// Track whether a custom transport-vended client was used.
	customClientUsed := false

	// Create a custom HTTP client that records usage.
	customTransport := &testTransport{
		onRoundTrip: func(req *http.Request) (*http.Response, error) {
			customClientUsed = true
			// Just forward to default transport for the test.
			return http.DefaultTransport.RoundTrip(req)
		},
	}
	customClient := &http.Client{Transport: customTransport}

	// Create a fake Ollama server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := ollamaChatResponse{
			Message: ollamaResponseMessage{
				Role:    "assistant",
				Content: "Hello!",
			},
			Done: true,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create backend with custom HTTP client (wrapping transport-vended).
	backend := NewOllamaBackend(
		WithBaseURL(server.URL),
		WithHTTPClient(customClient),
	)

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Hello!"}
	opts := agentbackend.TurnOptions{Model: "llama2"}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	stream.Close()

	// Verify the custom HTTP client was used.
	if !customClientUsed {
		t.Error("expected custom HTTP client to be used, but it wasn't")
	}

	// Also verify that a backend created without WithHTTPClient uses
	// the transport-vended client (same pointer as NewRawHTTPClient).
	backendWithDefault := NewOllamaBackend(WithBaseURL(server.URL))
	if backendWithDefault.httpClient != transport.NewRawHTTPClient() {
		t.Error("expected default backend to use transport.NewRawHTTPClient() for its HTTP client")
	}
}

// fakeAuthResolver is a test helper that records calls and returns configured results.
type fakeAuthResolver struct {
	onResolve func(route auth.AgentRoute) (auth.Credential, error)
}

func (r *fakeAuthResolver) ResolveCredential(ctx context.Context, route auth.AgentRoute) (auth.Credential, error) {
	if r.onResolve != nil {
		return r.onResolve(route)
	}
	return auth.Credential{}, nil
}

// testTransport is a test helper that wraps an http.RoundTripper for verification.
type testTransport struct {
	onRoundTrip func(req *http.Request) (*http.Response, error)
}

func (t *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.onRoundTrip != nil {
		return t.onRoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}

// TestOllamaBackend_MultiStep_ToolCall tests two sequential tool-call roundtrips
// to ensure lastAssistantMsg is correctly updated after each SendToolResult call.
// This catches the stale lastAssistantMsg bug where the first assistant message
// would be incorrectly used in subsequent tool-call history.
func TestOllamaBackend_MultiStep_ToolCall(t *testing.T) {
	requestCount := 0
	var secondAssistantContent string

	// Create a fake Ollama server that returns different responses for each call.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		if r.URL.Path != "/api/chat" {
			t.Errorf("expected path /api/chat, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}

		// Parse the request to verify structure.
		var reqBody ollamaChatRequest
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if requestCount == 1 {
			// First request: user message only.
			if len(reqBody.Messages) != 1 {
				t.Errorf("expected 1 message in first request, got %d", len(reqBody.Messages))
			}
			if reqBody.Messages[0].Content != "Get weather and time" {
				t.Errorf("expected user message 'Get weather and time', got %s", reqBody.Messages[0].Content)
			}

			// Return first tool call response (get_weather).
			resp := ollamaChatResponse{
				Message: ollamaResponseMessage{
					Role:    "assistant",
					Content: "I'll get the weather for you.",
					ToolCalls: []ollamaToolCall{
						{
							Function: ollamaToolCallFunction{
								Name: "get_weather",
								Arguments: map[string]any{
									"location": "Boston",
								},
							},
						},
					},
				},
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		} else if requestCount == 2 {
			// Second request: should have assistant message + tool result.
			if len(reqBody.Messages) != 3 {
				t.Errorf("expected 3 messages in second request, got %d", len(reqBody.Messages))
			}
			if reqBody.Messages[0].Content != "Get weather and time" {
				t.Errorf("expected original user message, got %s", reqBody.Messages[0].Content)
			}
			// Verify message[1] is the FIRST assistant message with get_weather tool_calls.
			if reqBody.Messages[1].Role != "assistant" {
				t.Errorf("expected assistant role for message 1, got %s", reqBody.Messages[1].Role)
			}
			if reqBody.Messages[1].Content != "I'll get the weather for you." {
				t.Errorf("expected first assistant message content, got %s", reqBody.Messages[1].Content)
			}
			if len(reqBody.Messages[1].ToolCalls) == 0 {
				t.Errorf("expected message[1] to carry tool_calls, got none")
			}
			if reqBody.Messages[1].ToolCalls[0].Function.Name != "get_weather" {
				t.Errorf("expected tool call get_weather, got %s", reqBody.Messages[1].ToolCalls[0].Function.Name)
			}
			// Verify message[2] is the tool result.
			if reqBody.Messages[2].Role != "tool" {
				t.Errorf("expected tool role for message 2, got %s", reqBody.Messages[2].Role)
			}

			// Return second tool call response (get_time) - this tests that lastAssistantMsg
			// was correctly updated to this new assistant message.
			secondAssistantContent = "Now let me get the time."
			resp := ollamaChatResponse{
				Message: ollamaResponseMessage{
					Role:    "assistant",
					Content: secondAssistantContent,
					ToolCalls: []ollamaToolCall{
						{
							Function: ollamaToolCallFunction{
								Name:      "get_time",
								Arguments: map[string]any{},
							},
						},
					},
				},
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		} else if requestCount == 3 {
			// Third request: should have the SECOND assistant message + tool result.
			// THIS IS THE KEY ASSERTION - if lastAssistantMsg is stale, we'd see
			// the first assistant message ("I'll get the weather...") instead of
			// the second one ("Now let me get the time.").
			if len(reqBody.Messages) != 5 {
				t.Errorf("expected 5 messages in third request, got %d", len(reqBody.Messages))
			}

			// Message 0: original user message.
			if reqBody.Messages[0].Content != "Get weather and time" {
				t.Errorf("expected original user message, got %s", reqBody.Messages[0].Content)
			}

			// Message 1: first assistant message with get_weather.
			if reqBody.Messages[1].Role != "assistant" {
				t.Errorf("expected assistant role for message 1, got %s", reqBody.Messages[1].Role)
			}
			if reqBody.Messages[1].Content != "I'll get the weather for you." {
				t.Errorf("expected first assistant message, got %s", reqBody.Messages[1].Content)
			}

			// Message 2: first tool result.
			if reqBody.Messages[2].Role != "tool" {
				t.Errorf("expected tool role for message 2, got %s", reqBody.Messages[2].Role)
			}

			// Message 3: SECOND assistant message with get_time.
			// THIS IS WHERE THE BUG WOULD SHOW: if lastAssistantMsg wasn't updated,
			// this would incorrectly be "I'll get the weather for you." instead of
			// "Now let me get the time."
			if reqBody.Messages[3].Role != "assistant" {
				t.Errorf("expected assistant role for message 3, got %s", reqBody.Messages[3].Role)
			}
			if reqBody.Messages[3].Content != secondAssistantContent {
				t.Errorf("BUG: expected second assistant message '%s', got '%s' (stale lastAssistantMsg?)", secondAssistantContent, reqBody.Messages[3].Content)
			}
			if len(reqBody.Messages[3].ToolCalls) == 0 {
				t.Errorf("expected message[3] to carry tool_calls, got none")
			}
			if reqBody.Messages[3].ToolCalls[0].Function.Name != "get_time" {
				t.Errorf("expected tool call get_time, got %s", reqBody.Messages[3].ToolCalls[0].Function.Name)
			}

			// Message 4: second tool result.
			if reqBody.Messages[4].Role != "tool" {
				t.Errorf("expected tool role for message 4, got %s", reqBody.Messages[4].Role)
			}

			// Return final text response.
			resp := ollamaChatResponse{
				Message: ollamaResponseMessage{
					Role:    "assistant",
					Content: "It's sunny and 75°F in Boston, and the time is 2:30 PM.",
				},
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		} else {
			t.Errorf("unexpected request count: %d", requestCount)
		}
	}))
	defer server.Close()

	backend := NewOllamaBackend(WithBaseURL(server.URL))

	ctx := context.Background()
	msg := agentbackend.UserMessage{Text: "Get weather and time"}
	opts := agentbackend.TurnOptions{
		Model: "llama2",
		Tools: []agentbackend.ToolSchema{
			{
				Name:        "get_weather",
				Description: "Get weather for a location",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"location": map[string]any{"type": "string"},
					},
				},
			},
			{
				Name:        "get_time",
				Description: "Get current time",
				Parameters: map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
		},
	}

	stream, err := backend.StartTurn(ctx, msg, opts)
	if err != nil {
		t.Fatalf("StartTurn failed: %v", err)
	}
	defer stream.Close()

	// First roundtrip: receive get_weather tool call.
	var gotFirstTool bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			t.Fatal("expected tool_use event before EOF in first roundtrip")
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		if event.Type == agentbackend.TurnEventToolUse {
			if event.Tool == nil {
				t.Fatal("expected non-nil Tool in tool_use event")
			}
			if event.Tool.Name != "get_weather" {
				t.Errorf("expected tool name get_weather, got %s", event.Tool.Name)
			}
			gotFirstTool = true
			break
		}
	}

	if !gotFirstTool {
		t.Fatal("expected to receive get_weather tool_use event")
	}

	// Send first tool result.
	result1 := agentbackend.ToolResult{
		ToolUseID: "ollama_tool_1",
		Content:   "Sunny, 75°F in Boston",
	}
	if err := stream.SendToolResult(ctx, result1); err != nil {
		t.Fatalf("SendToolResult failed: %v", err)
	}

	// Second roundtrip: receive get_time tool call.
	// THIS IS WHERE THE BUG WOULD MANIFEST: if lastAssistantMsg is stale,
	// the next request would incorrectly include the first assistant message again.
	var gotSecondTool bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			t.Fatal("expected tool_use event before EOF in second roundtrip")
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		if event.Type == agentbackend.TurnEventToolUse {
			if event.Tool == nil {
				t.Fatal("expected non-nil Tool in tool_use event")
			}
			if event.Tool.Name != "get_time" {
				t.Errorf("expected tool name get_time, got %s", event.Tool.Name)
			}
			gotSecondTool = true
			break
		}
	}

	if !gotSecondTool {
		t.Fatal("expected to receive get_time tool_use event")
	}

	// Send second tool result.
	result2 := agentbackend.ToolResult{
		ToolUseID: "ollama_tool_2",
		Content:   "2:30 PM",
	}
	if err := stream.SendToolResult(ctx, result2); err != nil {
		t.Fatalf("SendToolResult failed: %v", err)
	}

	// Receive final text response.
	var gotFinalText bool
	var gotFinalEnd bool
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		switch event.Type {
		case agentbackend.TurnEventText:
			gotFinalText = true
			if event.Text != "It's sunny and 75°F in Boston, and the time is 2:30 PM." {
				t.Errorf("expected final response, got %s", event.Text)
			}
		case agentbackend.TurnEventTurnEnd:
			gotFinalEnd = true
		}
	}

	if !gotFinalText {
		t.Error("expected to receive final text event")
	}
	if !gotFinalEnd {
		t.Error("expected to receive final turn_end event")
	}

	// Verify we made exactly 3 /api/chat calls.
	if requestCount != 3 {
		t.Errorf("expected 3 /api/chat calls, got %d", requestCount)
	}
}
