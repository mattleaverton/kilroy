// Package agents provides the unified agent dispatcher and backend adapters.
//
// This file contains the OllamaBackend implementation — a third AgentBackend
// that composes the existing transport and auth infrastructure to talk to
// a local Ollama HTTP API.
package agents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	"github.com/danshapiro/kilroy/internal/attractor/agents/auth"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
)

// Compile-time assertion: OllamaBackend implements AgentBackend.
var _ agentbackend.AgentBackend = (*OllamaBackend)(nil)

// OllamaBackend implements AgentBackend for the Ollama HTTP API.
// It talks to Ollama's /api/chat endpoint and emits TurnStream events.
type OllamaBackend struct {
	baseURL      string
	httpClient   *http.Client
	authResolver auth.AuthResolver
}

// OllamaBackendOption configures an OllamaBackend.
type OllamaBackendOption func(*OllamaBackend)

// WithBaseURL sets a custom base URL for the Ollama API.
// Defaults to "http://localhost:11434".
func WithBaseURL(url string) OllamaBackendOption {
	return func(b *OllamaBackend) {
		b.baseURL = url
	}
}

// WithHTTPClient sets a custom HTTP client (typically from transport.NewRawHTTPClient).
// Defaults to the client returned by transport.NewRawHTTPClient().
func WithHTTPClient(client *http.Client) OllamaBackendOption {
	return func(b *OllamaBackend) {
		b.httpClient = client
	}
}

// WithAuthResolver sets the auth resolver for credential resolution.
// The resolver is called at StartTurn for consistency, even though Ollama
// typically doesn't require credentials.
func WithAuthResolver(resolver auth.AuthResolver) OllamaBackendOption {
	return func(b *OllamaBackend) {
		b.authResolver = resolver
	}
}

// NewOllamaBackend creates a new OllamaBackend with the given options.
func NewOllamaBackend(opts ...OllamaBackendOption) *OllamaBackend {
	b := &OllamaBackend{
		baseURL:    "http://localhost:11434",
		httpClient: transport.NewRawHTTPClient(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// ollamaChatRequest is the JSON body for Ollama's /api/chat endpoint.
type ollamaChatRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   bool            `json:"stream"`
	Tools    []ollamaTool    `json:"tools,omitempty"`
	Options  map[string]any  `json:"options,omitempty"`
}

// ollamaMessage represents a single message in the chat.
type ollamaMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content"`
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
}

// ollamaTool represents a tool definition for Ollama.
type ollamaTool struct {
	Type     string         `json:"type"`
	Function ollamaFunction `json:"function"`
}

// ollamaFunction describes the function schema.
type ollamaFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// ollamaChatResponse is the JSON response from Ollama's /api/chat endpoint.
type ollamaChatResponse struct {
	Message ollamaResponseMessage `json:"message"`
	Done    bool                  `json:"done"`
}

// ollamaResponseMessage is the message in the response.
type ollamaResponseMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content"`
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
}

// ollamaToolCall represents a tool call in the response.
type ollamaToolCall struct {
	Function ollamaToolCallFunction `json:"function"`
}

// ollamaToolCallFunction contains the tool call details.
type ollamaToolCallFunction struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// StartTurn begins a single conversation turn by calling Ollama's /api/chat.
func (b *OllamaBackend) StartTurn(ctx context.Context, msg agentbackend.UserMessage, opts agentbackend.TurnOptions) (agentbackend.TurnStream, error) {
	// Call auth resolver for consistency, even though Ollama typically
	// doesn't require credentials. This ensures uniform behavior across
	// all backends.
	if b.authResolver != nil {
		route := auth.AgentRoute{
			Provider: "ollama",
			Driver:   "ollama",
		}
		if _, err := b.authResolver.ResolveCredential(ctx, route); err != nil {
			return nil, fmt.Errorf("resolve credential: %w", err)
		}
	}

	// Build initial conversation history.
	messages := []ollamaMessage{
		{Role: "user", Content: msg.Text},
	}

	// Build the request body.
	reqBody := ollamaChatRequest{
		Model:    opts.Model,
		Stream:   false, // Non-streaming for simpler implementation
		Messages: messages,
	}

	// Add tools if provided.
	if len(opts.Tools) > 0 {
		reqBody.Tools = make([]ollamaTool, len(opts.Tools))
		for i, tool := range opts.Tools {
			reqBody.Tools[i] = ollamaTool{
				Type: "function",
				Function: ollamaFunction{
					Name:        tool.Name,
					Description: tool.Description,
					Parameters:  tool.Parameters,
				},
			}
		}
	}

	// Marshal the request.
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal chat request: %w", err)
	}

	// Create the HTTP request.
	url := b.baseURL + "/api/chat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the request.
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chat request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check for HTTP errors.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chat request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	// Parse the response.
	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("decode chat response: %w", err)
	}

	// Create and return the turn stream with the response.
	return &ollamaTurnStream{
		backend:          b,
		model:            opts.Model,
		tools:            opts.Tools,
		messages:         messages,
		response:         chatResp,
		lastAssistantMsg: chatResp.Message,
	}, nil
}

// ToolControl reports that Ollama uses kilroy-side tool control.
func (b *OllamaBackend) ToolControl() agentbackend.ToolControlMode {
	return agentbackend.ToolControlKilroy
}

// Capabilities surfaces optional features for the Ollama backend.
func (b *OllamaBackend) Capabilities() agentbackend.BackendCapabilities {
	return agentbackend.BackendCapabilities{
		Thinking:       false, // Ollama doesn't support thinking blocks
		TokenStreaming: false, // Not implemented in this version
		CostTracking:   false, // Ollama doesn't report cost
		ToolInjection:  true,  // Kilroy controls the tool loop
	}
}

// Close releases backend-held resources.
func (b *OllamaBackend) Close() error {
	// Nothing to close for this implementation.
	return nil
}

// ollamaTurnStream implements TurnStream for OllamaBackend.
type ollamaTurnStream struct {
	backend          *OllamaBackend
	model            string
	tools            []agentbackend.ToolSchema
	messages         []ollamaMessage
	response         ollamaChatResponse
	lastAssistantMsg ollamaResponseMessage
	sentText         bool
	sentTools        bool
	sentEnd          bool
	toolIndex        int
	eventQueue       []agentbackend.TurnEvent
	mu               sync.Mutex
}

// Recv returns the next event in the stream.
func (s *ollamaTurnStream) Recv() (agentbackend.TurnEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, check if there are queued events from a tool result roundtrip.
	if len(s.eventQueue) > 0 {
		event := s.eventQueue[0]
		s.eventQueue = s.eventQueue[1:]
		return event, nil
	}

	// First, send any text content.
	if !s.sentText && s.response.Message.Content != "" {
		s.sentText = true
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventText,
			Text: s.response.Message.Content,
		}, nil
	}

	// Then, send tool calls one at a time.
	if !s.sentTools && s.toolIndex < len(s.response.Message.ToolCalls) {
		toolCall := s.response.Message.ToolCalls[s.toolIndex]
		s.toolIndex++
		// Mark as sent all tools after we've emitted the last one.
		if s.toolIndex >= len(s.response.Message.ToolCalls) {
			s.sentTools = true
		}
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventToolUse,
			Tool: &agentbackend.ToolCall{
				ID:    fmt.Sprintf("ollama_tool_%d", s.toolIndex),
				Name:  toolCall.Function.Name,
				Input: toolCall.Function.Arguments,
			},
		}, nil
	}
	// If no tool calls, mark tools as sent immediately.
	if !s.sentTools && len(s.response.Message.ToolCalls) == 0 {
		s.sentTools = true
	}

	// Finally, send the turn_end event.
	if !s.sentEnd {
		s.sentEnd = true
		return agentbackend.TurnEvent{
			Type: agentbackend.TurnEventTurnEnd,
			End: &agentbackend.TurnEndInfo{
				StopReason: "end_turn",
			},
		}, nil
	}

	// All events have been sent.
	return agentbackend.TurnEvent{}, io.EOF
}

// SendToolResult feeds a tool result back into the conversation.
// For ToolControlKilroy, this appends the tool result to the conversation
// history, makes a new /api/chat call, and queues the response events.
func (s *ollamaTurnStream) SendToolResult(ctx context.Context, r agentbackend.ToolResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, add the assistant's message with the tool call to history.
	// This preserves the tool_calls so Ollama can bind the result to the call.
	assistantMsg := ollamaMessage{
		Role:      "assistant",
		Content:   s.lastAssistantMsg.Content,
		ToolCalls: s.lastAssistantMsg.ToolCalls,
	}
	s.messages = append(s.messages, assistantMsg)

	// Then, add the tool result as a tool message.
	toolResultMsg := ollamaMessage{
		Role:    "tool",
		Content: r.Content,
	}
	s.messages = append(s.messages, toolResultMsg)

	// Build the request body for the follow-up call.
	reqBody := ollamaChatRequest{
		Model:    s.model,
		Stream:   false,
		Messages: s.messages,
	}

	// Add tools if available.
	if len(s.tools) > 0 {
		reqBody.Tools = make([]ollamaTool, len(s.tools))
		for i, tool := range s.tools {
			reqBody.Tools[i] = ollamaTool{
				Type: "function",
				Function: ollamaFunction{
					Name:        tool.Name,
					Description: tool.Description,
					Parameters:  tool.Parameters,
				},
			}
		}
	}

	// Marshal the request.
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal chat request: %w", err)
	}

	// Create the HTTP request.
	url := s.backend.baseURL + "/api/chat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the request.
	resp, err := s.backend.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("chat request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check for HTTP errors.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("chat request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	// Parse the response.
	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return fmt.Errorf("decode chat response: %w", err)
	}

	// Update the stream's response for the next Recv calls.
	s.response = chatResp

	// Update lastAssistantMsg so subsequent tool calls use the correct message.
	s.lastAssistantMsg = chatResp.Message

	// Reset the event emission state.
	s.sentText = false
	s.sentTools = false
	s.sentEnd = false
	s.toolIndex = 0

	return nil
}

// Close releases the stream resources.
func (s *ollamaTurnStream) Close() error {
	return nil
}
