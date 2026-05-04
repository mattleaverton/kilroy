// Tests for the unified agent-route resolver. ResolveAgentRoute is the
// single source of truth for what a node will dispatch to: class-resolved
// (policy chain), explicit agent_tool= (CLI driver), or llm_provider=+
// llm_model= (SDK driver). Vague nodes fail loudly here and at prelaunch
// — never silently in the handler layer.
package engine

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
)

func TestDriverForAgentTool(t *testing.T) {
	cases := []struct {
		tool    string
		want    string
		wantErr bool
	}{
		{"claude", "claude_cli", false},
		{"codex", "codex_cli", false},
		{"gemini", "gemini_cli", false},
		{"opencode", "opencode", false},
		{"CLAUDE", "claude_cli", false},
		{"  Codex  ", "codex_cli", false},
		{"", "", true},
		{"foo", "", true},
		{"claude_cli", "", true},
	}
	for _, c := range cases {
		t.Run(c.tool, func(t *testing.T) {
			got := DriverForAgentTool(c.tool)
			if c.wantErr {
				if got != "" {
					t.Fatalf("expected empty driver for %q, got %q", c.tool, got)
				}
				return
			}
			if got != c.want {
				t.Fatalf("agent_tool=%q: got driver %q, want %q", c.tool, got, c.want)
			}
		})
	}
}

func TestDriverForSDKProvider(t *testing.T) {
	cases := map[string]string{
		"anthropic": "anthropic_sdk",
		"openai":    "openai_sdk",
		"google":    "google_sdk",
		"gemini":    "google_sdk",
		"":          "",
		"foo":       "",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			got := DriverForSDKProvider(in)
			if got != want {
				t.Fatalf("llm_provider=%q: got %q, want %q", in, got, want)
			}
		})
	}
}

func TestResolveAgentRoute_AgentTool_CLI(t *testing.T) {
	node := &model.Node{
		ID:    "claude_write",
		Attrs: map[string]string{"agent_tool": "claude", "llm_model": "claude-sonnet-4-5"},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "claude_cli" {
		t.Fatalf("driver: got %q want %q", r.Driver, "claude_cli")
	}
	if r.Backend != BackendCLI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendCLI)
	}
	if r.Provider != "anthropic" {
		t.Fatalf("provider: got %q want %q", r.Provider, "anthropic")
	}
	if r.Model != "claude-sonnet-4-5" {
		t.Fatalf("model: got %q", r.Model)
	}
	if r.Source != "agent_tool=claude" {
		t.Fatalf("source: got %q", r.Source)
	}
	if !r.IsCLI() || r.IsAPI() {
		t.Fatalf("expected IsCLI=true IsAPI=false; got CLI=%v API=%v", r.IsCLI(), r.IsAPI())
	}
}

func TestResolveAgentRoute_LLMProvider_SDK(t *testing.T) {
	node := &model.Node{
		ID: "implement",
		Attrs: map[string]string{
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "openai_sdk" {
		t.Fatalf("driver: got %q want %q", r.Driver, "openai_sdk")
	}
	if r.Backend != BackendAPI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendAPI)
	}
	if r.Provider != "openai" {
		t.Fatalf("provider: got %q", r.Provider)
	}
	if r.Source != "llm_provider=openai" {
		t.Fatalf("source: got %q", r.Source)
	}
	if r.IsCLI() || !r.IsAPI() {
		t.Fatalf("expected IsCLI=false IsAPI=true; got CLI=%v API=%v", r.IsCLI(), r.IsAPI())
	}
}

func TestResolveAgentRoute_VagueNode_FailsLoudly(t *testing.T) {
	node := &model.Node{ID: "vague", Attrs: map[string]string{}}
	if _, err := ResolveAgentRoute(node, nil, PolicyDeps{}); err == nil {
		t.Fatalf("expected error for vague node")
	}
}

func TestResolveAgentRoute_UnknownAgentTool_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID:    "weird",
		Attrs: map[string]string{"agent_tool": "made-up-tool"},
	}
	_, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err == nil {
		t.Fatalf("expected error for unknown agent_tool")
	}
	if !strings.Contains(err.Error(), "made-up-tool") {
		t.Fatalf("error should name the unknown tool; got %q", err.Error())
	}
}

// Unknown llm_provider= names (kimi, zai, minimax, custom OpenAI-compat
// endpoints, etc.) are deferred to the runtime — they're routed via
// cfg.LLM.Providers in the legacy CodergenHandler path. ResolveAgentRoute
// returns an AgentRoute with Driver="" so the dispatcher can reject it
// while non-dispatcher paths can still resolve through run-config.
func TestResolveAgentRoute_UnknownLLMProvider_DefersToRuntime(t *testing.T) {
	node := &model.Node{
		ID: "weird",
		Attrs: map[string]string{
			"llm_provider": "kimi",
			"llm_model":    "kimi-k2.5",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unknown provider should not error at resolve time: %v", err)
	}
	if r.Driver != "" {
		t.Fatalf("unknown provider should yield empty driver (deferred), got %q", r.Driver)
	}
	if r.Provider != "kimi" {
		t.Fatalf("provider field should preserve raw name, got %q", r.Provider)
	}
	if r.Source != "llm_provider=kimi" {
		t.Fatalf("source: got %q", r.Source)
	}
}

func TestResolveAgentRoute_LLMProviderWithoutModel_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID: "weird",
		Attrs: map[string]string{
			"llm_provider": "openai",
		},
	}
	if _, err := ResolveAgentRoute(node, nil, PolicyDeps{}); err == nil {
		t.Fatalf("expected error for llm_provider without llm_model")
	}
}

// AgentTool wins when both agent_tool and llm_provider are present —
// the node is asking for a specific CLI tool, not just a provider.
func TestResolveAgentRoute_AgentToolWinsOverLLMProvider(t *testing.T) {
	node := &model.Node{
		ID: "mixed",
		Attrs: map[string]string{
			"agent_tool":   "claude",
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "claude_cli" {
		t.Fatalf("agent_tool should win: got driver %q want claude_cli", r.Driver)
	}
}

// AuthMethod and AuthSource convenience methods return "" when there's
// no ClassResult (non-class routes don't carry an auth snapshot).
func TestAgentRoute_AuthMethodSourceEmptyForNonClassRoute(t *testing.T) {
	r := AgentRoute{Driver: "openai_sdk"}
	if got := r.AuthMethod(); got != "" {
		t.Fatalf("AuthMethod: got %q want empty", got)
	}
	if got := r.AuthSource(); got != "" {
		t.Fatalf("AuthSource: got %q want empty", got)
	}
}
