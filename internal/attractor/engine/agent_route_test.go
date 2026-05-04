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
	"github.com/danshapiro/kilroy/internal/providerspec"
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

func TestResolveAgentRoute_ProviderRuntimeCLI_FirstClassRoute(t *testing.T) {
	node := &model.Node{
		ID: "implement",
		Attrs: map[string]string{
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{
		ProviderRuntimes: map[string]ProviderRuntime{
			"openai": {Key: "openai", Backend: BackendCLI},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "codex_cli" {
		t.Fatalf("driver: got %q want codex_cli", r.Driver)
	}
	if r.Backend != BackendCLI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendCLI)
	}
	if r.Provider != "openai" {
		t.Fatalf("provider: got %q want openai", r.Provider)
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

// Built-in OpenAI-compatible providers are first-class API routes. They must
// not resolve to Driver="" and leave dispatch to reinterpret the provider.
func TestResolveAgentRoute_OpenAICompatProvider_FirstClassRoute(t *testing.T) {
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
	if r.Driver != "openai_compat_api" {
		t.Fatalf("driver: got %q want openai_compat_api", r.Driver)
	}
	if r.Provider != "kimi" {
		t.Fatalf("provider field should preserve raw name, got %q", r.Provider)
	}
	if r.Backend != BackendAPI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendAPI)
	}
	if r.Source != "llm_provider=kimi" {
		t.Fatalf("source: got %q", r.Source)
	}
}

func TestResolveAgentRoute_AdHocProviderWithoutSpec_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID: "adhoc",
		Attrs: map[string]string{
			"llm_provider": "local-openai-compatible",
			"llm_model":    "local-model",
		},
	}
	_, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err == nil {
		t.Fatalf("expected ad-hoc provider without a loaded provider runtime/spec to fail")
	}
	for _, want := range []string{"local-openai-compatible", "no executable route", "provider runtime"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got %q", want, err.Error())
		}
	}
}

func TestResolveAgentRoute_ConfiguredOpenAICompatProvider_FirstClassRoute(t *testing.T) {
	node := &model.Node{
		ID: "adhoc",
		Attrs: map[string]string{
			"llm_provider": "local-openai-compatible",
			"llm_model":    "local-model",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{
		ProviderRuntimes: map[string]ProviderRuntime{
			"local-openai-compatible": {
				Key:     "local-openai-compatible",
				Backend: BackendAPI,
				API: providerspec.APISpec{
					Protocol: providerspec.ProtocolOpenAIChatCompletions,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("configured OpenAI-compatible provider should resolve: %v", err)
	}
	if r.Driver != "openai_compat_api" {
		t.Fatalf("driver: got %q want openai_compat_api", r.Driver)
	}
	if r.Backend != BackendAPI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendAPI)
	}
	if r.Provider != "local-openai-compatible" {
		t.Fatalf("provider = %q, want local-openai-compatible", r.Provider)
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

// agent_tool=claude + matching llm_provider=anthropic resolves cleanly.
// The metadata says the same thing the binary does.
func TestResolveAgentRoute_AgentToolWithMatchingLLMProvider(t *testing.T) {
	node := &model.Node{
		ID: "mixed",
		Attrs: map[string]string{
			"agent_tool":   "claude",
			"llm_provider": "anthropic",
			"llm_model":    "claude-sonnet-4-5",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "claude_cli" {
		t.Fatalf("driver: got %q want claude_cli", r.Driver)
	}
}

// agent_tool=claude + mismatched llm_provider=openai is a loud failure.
// Without this check, route metadata says one provider while the binary
// uses another — the silent-wrong-mapping class of bug.
func TestResolveAgentRoute_FixedProviderMismatch_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID: "mismatch",
		Attrs: map[string]string{
			"agent_tool":   "claude",
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	_, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err == nil {
		t.Fatalf("expected loud failure on agent_tool/llm_provider mismatch")
	}
	for _, want := range []string{"claude", "anthropic", "openai", "conflicts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got %q", want, err.Error())
		}
	}
}

// agent_tool=opencode is multi-provider — the node MUST set
// llm_provider= to disambiguate which provider opencode talks to.
func TestResolveAgentRoute_Opencode_RequiresExplicitProvider(t *testing.T) {
	node := &model.Node{
		ID:    "oc",
		Attrs: map[string]string{"agent_tool": "opencode"},
	}
	_, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err == nil {
		t.Fatalf("expected error: opencode without llm_provider should fail")
	}
	for _, want := range []string{"opencode", "llm_provider", "multi-provider"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got %q", want, err.Error())
		}
	}
}

// agent_tool=opencode + llm_provider=kimi resolves to a route whose
// Provider is the user-chosen one (not driver-implied). Driver=opencode,
// Backend=BackendCLI.
func TestResolveAgentRoute_Opencode_AcceptsKimi(t *testing.T) {
	node := &model.Node{
		ID: "oc_kimi",
		Attrs: map[string]string{
			"agent_tool":   "opencode",
			"llm_provider": "kimi",
			"llm_model":    "kimi-k2",
		},
	}
	r, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Driver != "opencode" {
		t.Fatalf("driver: got %q want opencode", r.Driver)
	}
	if r.Provider != "kimi" {
		t.Fatalf("provider: got %q want kimi", r.Provider)
	}
	if r.Backend != BackendCLI {
		t.Fatalf("backend: got %q want %q", r.Backend, BackendCLI)
	}
	if r.Source != "agent_tool=opencode" {
		t.Fatalf("source: got %q", r.Source)
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
