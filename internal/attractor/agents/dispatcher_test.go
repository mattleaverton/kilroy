// Acceptance tests for the unified Dispatcher. Verifies that DOT/policy
// intent — not a CLI flag — determines which path runs.
package agents

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
)

// dispatchPathForDriver maps the canonical CLI drivers to dispatchCLI
// and the canonical SDK drivers to dispatchAPI. Anything else is
// dispatchUnknown (deterministic failure at Execute time).
func TestDispatchPathForDriver_Mapping(t *testing.T) {
	cliDrivers := []string{"claude_cli", "codex_cli", "gemini_cli", "opencode"}
	for _, d := range cliDrivers {
		t.Run("cli/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchCLI {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchCLI(%d)", d, got, dispatchCLI)
			}
		})
	}

	sdkDrivers := []string{"anthropic_sdk", "openai_sdk", "google_sdk"}
	for _, d := range sdkDrivers {
		t.Run("api/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchAPI {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchAPI(%d)", d, got, dispatchAPI)
			}
		})
	}

	unknown := []string{"", "claude", "openai", "unknown_driver", "claude_sdk"}
	for _, d := range unknown {
		t.Run("unknown/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchUnknown {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchUnknown(%d)", d, got, dispatchUnknown)
			}
		})
	}
}

// driverForAgentTool maps every documented agent_tool= value to its
// CLI driver. Unknowns return "" so the caller surfaces a clean error.
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
		// Case-insensitive.
		{"CLAUDE", "claude_cli", false},
		{"  Codex  ", "codex_cli", false},
		// Unknown.
		{"", "", true},
		{"foo", "", true},
		{"claude_cli", "", true}, // already-resolved name doesn't match the legacy tool list
	}
	for _, c := range cases {
		t.Run(c.tool, func(t *testing.T) {
			got := driverForAgentTool(c.tool)
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

// driverForSDKProvider maps llm_provider= to its SDK driver.
// "google" and "gemini" both alias to google_sdk.
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
			got := driverForSDKProvider(in)
			if got != want {
				t.Fatalf("llm_provider=%q: got %q, want %q", in, got, want)
			}
		})
	}
}

// resolveDriverForDispatch falls back to agent_tool= when no
// agent_class is set. This is the legacy stylesheet path that
// pre-policy-class graphs use (e.g. multi-tool-exercise).
func TestResolveDriverForDispatch_AgentTool_RoutesToCLI(t *testing.T) {
	node := &model.Node{
		ID:    "claude_write",
		Attrs: map[string]string{"agent_tool": "claude"},
	}
	driver, source, err := resolveDriverForDispatch(node, nil, nopPolicyDeps())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if driver != "claude_cli" {
		t.Fatalf("driver: got %q want %q", driver, "claude_cli")
	}
	if source != "agent_tool=claude" {
		t.Fatalf("source: got %q", source)
	}
	if dispatchPathForDriver(driver) != dispatchCLI {
		t.Fatalf("driver %q should dispatch to CLI path", driver)
	}
}

// When neither agent_class nor agent_tool is set but llm_provider+
// llm_model are present, the dispatcher resolves to the SDK driver.
func TestResolveDriverForDispatch_LLMProvider_RoutesToAPI(t *testing.T) {
	node := &model.Node{
		ID: "implement",
		Attrs: map[string]string{
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	driver, source, err := resolveDriverForDispatch(node, nil, nopPolicyDeps())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if driver != "openai_sdk" {
		t.Fatalf("driver: got %q want %q", driver, "openai_sdk")
	}
	if source != "llm_provider=openai" {
		t.Fatalf("source: got %q", source)
	}
	if dispatchPathForDriver(driver) != dispatchAPI {
		t.Fatalf("driver %q should dispatch to API path", driver)
	}
}

// A node with no class, no agent_tool, and no llm_provider+llm_model
// has no resolvable driver — the dispatcher fails loudly rather than
// guessing.
func TestResolveDriverForDispatch_VagueNode_FailsLoudly(t *testing.T) {
	node := &model.Node{ID: "vague", Attrs: map[string]string{}}
	driver, _, err := resolveDriverForDispatch(node, nil, nopPolicyDeps())
	if err == nil {
		t.Fatalf("expected error for vague node, got driver %q", driver)
	}
}

// agent_tool= with an unknown value also fails loudly — no silent
// fallback to "claude_cli" or any default.
func TestResolveDriverForDispatch_UnknownAgentTool_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID:    "weird",
		Attrs: map[string]string{"agent_tool": "made-up-tool"},
	}
	if _, _, err := resolveDriverForDispatch(node, nil, nopPolicyDeps()); err == nil {
		t.Fatalf("expected error for unknown agent_tool")
	}
}

// llm_provider= with an unknown value also fails loudly.
func TestResolveDriverForDispatch_UnknownLLMProvider_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID: "weird",
		Attrs: map[string]string{
			"llm_provider": "made-up-provider",
			"llm_model":    "x",
		},
	}
	if _, _, err := resolveDriverForDispatch(node, nil, nopPolicyDeps()); err == nil {
		t.Fatalf("expected error for unknown llm_provider")
	}
}

// llm_provider= without llm_model is treated as vague (the model is
// part of the route; pinning a provider with no model isn't a
// complete decision).
func TestResolveDriverForDispatch_LLMProviderWithoutModel_FailsLoudly(t *testing.T) {
	node := &model.Node{
		ID: "weird",
		Attrs: map[string]string{
			"llm_provider": "openai",
			// no llm_model
		},
	}
	if _, _, err := resolveDriverForDispatch(node, nil, nopPolicyDeps()); err == nil {
		t.Fatalf("expected error for llm_provider without llm_model")
	}
}

// nopPolicyDeps returns engine.PolicyDeps with zero values. The fixtures
// above don't set agent_class= so the policy resolver path isn't
// exercised; zero deps suffice.
func nopPolicyDeps() engine.PolicyDeps { return engine.PolicyDeps{} }
