package engine

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/policy"
)

// makeTestPolicy builds a minimal *policy.Data with a single "hard_coding" class
// containing one anthropic_sdk candidate guarded by ANTHROPIC_API_KEY.
func makeTestPolicy() *policy.Data {
	return &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Description: "test class",
				Chain: []policy.Candidate{
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "anthropic_sdk",
						Transport:   "http",
						HistorySink: "jsonl_local",
						Auth: policy.AuthReq{
							Kind:   "env_var",
							EnvVar: "ANTHROPIC_API_KEY",
						},
					},
				},
			},
		},
	}
}

// makeTestState returns a MachineState that marks ANTHROPIC_API_KEY as present and OK.
func makeTestState() policy.MachineState {
	return policy.MachineState{
		Auth: auth.ListOutput{
			Entries: []auth.Entry{
				{
					ID:    "anthropic.env.ANTHROPIC_API_KEY",
					Kind:  auth.KindEnvVar,
					State: auth.StateOK,
					Source: auth.Source{
						EnvVar: "ANTHROPIC_API_KEY",
					},
				},
			},
		},
	}
}

// makeTestRouter builds an AgentRouter with injected policy loader and state collector.
func makeTestRouter(data *policy.Data, state policy.MachineState) *AgentRouter {
	return &AgentRouter{
		policyLoad:    func() (*policy.Data, error) { return data, nil },
		policyCollect: func() policy.MachineState { return state },
	}
}

// TestAgentRouter_ClassAttribute_OverridesStylesheet verifies that when a node
// carries class="hard_coding", the policy resolver overrides the bogus
// llm_provider/llm_model stylesheet attributes.
func TestAgentRouter_ClassAttribute_OverridesStylesheet(t *testing.T) {
	router := makeTestRouter(makeTestPolicy(), makeTestState())

	node := model.NewNode("test-node")
	node.Attrs["class"] = "hard_coding"
	// Set bogus stylesheet values to prove the override is real.
	node.Attrs["llm_provider"] = "bad"
	node.Attrs["llm_model"] = "bad-model"

	prov, mdl, backend, source, err := router.resolveNodeRoute(node, nil)
	if err != nil {
		t.Fatalf("resolveNodeRoute: unexpected error: %v", err)
	}

	if prov != "anthropic" {
		t.Errorf("provider = %q, want %q", prov, "anthropic")
	}
	if mdl != "claude-opus-4-7" {
		t.Errorf("model = %q, want %q", mdl, "claude-opus-4-7")
	}
	if backend != BackendAPI {
		t.Errorf("backend = %q, want %q", backend, BackendAPI)
	}
	if source != "policy_class:hard_coding" {
		t.Errorf("source = %q, want %q", source, "policy_class:hard_coding")
	}
}

// TestAgentRouter_ClassAttribute_UnknownClass_FallsThrough verifies that
// an unknown class name is treated as a stylesheet selector, not as a
// typo'd policy class. The resolver returns ok=false (no error) and the
// router falls through to legacy llm_provider/llm_model attrs. This is
// what makes pre-Step-4b graphs like coding-loop (which use
// class="implementer" purely as a stylesheet selector) keep working.
func TestAgentRouter_ClassAttribute_UnknownClass_FallsThrough(t *testing.T) {
	runtimes := map[string]ProviderRuntime{
		"anthropic": {Key: "anthropic", Backend: BackendAPI},
	}
	router := &AgentRouter{
		policyLoad:       func() (*policy.Data, error) { return makeTestPolicy(), nil },
		policyCollect:    func() policy.MachineState { return makeTestState() },
		providerRuntimes: runtimes,
	}

	node := model.NewNode("test-node")
	node.Attrs["class"] = "totally_made_up"
	// Stylesheet attrs are what the engine should fall through to.
	node.Attrs["llm_provider"] = "anthropic"
	node.Attrs["llm_model"] = "claude-sonnet-4-6"

	prov, mdl, backend, source, err := router.resolveNodeRoute(node, nil)
	if err != nil {
		t.Fatalf("expected fall-through (no error) for unknown class, got: %v", err)
	}
	if prov != "anthropic" {
		t.Errorf("provider = %q, want anthropic (fall-through to stylesheet)", prov)
	}
	if mdl != "claude-sonnet-4-6" {
		t.Errorf("model = %q, want claude-sonnet-4-6 (fall-through)", mdl)
	}
	if backend != BackendAPI {
		t.Errorf("backend = %q, want %q", backend, BackendAPI)
	}
	if source != "graph_attrs" {
		t.Errorf("source = %q, want graph_attrs (the fall-through marker)", source)
	}
}

// TestAgentRouter_NoClass_FallsBackToStylesheet verifies that when no class is
// set, the router uses llm_provider/llm_model from node attributes.
func TestAgentRouter_NoClass_FallsBackToStylesheet(t *testing.T) {
	runtimes := map[string]ProviderRuntime{
		"anthropic": {Key: "anthropic", Backend: BackendAPI},
	}
	router := NewAgentRouterWithRuntimes(nil, nil, runtimes)

	node := model.NewNode("test-node")
	node.Attrs["llm_provider"] = "anthropic"
	node.Attrs["llm_model"] = "claude-test-model"

	prov, mdl, backend, source, err := router.resolveNodeRoute(node, nil)
	if err != nil {
		t.Fatalf("resolveNodeRoute: unexpected error: %v", err)
	}

	if prov != "anthropic" {
		t.Errorf("provider = %q, want %q", prov, "anthropic")
	}
	if mdl != "claude-test-model" {
		t.Errorf("model = %q, want %q", mdl, "claude-test-model")
	}
	if backend != BackendAPI {
		t.Errorf("backend = %q, want %q", backend, BackendAPI)
	}
	if source != "graph_attrs" {
		t.Errorf("source = %q, want %q", source, "graph_attrs")
	}
}

// TestProviderAndBackendForDriver_Table is a table-driven test of the
// driver→(provider, BackendKind) mapping covering all 5 known drivers plus
// one unknown driver.
func TestProviderAndBackendForDriver_Table(t *testing.T) {
	tests := []struct {
		driver      string
		wantProv    string
		wantBackend BackendKind
	}{
		{"claude_cli", "anthropic", BackendCLI},
		{"anthropic_sdk", "anthropic", BackendAPI},
		{"codex_cli", "openai", BackendCLI},
		{"openai_sdk", "openai", BackendAPI},
		{"google_sdk", "google", BackendAPI},
		{"unknown_driver", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.driver, func(t *testing.T) {
			gotProv, gotBackend := providerAndBackendForDriver(tc.driver)
			if gotProv != tc.wantProv {
				t.Errorf("providerAndBackendForDriver(%q) provider = %q, want %q", tc.driver, gotProv, tc.wantProv)
			}
			if gotBackend != tc.wantBackend {
				t.Errorf("providerAndBackendForDriver(%q) backend = %q, want %q", tc.driver, gotBackend, tc.wantBackend)
			}
		})
	}
}
