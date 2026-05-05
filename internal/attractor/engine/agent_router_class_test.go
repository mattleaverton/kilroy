package engine

import (
	"errors"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// makeTestPolicy builds a minimal *policy.Data with a single "hard_coding" class
// containing one anthropic_sdk candidate guarded by api_key auth.
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
						Requires:    binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
					},
				},
			},
		},
	}
}

// authResolverFactory returns a fixture binding.Resolver factory whose detection
// view marks ANTHROPIC_API_KEY as present, so the test policy candidate
// resolves cleanly.
func authResolverFactory() func(string) (*binding.Resolver, error) {
	cfg := &binding.Config{
		Bindings: map[string]string{"anthropic/api_key": "anthropic_api_key"},
		Chains: map[string]binding.Chain{
			"anthropic_api_key": {
				Name:     "anthropic_api_key",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY"}},
			},
		},
	}
	view := testDetectionView{envs: map[string]bool{"ANTHROPIC_API_KEY": true}}
	r := binding.NewResolver(cfg, view)
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// testDetectionView is a binding.DetectionView for engine tests.
type testDetectionView struct {
	envs map[string]bool
	clis map[string]bool
}

func (v testDetectionView) EnvVarPresent(name string) bool { return v.envs[name] }
func (v testDetectionView) CLISessionOK(tool string) bool  { return v.clis[tool] }

// makeTestRouter builds an AgentRouter with injected policy loader and resolver factory.
func makeTestRouter(data *policy.Data, factory func(string) (*binding.Resolver, error)) *AgentRouter {
	return &AgentRouter{
		policyLoad:     func() (*policy.Data, error) { return data, nil },
		policyResolver: factory,
	}
}

// TestAgentRouter_ClassAttribute_OverridesStylesheet verifies that when a node
// carries class="hard_coding", the policy resolver overrides the bogus
// llm_provider/llm_model stylesheet attributes.
func TestAgentRouter_ClassAttribute_OverridesStylesheet(t *testing.T) {
	router := makeTestRouter(makeTestPolicy(), authResolverFactory())

	node := model.NewNode("test-node")
	node.Attrs["agent_class"] = "hard_coding"
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

// TestAgentRouter_AgentClass_UnknownClass_Errors verifies that an
// unknown agent_class= name fails loudly (typed policy.ErrUnknownClass).
// agent_class is the policy-routing attribute and must be strict — typos
// are caught at validation time, not silently fallen through. (CSS-style
// stylesheet selectors live on the unrelated `class=` attribute.)
func TestAgentRouter_AgentClass_UnknownClass_Errors(t *testing.T) {
	router := makeTestRouter(makeTestPolicy(), authResolverFactory())

	node := model.NewNode("test-node")
	node.Attrs["agent_class"] = "totally_made_up"

	_, _, _, _, err := router.resolveNodeRoute(node, nil)
	if err == nil {
		t.Fatal("expected error for unknown agent_class, got nil")
	}
	var unknownErr policy.ErrUnknownClass
	if !errors.As(err, &unknownErr) {
		t.Errorf("error %v does not wrap policy.ErrUnknownClass", err)
	}
}

// TestAgentRouter_NoClass_FallsBackToStylesheet verifies that when no class is
// set, the router uses llm_provider/llm_model from node attributes.
func TestAgentRouter_NoClass_FallsBackToStylesheet(t *testing.T) {
	runtimes := map[string]ProviderRuntime{
		"anthropic": {Key: "anthropic", Backend: BackendAPI},
	}
	router := NewAgentRouterWithRuntimes(nil, runtimes)

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

func TestAgentRouter_DispatcherResolvedSDKRoute_NotDowngradedByCfg(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"anthropic": {Backend: BackendCLI},
	}
	runtimes := map[string]ProviderRuntime{
		"anthropic": {Key: "anthropic", Backend: BackendCLI},
	}
	router := NewAgentRouterWithRuntimes(cfg, runtimes)

	node := model.NewNode("sdk-node")
	node.Attrs["llm_provider"] = "anthropic"
	node.Attrs["llm_model"] = "claude-test"

	resolved, err := ResolveAgentRoute(node, nil, PolicyDeps{})
	if err != nil {
		t.Fatalf("ResolveAgentRoute: %v", err)
	}
	prov, mdl, backend, source, err := func() (string, string, BackendKind, string, error) {
		route, err := router.nodeRouteFromAgentRoute(node, resolved, true)
		if err != nil {
			return "", "", "", "", err
		}
		return route.provider, route.model, route.backend, route.source, nil
	}()
	if err != nil {
		t.Fatalf("resolveNodeRouteForRun: unexpected error: %v", err)
	}

	if prov != "anthropic" {
		t.Errorf("provider = %q, want %q", prov, "anthropic")
	}
	if mdl != "claude-test" {
		t.Errorf("model = %q, want %q", mdl, "claude-test")
	}
	if backend != BackendAPI {
		t.Errorf("backend = %q, want %q; dispatcher-resolved SDK routes must not be downgraded by cfg defaults", backend, BackendAPI)
	}
	if source != "llm_provider=anthropic" {
		t.Errorf("source = %q, want %q", source, "llm_provider=anthropic")
	}
}

// TestProviderAndBackendForDriver_Table is a table-driven test of the
// driver→(provider, BackendKind) mapping covering all 6 known drivers plus
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
		{"gemini_cli", "google", BackendCLI},
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
