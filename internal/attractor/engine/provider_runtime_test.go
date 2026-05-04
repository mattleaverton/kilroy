package engine

import "testing"

func TestResolveProviderRuntimes_MergesBuiltinAndConfigOverrides(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"kimi": {
			Backend:  BackendAPI,
			Failover: []string{"zai"},
			API: ProviderAPIConfig{
				APIKeyEnv: "KIMI_API_KEY",
				Headers:   map[string]string{"X-Trace": "1"},
			},
		},
	}

	rt, err := resolveProviderRuntimes(cfg)
	if err != nil {
		t.Fatalf("resolveProviderRuntimes: %v", err)
	}
	// Kimi defaults to Moonshot's general-purpose API (api.moonshot.ai),
	// which speaks OpenAI-compatible chat completions. The kimi.com/coding
	// "Kimi Coding CLI" product (anthropic_messages) is reachable via
	// per-run config overrides — see kimi_zai_api_integration_test.go.
	if rt["kimi"].API.Protocol != "openai_chat_completions" {
		t.Fatalf("kimi protocol = %q, want openai_chat_completions", rt["kimi"].API.Protocol)
	}
	kimi, ok := rt["kimi"]
	if !ok {
		t.Fatalf("missing runtime for kimi")
	}
	if got := kimi.Failover; len(got) != 1 || got[0] != "zai" {
		t.Fatalf("kimi failover=%v want [zai]", got)
	}
	zai, ok := rt["zai"]
	if !ok {
		t.Fatalf("expected synthesized failover target runtime for zai")
	}
	if zai.API.DefaultPath != "/api/coding/paas/v4/chat/completions" {
		t.Fatalf("expected synthesized zai default path")
	}
	if len(zai.Failover) != 0 {
		t.Fatalf("zai synthesized failover=%v want [] (no implicit chained defaults)", zai.Failover)
	}
	if _, ok := rt["cerebras"]; ok {
		t.Fatalf("unexpected recursive synthesized failover target cerebras without explicit runfile chain")
	}
	if got := kimi.APIHeaders(); got["X-Trace"] != "1" {
		t.Fatalf("expected runtime headers copy, got %v", got)
	}
}

func TestResolveProviderRuntimes_ExplicitEmptyFailoverDisablesBuiltinFallback(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"zai": {
			Backend:  BackendAPI,
			Failover: []string{},
			API: ProviderAPIConfig{
				Protocol:  "openai_chat_completions",
				APIKeyEnv: "ZAI_API_KEY",
			},
		},
	}

	rt, err := resolveProviderRuntimes(cfg)
	if err != nil {
		t.Fatalf("resolveProviderRuntimes: %v", err)
	}
	if got := len(rt["zai"].Failover); got != 0 {
		t.Fatalf("zai failover len=%d want 0 for explicit empty override", got)
	}
	if !rt["zai"].FailoverExplicit {
		t.Fatalf("zai failover should be marked explicit")
	}
}

func TestResolveProviderRuntimes_OmittedFailoverHasNoFallback(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"zai": {
			Backend: BackendAPI,
			API: ProviderAPIConfig{
				Protocol:  "openai_chat_completions",
				APIKeyEnv: "ZAI_API_KEY",
			},
		},
	}

	rt, err := resolveProviderRuntimes(cfg)
	if err != nil {
		t.Fatalf("resolveProviderRuntimes: %v", err)
	}
	zai, ok := rt["zai"]
	if !ok {
		t.Fatalf("missing runtime for zai")
	}
	if len(zai.Failover) != 0 {
		t.Fatalf("zai failover=%v want []", zai.Failover)
	}
	if zai.FailoverExplicit {
		t.Fatalf("zai failover should not be marked explicit when omitted")
	}
	if _, ok := rt["cerebras"]; ok {
		t.Fatalf("unexpected synthesized failover target cerebras without explicit failover")
	}
}

func TestResolveProviderRuntimes_BuiltinFailoverIgnoredWhenOmitted(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendAPI},
	}

	rt, err := resolveProviderRuntimes(cfg)
	if err != nil {
		t.Fatalf("resolveProviderRuntimes: %v", err)
	}
	openai, ok := rt["openai"]
	if !ok {
		t.Fatalf("missing runtime for openai")
	}
	if len(openai.Failover) != 0 {
		t.Fatalf("openai failover=%v want [] when runfile omits failover", openai.Failover)
	}
	if _, ok := rt["google"]; ok {
		t.Fatalf("unexpected synthesized google runtime without explicit runfile failover")
	}
}

func TestResolveProviderRuntimes_RejectsCanonicalAliasCollisions(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"zai": {
			Backend: BackendAPI,
			API: ProviderAPIConfig{
				Protocol:  "openai_chat_completions",
				APIKeyEnv: "ZAI_API_KEY",
			},
		},
		"z-ai": {
			Backend: BackendAPI,
			API: ProviderAPIConfig{
				Protocol:  "openai_chat_completions",
				APIKeyEnv: "ZAI_API_KEY",
			},
		},
	}

	_, err := resolveProviderRuntimes(cfg)
	if err == nil {
		t.Fatalf("expected canonical collision error, got nil")
	}
	const want = `duplicate provider config after canonicalization: "z-ai" and "zai" both map to "zai"`
	if err.Error() != want {
		t.Fatalf("expected canonical collision error, got %v", err)
	}
}
