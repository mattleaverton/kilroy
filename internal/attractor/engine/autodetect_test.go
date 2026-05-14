// Unit tests for provider auto-detection from environment.

package engine

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func clearAutodetectEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY_KILROY",
		"OPENAI_API_KEY", "OPENAI_API_KEY_KILROY",
		"GOOGLE_API_KEY", "GOOGLE_API_KEY_KILROY",
		"GEMINI_API_KEY", "GEMINI_API_KEY_KILROY",
		"GOOGLE_GENERATIVE_AI_API_KEY",
		"KIMI_API_KEY", "KIMI_API_KEY_KILROY",
		"ZAI_API_KEY", "ZAI_API_KEY_KILROY",
		"CEREBRAS_API_KEY", "CEREBRAS_API_KEY_KILROY",
		"MINIMAX_API_KEY", "MINIMAX_API_KEY_KILROY",
		"INCEPTION_API_KEY", "INCEPTION_API_KEY_KILROY",
	} {
		t.Setenv(name, "")
	}
}

func TestDetectProviders_AnthropicAPIKey(t *testing.T) {
	clearAutodetectEnv(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test-123")

	// No CLI binary on path — should fall back to API backend.
	detected := detectProvidersWithLookPath(func(name string) (string, error) {
		return "", fmt.Errorf("not found: %s", name)
	})
	var found *DetectedProvider
	for i := range detected {
		if detected[i].Key == "anthropic" {
			found = &detected[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected anthropic provider to be detected")
	}
	if found.Backend != BackendAPI {
		t.Fatalf("expected api backend, got %q", found.Backend)
	}
}

func TestDetectProviders_CLIPreferred(t *testing.T) {
	clearAutodetectEnv(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test-123")

	// Simulate claude binary on path.
	detected := detectProvidersWithLookPath(func(name string) (string, error) {
		if name == "claude" {
			return "/usr/bin/claude", nil
		}
		return "", fmt.Errorf("not found: %s", name)
	})
	var found *DetectedProvider
	for i := range detected {
		if detected[i].Key == "anthropic" {
			found = &detected[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected anthropic provider to be detected")
	}
	if found.Backend != BackendCLI {
		t.Fatalf("expected cli backend when binary found, got %q", found.Backend)
	}
}

func TestDetectProviders_GoogleFallbackKey(t *testing.T) {
	clearAutodetectEnv(t)
	t.Setenv("GOOGLE_API_KEY", "goog-test-123")

	detected := detectProvidersWithLookPath(func(name string) (string, error) {
		return "", fmt.Errorf("not found: %s", name)
	})
	var found *DetectedProvider
	for i := range detected {
		if detected[i].Key == "google" {
			found = &detected[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected google provider to be detected via GOOGLE_API_KEY")
	}
}

func TestDetectProviders_KilroySuffixedKey(t *testing.T) {
	clearAutodetectEnv(t)
	t.Setenv("KIMI_API_KEY_KILROY", "kimi-test-123")

	detected := detectProvidersWithLookPath(func(name string) (string, error) {
		return "", fmt.Errorf("not found: %s", name)
	})
	var found *DetectedProvider
	for i := range detected {
		if detected[i].Key == "kimi" {
			found = &detected[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected kimi provider to be detected via KIMI_API_KEY_KILROY")
	}
	if found.Backend != BackendAPI {
		t.Fatalf("expected api backend, got %q", found.Backend)
	}
}

func TestDetectProviders_NoKeysNoResults(t *testing.T) {
	clearAutodetectEnv(t)

	detected := detectProvidersWithLookPath(func(name string) (string, error) {
		return "", fmt.Errorf("not found: %s", name)
	})
	if len(detected) != 0 {
		keys := make([]string, 0, len(detected))
		for _, d := range detected {
			keys = append(keys, d.Key)
		}
		sort.Strings(keys)
		t.Fatalf("expected no providers, got: %s", strings.Join(keys, ", "))
	}
}

func TestApplyDetectedProviders_DoesNotOverwrite(t *testing.T) {
	cfg := &RunConfigFile{}
	cfg.LLM.Providers = map[string]ProviderConfig{
		"anthropic": {Backend: BackendCLI},
	}
	ApplyDetectedProviders(cfg, []DetectedProvider{
		{Key: "anthropic", Backend: BackendAPI},
		{Key: "openai", Backend: BackendAPI},
	})
	if cfg.LLM.Providers["anthropic"].Backend != BackendCLI {
		t.Fatal("existing provider config should not be overwritten")
	}
	if _, ok := cfg.LLM.Providers["openai"]; !ok {
		t.Fatal("new provider should be added")
	}
}
