// Auto-detect available LLM providers from the environment.
// Scans for API keys and CLI binaries to populate provider config.

package engine

import (
	"os"
	"os/exec"
	"strings"

	"github.com/danshapiro/kilroy/internal/providerspec"
)

// DetectedProvider describes a provider found via environment scanning.
type DetectedProvider struct {
	Key     string
	Backend BackendKind
	APIKey  string
}

// DetectProviders scans the environment for known API keys and returns
// provider configurations for each detected provider. For providers with
// a CLI spec, the CLI backend is preferred when the binary is on PATH.
func DetectProviders() []DetectedProvider {
	return detectProvidersWithLookPath(exec.LookPath)
}

func detectProvidersWithLookPath(lookPath func(string) (string, error)) []DetectedProvider {
	var detected []DetectedProvider
	for key, spec := range providerspec.Builtins() {
		if spec.API == nil || spec.API.DefaultAPIKeyEnv == "" {
			continue
		}
		apiKey := firstSetEnv(apiKeyEnvCandidates(key, spec)...)
		if apiKey == "" {
			continue
		}
		backend := BackendAPI
		if spec.CLI != nil {
			if _, err := lookPath(spec.CLI.DefaultExecutable); err == nil {
				backend = BackendCLI
			}
		}
		detected = append(detected, DetectedProvider{
			Key:     key,
			Backend: backend,
			APIKey:  apiKey,
		})
	}
	return detected
}

func firstSetEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func apiKeyEnvCandidates(key string, spec providerspec.Spec) []string {
	var candidates []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		for _, existing := range candidates {
			if existing == name {
				return
			}
		}
		candidates = append(candidates, name)
	}

	defaultEnv := ""
	if spec.API != nil {
		defaultEnv = spec.API.DefaultAPIKeyEnv
	}
	if strings.HasSuffix(defaultEnv, "_API_KEY") {
		add(strings.TrimSuffix(defaultEnv, "_API_KEY") + "_API_KEY_KILROY")
	}
	add(defaultEnv)

	// Google API credentials are accepted under multiple names across SDKs and
	// CLIs; keep auto-detection aligned with the auth template's budget-first
	// ordering.
	if key == "google" {
		add("GOOGLE_API_KEY_KILROY")
		add("GEMINI_API_KEY_KILROY")
		add("GOOGLE_API_KEY")
		add("GEMINI_API_KEY")
		add("GOOGLE_GENERATIVE_AI_API_KEY")
	}
	return candidates
}

// ApplyDetectedProviders populates cfg.LLM.Providers from auto-detected
// providers. Only providers not already configured are added.
func ApplyDetectedProviders(cfg *RunConfigFile, detected []DetectedProvider) {
	if cfg.LLM.Providers == nil {
		cfg.LLM.Providers = map[string]ProviderConfig{}
	}
	for _, dp := range detected {
		if _, exists := cfg.LLM.Providers[dp.Key]; exists {
			continue
		}
		cfg.LLM.Providers[dp.Key] = ProviderConfig{
			Backend: dp.Backend,
		}
	}
}
