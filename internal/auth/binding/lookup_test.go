// Tests for lookup.go — verify _KILROY-first env precedence and guard against
// drift between the hardcoded order and the embedded default_chains.toml.

package binding

import (
	"reflect"
	"testing"
)

func TestAPIKeyEnvOrder_MatchesDefaultChains(t *testing.T) {
	cfg, err := LoadDefaultTemplates()
	if err != nil {
		t.Fatalf("LoadDefaultTemplates: %v", err)
	}

	cases := map[string]string{
		"anthropic": "anthropic_api_key",
		"openai":    "openai_api_key",
		"google":    "google_api_key",
	}

	for provider, chainName := range cases {
		t.Run(provider, func(t *testing.T) {
			chain, ok := cfg.Chains[chainName]
			if !ok {
				t.Fatalf("chain %q missing from defaults", chainName)
			}
			var want []string
			for _, src := range chain.Sources {
				if src.Kind != SourceEnvVar {
					continue
				}
				want = append(want, src.Name)
			}
			got := APIKeyEnvOrder(provider)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("APIKeyEnvOrder(%q) = %v, want %v (drift from default_chains.toml)", provider, got, want)
			}
		})
	}
}

func TestAPIKeyEnvOrder_UnknownProvider(t *testing.T) {
	if got := APIKeyEnvOrder("nope"); got != nil {
		t.Errorf("APIKeyEnvOrder(unknown) = %v, want nil", got)
	}
}

func TestLookupAPIKeyEnv_KilroyTakesPrecedence(t *testing.T) {
	cases := []struct {
		name        string
		provider    string
		set         map[string]string
		wantValue   string
		wantSource  string
		wantOK      bool
		clearOthers []string
	}{
		{
			name:       "anthropic kilroy beats canonical",
			provider:   "anthropic",
			set:        map[string]string{"ANTHROPIC_API_KEY_KILROY": "kilroy-key", "ANTHROPIC_API_KEY": "canonical-key"},
			wantValue:  "kilroy-key",
			wantSource: "ANTHROPIC_API_KEY_KILROY",
			wantOK:     true,
		},
		{
			name:       "anthropic falls back to canonical",
			provider:   "anthropic",
			set:        map[string]string{"ANTHROPIC_API_KEY": "canonical-key"},
			wantValue:  "canonical-key",
			wantSource: "ANTHROPIC_API_KEY",
			wantOK:     true,
		},
		{
			name:       "openai kilroy beats canonical",
			provider:   "openai",
			set:        map[string]string{"OPENAI_API_KEY_KILROY": "kilroy-key", "OPENAI_API_KEY": "canonical-key"},
			wantValue:  "kilroy-key",
			wantSource: "OPENAI_API_KEY_KILROY",
			wantOK:     true,
		},
		{
			name:       "openai falls back to canonical",
			provider:   "openai",
			set:        map[string]string{"OPENAI_API_KEY": "canonical-key"},
			wantValue:  "canonical-key",
			wantSource: "OPENAI_API_KEY",
			wantOK:     true,
		},
		{
			name:       "google GOOGLE_KILROY beats GEMINI_KILROY beats GOOGLE",
			provider:   "google",
			set:        map[string]string{"GOOGLE_API_KEY_KILROY": "g-kilroy", "GEMINI_API_KEY_KILROY": "gem-kilroy", "GOOGLE_API_KEY": "g-canonical"},
			wantValue:  "g-kilroy",
			wantSource: "GOOGLE_API_KEY_KILROY",
			wantOK:     true,
		},
		{
			name:       "google GEMINI_KILROY beats GOOGLE",
			provider:   "google",
			set:        map[string]string{"GEMINI_API_KEY_KILROY": "gem-kilroy", "GOOGLE_API_KEY": "g-canonical"},
			wantValue:  "gem-kilroy",
			wantSource: "GEMINI_API_KEY_KILROY",
			wantOK:     true,
		},
		{
			name:       "google GENERATIVE_AI is last resort",
			provider:   "google",
			set:        map[string]string{"GOOGLE_GENERATIVE_AI_API_KEY": "gen-ai"},
			wantValue:  "gen-ai",
			wantSource: "GOOGLE_GENERATIVE_AI_API_KEY",
			wantOK:     true,
		},
		{
			name:     "no env vars set returns ok=false",
			provider: "anthropic",
			set:      map[string]string{},
			wantOK:   false,
		},
		{
			name:       "whitespace-only env var skipped",
			provider:   "anthropic",
			set:        map[string]string{"ANTHROPIC_API_KEY_KILROY": "   ", "ANTHROPIC_API_KEY": "real-key"},
			wantValue:  "real-key",
			wantSource: "ANTHROPIC_API_KEY",
			wantOK:     true,
		},
		{
			name:     "unknown provider returns ok=false",
			provider: "minimax",
			set:      map[string]string{"MINIMAX_API_KEY": "x"},
			wantOK:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Unset every env var the helper might consult so leaks from a
			// developer shell don't pollute the result.
			for _, p := range []string{"anthropic", "openai", "google"} {
				for _, name := range APIKeyEnvOrder(p) {
					t.Setenv(name, "")
				}
			}
			for k, v := range tc.set {
				t.Setenv(k, v)
			}
			value, source, ok := LookupAPIKeyEnv(tc.provider)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if value != tc.wantValue {
				t.Errorf("value = %q, want %q", value, tc.wantValue)
			}
			if source != tc.wantSource {
				t.Errorf("source = %q, want %q", source, tc.wantSource)
			}
		})
	}
}
