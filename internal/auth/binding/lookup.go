// lookup.go — env-only API key lookup for non-class API adapters.
// Mirrors the api_key chains in default_chains.toml so _KILROY-suffixed env
// vars take precedence; a drift test in lookup_test.go keeps the two in sync.

package binding

import (
	"os"
	"strings"
)

// APIKeyEnvOrder returns the env var names checked, in precedence order, for
// a given provider's api_key chain. The list mirrors default_chains.toml so
// the non-class adapters honor the same _KILROY-first precedence as the
// class-routed resolver.
func APIKeyEnvOrder(provider string) []string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "anthropic":
		return []string{"ANTHROPIC_API_KEY_KILROY", "ANTHROPIC_API_KEY"}
	case "openai":
		return []string{"OPENAI_API_KEY_KILROY", "OPENAI_API_KEY"}
	case "google":
		return []string{
			"GOOGLE_API_KEY_KILROY",
			"GEMINI_API_KEY_KILROY",
			"GOOGLE_API_KEY",
			"GEMINI_API_KEY",
			"GOOGLE_GENERATIVE_AI_API_KEY",
		}
	}
	return nil
}

// LookupAPIKeyEnv reads the api_key env vars for the named provider in
// precedence order and returns the first non-empty value plus the env var
// that supplied it. ok is false when no listed env var is set.
//
// Used by the non-class API adapters at process start. The class-routed
// path uses the full Resolver, which can also draw from user/project
// auth.toml chains and CLI sessions.
func LookupAPIKeyEnv(provider string) (value, source string, ok bool) {
	for _, name := range APIKeyEnvOrder(provider) {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v, name, true
		}
	}
	return "", "", false
}
