// OpenCode CLI invocation template.
package templates

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
	"github.com/danshapiro/kilroy/internal/providerspec"
)

// OpenCode returns an invocation template for the opencode CLI.
//
// opencode is multi-provider: it picks anthropic / kimi / zai / etc.
// based on its model arg ("anthropic/claude-...", "kimi/kimi-k2", etc.)
// and the provider config block embedded via OPENCODE_CONFIG_CONTENT.
// The model prefix comes from the resolved AgentRoute.Provider, which
// tmux_handler passes through to BuildArgs as the `provider` parameter.
// PrepareSession reads it from KILROY_AGENT_PROVIDER in the child env
// map (set by tmux_handler) for the OPENCODE_CONFIG_CONTENT block.
func OpenCode() Template {
	return Template{
		Name:       "opencode",
		Binary:     "opencode",
		LogLocator: &agentlog.OpenCodeLogLocator{},
		BuildArgs: func(prompt, workDir, model, _, provider string) []string {
			args := []string{"run", "--format", "json", "--pure"}
			if model != "" {
				// opencode takes provider/model (e.g. "anthropic/claude-sonnet-4-5",
				// "kimi/kimi-k2"). Use the resolved provider for the prefix
				// when set. Otherwise default to "anthropic/" so legacy
				// fixtures that don't pass a provider continue to work. Dots
				// normalize to dashes — opencode's model registry uses dashes.
				m := strings.ReplaceAll(model, ".", "-")
				if !strings.Contains(m, "/") {
					p := strings.TrimSpace(provider)
					if p == "" {
						p = "anthropic"
					}
					m = p + "/" + m
				}
				args = append(args, "--model", m)
			}
			if workDir != "" {
				args = append(args, "--dir", workDir)
			}
			args = append(args, prompt)
			return args
		},
		// PARTIAL: opencode is still outside the full auth binder model
		// (separate credential DB at ~/.local/share/opencode/opencode.db,
		// no env-scrub like claude/codex), but the budget-isolation
		// convention is respected: BuildEnv prefers <NAME>_KILROY over
		// the canonical <NAME> for each known provider. The exported env
		// var is always the canonical name because OPENCODE_CONFIG_CONTENT
		// references {env:CANONICAL_NAME}; only the *value* comes from the
		// _KILROY variant when present.
		//
		// Tracking: full opencode binder integration is a follow-up; design
		// is in plan §11 / docs/auth.md "What's NOT covered".
		BuildEnv: func() map[string]string {
			env := map[string]string{}
			// For each known provider key, prefer the _KILROY-suffixed
			// variant (kilroy budget-isolation convention from auth.toml)
			// and fall back to the canonical name. The map key is always
			// the canonical name — opencode's `{env:NAME}` substitution
			// reads canonical env names from its own config block.
			for _, name := range []string{
				"ANTHROPIC_API_KEY",
				"OPENAI_API_KEY",
				"GEMINI_API_KEY",
				"KIMI_API_KEY",
				"ZAI_API_KEY",
				"CEREBRAS_API_KEY",
				"MINIMAX_API_KEY",
				"INCEPTION_API_KEY",
			} {
				if val := os.Getenv(name + "_KILROY"); val != "" {
					env[name] = val
					continue
				}
				if val := os.Getenv(name); val != "" {
					env[name] = val
				}
			}
			return env
		},
		PrepareSession: func(stageDir string, env map[string]string) error {
			// Build OPENCODE_CONFIG_CONTENT for whichever provider the
			// route resolved to. tmux_handler writes route.Provider into
			// KILROY_AGENT_PROVIDER before this runs. Default is
			// anthropic for back-compat with fixtures that don't go
			// through ResolveAgentRoute.
			provider := strings.TrimSpace(env["KILROY_AGENT_PROVIDER"])
			if provider == "" {
				provider = "anthropic"
			}
			env["OPENCODE_CONFIG_CONTENT"] = buildOpencodeConfig(provider)
			return nil
		},
		PromptPrefix:     ">",
		BusyIndicators:   []string{},
		ProcessNames:     []string{"opencode"},
		StructuredOutput: true,
		ExitsOnComplete:  true,
		StartupTimeout:   15 * time.Second,
	}
}

// buildOpencodeConfig produces opencode's provider configuration JSON
// for one provider. Looks up the provider's API spec in providerspec to
// pick the correct API key env var and base URL. For unknown providers,
// emits a minimal config with the canonical API key env var pattern.
func buildOpencodeConfig(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))

	options := map[string]any{}

	if spec, ok := providerspec.Builtin(provider); ok && spec.API != nil {
		if env := strings.TrimSpace(spec.API.DefaultAPIKeyEnv); env != "" {
			options["apiKey"] = "{env:" + env + "}"
		}
		if base := strings.TrimSpace(spec.API.DefaultBaseURL); base != "" {
			options["baseURL"] = base
		}
	} else {
		// Unknown provider: best-effort canonical env var name.
		options["apiKey"] = "{env:" + strings.ToUpper(provider) + "_API_KEY}"
	}

	cfg := map[string]any{
		"provider": map[string]any{
			provider: map[string]any{
				"options": options,
			},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		// Should never fail for plain map[string]any. Fall back to a
		// minimal hardcoded anthropic block so we don't return a malformed
		// config.
		return `{"provider":{"anthropic":{"options":{"apiKey":"{env:ANTHROPIC_API_KEY}"}}}}`
	}
	return string(data)
}
