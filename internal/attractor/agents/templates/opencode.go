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
				// "kimi/kimi-k2.5"). Use the resolved provider for the prefix
				// when set. Otherwise default to "anthropic/" so legacy fixtures
				// that don't pass a provider continue to work.
				//
				// Dot-to-dash normalization is anthropic-only: anthropic's
				// catalog stores model ids with dots (claude-sonnet-4.6) but
				// the Claude CLI / API expect dashes. Other providers (kimi,
				// zai, moonshot) ship model ids with literal dots in the name
				// (kimi-k2.5) — normalizing breaks them. Pass through verbatim
				// for non-anthropic providers.
				p := strings.TrimSpace(provider)
				if p == "" {
					p = "anthropic"
				}
				m := model
				if strings.EqualFold(p, "anthropic") {
					m = strings.ReplaceAll(m, ".", "-")
				}
				if !strings.Contains(m, "/") {
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
			// route resolved to. tmux_handler writes route.Provider /
			// route.Model into KILROY_AGENT_PROVIDER /
			// KILROY_AGENT_MODEL before this runs. Default is anthropic
			// for back-compat with fixtures that don't go through
			// ResolveAgentRoute.
			provider := strings.TrimSpace(env["KILROY_AGENT_PROVIDER"])
			if provider == "" {
				provider = "anthropic"
			}
			model := strings.TrimSpace(env["KILROY_AGENT_MODEL"])
			env["OPENCODE_CONFIG_CONTENT"] = buildOpencodeConfig(provider, model)
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
// for one provider+model pair.
//
// Two shapes:
//
//  1. Native opencode providers (anthropic, openai, google, etc.) — emit
//     a minimal block with options.apiKey + options.baseURL. opencode's
//     own provider registry knows the rest (npm package, model list,
//     etc.) so the minimal config is enough.
//
//  2. Custom providers kilroy adds via providerspec (kimi, zai, cerebras,
//     minimax, inception) — opencode does not know them natively, so we
//     emit the full custom-provider declaration: npm package (chosen from
//     protocol), name, options (baseURL with /v1 appended for
//     anthropic_messages, apiKey), and a `models` map declaring the model
//     being launched. Without `models`, opencode rejects the launch with
//     "ProviderModelNotFoundError" even when the provider config is
//     otherwise valid.
//
// The decider is `ProviderOptionsKey != Key`: when the spec says "this
// provider speaks <other>'s protocol," it's a compat-mode provider and
// needs the full declaration. Pure native specs (anthropic→anthropic,
// openai→openai, etc.) keep the minimal config.
//
// model="" still emits a config without a models block — the caller will
// fall back to opencode's defaults; useful for direct-use callers that
// don't pass a model.
func buildOpencodeConfig(provider, model string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	model = strings.TrimSpace(model)

	spec, ok := providerspec.Builtin(provider)
	if !ok || spec.API == nil {
		// Unknown provider: best-effort canonical-env-var minimal config.
		// Use the input name verbatim to surface typos at launch time.
		options := map[string]any{
			"apiKey": "{env:" + strings.ToUpper(provider) + "_API_KEY}",
		}
		return marshalOpencodeConfig(provider, options, "", "", model)
	}

	options := map[string]any{}
	if env := strings.TrimSpace(spec.API.DefaultAPIKeyEnv); env != "" {
		options["apiKey"] = "{env:" + env + "}"
	}
	// "Custom" = not a native opencode provider. Heuristic: the spec's
	// ProfileFamily disagrees with the provider key (e.g. kimi/zai/cerebras
	// all live under "openai" family). Native providers (anthropic, openai,
	// google) have ProfileFamily == provider name. Drives whether we emit
	// the full custom declaration (npm + name + models) or the minimal
	// options-only block opencode's registry can fill in itself.
	customNeeded := strings.TrimSpace(spec.API.ProfileFamily) != "" &&
		!strings.EqualFold(spec.API.ProfileFamily, provider)

	npm := ""
	if customNeeded {
		// opencode's @ai-sdk/<package> name is determined by the
		// underlying API protocol. Without npm, opencode reports
		// "ProviderModelNotFoundError" because it can't load the SDK.
		switch spec.API.Protocol {
		case providerspec.ProtocolAnthropicMessages:
			npm = "@ai-sdk/anthropic"
		case providerspec.ProtocolOpenAIChatCompletions:
			npm = "@ai-sdk/openai-compatible"
		case providerspec.ProtocolOpenAIResponses:
			npm = "@ai-sdk/openai"
		case providerspec.ProtocolGoogleGenerateContent:
			npm = "@ai-sdk/google"
		}
	}

	// baseURL: opencode's @ai-sdk packages all expect baseURL to be
	// "<host>/<path-prefix>" — they append the protocol-specific endpoint
	// suffix (/messages, /chat/completions, /responses, etc.). Compute
	// the prefix by stripping the suffix from DefaultPath. Examples:
	//   kimi anthropic_messages: path /v1/messages → prefix /v1
	//   zai openai_chat_completions: path /api/coding/paas/v4/chat/completions
	//                                → prefix /api/coding/paas/v4
	//   moonshot openai_chat_completions: path /v1/chat/completions → prefix /v1
	if base := strings.TrimSpace(spec.API.DefaultBaseURL); base != "" {
		options["baseURL"] = composeOpencodeBaseURL(base, spec.API.DefaultPath, spec.API.Protocol)
	}
	displayName := strings.ToTitle(provider[:1]) + provider[1:]
	return marshalOpencodeConfig(provider, options, npm, displayName, model)
}

// composeOpencodeBaseURL combines DefaultBaseURL with the path prefix
// portion of DefaultPath (everything before the protocol-specific
// endpoint suffix). The opencode @ai-sdk packages append the suffix
// themselves, so passing the full DefaultPath would produce
// double-suffixed URLs like "/v1/messages/messages".
func composeOpencodeBaseURL(base, path string, protocol providerspec.APIProtocol) string {
	host := strings.TrimRight(strings.TrimSpace(base), "/")
	prefix := strings.TrimSpace(path)
	if prefix == "" {
		return host
	}
	suffix := ""
	switch protocol {
	case providerspec.ProtocolAnthropicMessages:
		suffix = "/messages"
	case providerspec.ProtocolOpenAIChatCompletions:
		suffix = "/chat/completions"
	case providerspec.ProtocolOpenAIResponses:
		suffix = "/responses"
	}
	if suffix != "" && strings.HasSuffix(prefix, suffix) {
		prefix = strings.TrimSuffix(prefix, suffix)
	}
	if prefix == "" || prefix == "/" {
		return host
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return host + prefix
}

// marshalOpencodeConfig emits the JSON config block for one provider.
// When npm is empty, only `provider.<name>.options` is set (minimal
// shape for opencode-native providers). When npm is non-empty, the
// full custom-provider declaration is emitted (npm + name + options +
// models). model="" omits the models map.
func marshalOpencodeConfig(provider string, options map[string]any, npm, displayName, model string) string {
	providerBlock := map[string]any{
		"options": options,
	}
	if npm != "" {
		providerBlock["npm"] = npm
		if displayName != "" {
			providerBlock["name"] = displayName
		}
	}
	if model != "" {
		providerBlock["models"] = map[string]any{
			model: map[string]any{},
		}
	}

	cfg := map[string]any{
		"provider": map[string]any{
			provider: providerBlock,
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
