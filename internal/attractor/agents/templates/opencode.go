// OpenCode CLI invocation template.
package templates

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
)

// OpenCode returns an invocation template for the opencode CLI.
func OpenCode() Template {
	return Template{
		Name:       "opencode",
		Binary:     "opencode",
		LogLocator: &agentlog.OpenCodeLogLocator{},
		BuildArgs: func(prompt, workDir, model, _ string) []string {
			args := []string{"run", "--format", "json", "--pure"}
			if model != "" {
				// opencode uses provider/model format (e.g. "anthropic/claude-sonnet-4-5").
				// Add "anthropic/" prefix if missing, normalize dots to dashes.
				m := strings.ReplaceAll(model, ".", "-")
				if !strings.Contains(m, "/") {
					m = "anthropic/" + m
				}
				args = append(args, "--model", m)
			}
			if workDir != "" {
				args = append(args, "--dir", workDir)
			}
			args = append(args, prompt)
			return args
		},
		// KNOWN GAP: opencode is OUTSIDE the binder model.
		//
		// opencode is a multi-provider tool with its own internal config DB
		// (~/.local/share/opencode/opencode.db) and its own credential
		// management — it doesn't fit the binder pattern (one driver, one
		// chain) cleanly. For now, BuildEnv passes through provider env vars
		// (canonical names only) so opencode's existing `{env:NAME}` config
		// substitution keeps working.
		//
		// Concrete consequence: opencode runs do NOT honor the kilroy auth
		// chain — _KILROY-suffixed keys are not preferred, and the
		// claude/codex env-scrub patterns don't apply. If you route
		// opencode through a class, the resolver's auth choice is recorded
		// in resolution.json but opencode uses whatever canonical env vars
		// happen to be set in the launcher.
		//
		// Tracking: full opencode binder integration is a follow-up; design
		// is in plan §11 / docs/auth.md "What's NOT covered".
		BuildEnv: func() map[string]string {
			env := map[string]string{}
			if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
				env["ANTHROPIC_API_KEY"] = key
			}
			if key := os.Getenv("OPENAI_API_KEY"); key != "" {
				env["OPENAI_API_KEY"] = key
			}
			return env
		},
		PrepareSession: func(stageDir string, env map[string]string) error {
			// Inject provider config via OPENCODE_CONFIG_CONTENT so opencode
			// doesn't rely on ~/.config/opencode/ or interactive auth.
			config := map[string]any{
				"provider": map[string]any{
					"anthropic": map[string]any{
						"options": map[string]any{
							"apiKey": "{env:ANTHROPIC_API_KEY}",
						},
					},
				},
			}
			data, _ := json.Marshal(config)
			env["OPENCODE_CONFIG_CONTENT"] = string(data)
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
