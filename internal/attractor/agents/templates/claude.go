// Claude Code invocation template.
package templates

import (
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
)

// Claude returns an invocation template for Claude Code (--bare --print mode).
func Claude() Template {
	return Template{
		Name:       "claude",
		Binary:     "claude",
		LogLocator: &agentlog.ClaudeLogLocator{},
		BuildArgs: func(prompt, workDir, model string) []string {
			args := []string{
				"--bare", "--dangerously-skip-permissions", "--print",
				"--output-format", "stream-json", "--verbose",
			}
			if model != "" {
				// Claude CLI uses dashes (claude-sonnet-4-6), not dots (claude-sonnet-4.6).
				args = append(args, "--model", strings.ReplaceAll(model, ".", "-"))
			}
			args = append(args, prompt)
			return args
		},
		BuildEnv: func() map[string]string {
			// Credential delivery is the binder's job (see
			// internal/attractor/engine/binder_anthropic.go).
			// In particular: when claude_cli is the resolved driver, the
			// binder SCRUBS ANTHROPIC_API_KEY from the child env so the
			// CLI uses the logged-in subscription session, not the env key
			// (silent wrong-billing prevention). The template must NOT
			// pass through env keys here.
			return map[string]string{}
		},
		StructuredOutput: true,
		PromptPrefix:     "❯",
		BusyIndicators:   []string{"esc to interrupt"},
		ProcessNames:     []string{"claude", "node"},
		ExitsOnComplete:  true,
		StartupDialogs:   nil,
		StartupTimeout:   15 * time.Second,
	}
}
