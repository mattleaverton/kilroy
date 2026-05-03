// Codex CLI invocation template.
package templates

import (
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
)

// Codex returns an invocation template for OpenAI Codex CLI (exec mode).
func Codex() Template {
	return Template{
		Name:       "codex",
		Binary:     "codex",
		LogLocator: &agentlog.CodexLogLocator{},
		BuildArgs: func(prompt, workDir, model, _ string) []string {
			args := []string{"exec", "--sandbox", "workspace-write", "--skip-git-repo-check", "--json", "-c", "web_search=\"disabled\""}
			if model != "" {
				args = append(args, "--model", model)
			}
			if workDir != "" {
				args = append(args, "-C", workDir)
			}
			args = append(args, prompt)
			return args
		},
		BuildEnv: func() map[string]string {
			// Credential delivery is the binder's job (see
			// internal/attractor/engine/binder_openai.go BindCodexCLI).
			// Binder writes the isolated auth.json under <stage>/.codex/
			// and sets CODEX_HOME. Template must NOT pass through env keys.
			return map[string]string{}
		},
		// PrepareSession is unused after the binder migration. Removed to
		// avoid confusion — auth.json materialization happens via the
		// binder's FilesToWrite, applied by tmux_handler before session
		// creation.
		PrepareSession:   nil,
		PromptPrefix:     "›",
		BusyIndicators:   []string{"Working", "esc to interrupt"},
		ProcessNames:     []string{"codex", "node"},
		StructuredOutput: true,
		ExitsOnComplete:  true, // exec mode exits on completion
		StartupTimeout:   30 * time.Second,
	}
}
