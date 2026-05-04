// Codex CLI invocation template.
package templates

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
)

// codexAuthPath is the location of the codex CLI's auth.json (overridable
// in tests). Codex distinguishes its login modes via the "auth_mode"
// field: "chatgpt" (subscription login, restricted model allowlist) vs
// "api_key" (free choice). When mode is "chatgpt" passing --model with
// an unsupported value (e.g. gpt-5.4-nano) yields a 400 from the
// upstream API. The template detects this and drops --model so codex
// can pick a supported default.
var codexAuthPath = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".codex", "auth.json")
}()

// codexAuthModeIsChatGPT reports whether the codex CLI's local auth is
// in subscription ("chatgpt") mode, in which case the model allowlist
// is restricted and --model should be omitted.
func codexAuthModeIsChatGPT() bool {
	if codexAuthPath == "" {
		return false
	}
	data, err := os.ReadFile(codexAuthPath)
	if err != nil {
		return false
	}
	var f struct {
		AuthMode string `json:"auth_mode"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return false
	}
	return f.AuthMode == "chatgpt"
}

// Codex returns an invocation template for OpenAI Codex CLI (exec mode).
func Codex() Template {
	return Template{
		Name:       "codex",
		Binary:     "codex",
		LogLocator: &agentlog.CodexLogLocator{},
		BuildArgs: func(prompt, workDir, model, _ string) []string {
			args := []string{"exec", "--sandbox", "workspace-write", "--skip-git-repo-check", "--json", "-c", "web_search=\"disabled\""}
			// chatgpt-mode codex restricts the model allowlist; passing an
			// unsupported model returns 400 from the upstream API. Drop
			// --model in that case so codex picks a supported default.
			// api_key mode honors the request as-is.
			if model != "" && !codexAuthModeIsChatGPT() {
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
