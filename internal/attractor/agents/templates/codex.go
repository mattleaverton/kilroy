// Codex CLI invocation template.
package templates

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/agentlog"
	"github.com/danshapiro/kilroy/internal/auth"
)

// codexAuthPath is the location of the codex CLI's auth.json (overridable
// in tests). Codex distinguishes its login modes via the "auth_mode"
// field: "chatgpt" (subscription login, restricted model allowlist) vs
// "apikey" (free choice). When the resolved auth method indicates a
// subscription login, passing --model with a non-allowlisted value
// (e.g. gpt-5.4-nano) yields a 400 from the upstream API.
var codexAuthPath = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".codex", "auth.json")
}()

// codexShouldDropModel decides whether to omit --model from the codex
// invocation, given the kilroy-resolved authMethod. The resolved
// authMethod is authoritative when non-empty:
//
//   - "api_key"   → keep --model. Codex was bound to an env-var key
//     (binder writes auth.json with "auth_mode":"apikey")
//     and the api_key path supports any model.
//   - "cli_oauth" → drop --model. The user is logged in to codex via
//     subscription, which restricts the model allowlist.
//
// The empty case ("") is the legacy stylesheet path with no policy
// resolution. As a fallback, peek at the user's global ~/.codex/auth.json
// to detect chatgpt mode. Without this, legacy graphs that pin a model
// would silently 400 on subscription-bound codex.
func codexShouldDropModel(authMethod string) bool {
	switch authMethod {
	case "api_key":
		return false
	case "cli_oauth":
		return true
	}
	// Legacy fallback only — no resolved method means we don't know;
	// inspect global state.
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
	return f.AuthMode == auth.CodexAuthModeChatGPT
}

// Codex returns an invocation template for OpenAI Codex CLI (exec mode).
func Codex() Template {
	return Template{
		Name:       "codex",
		Binary:     "codex",
		LogLocator: &agentlog.CodexLogLocator{},
		BuildArgs: func(prompt, workDir, model, authMethod, _ string) []string {
			args := []string{"exec", "--sandbox", "workspace-write", "--skip-git-repo-check", "--json", "-c", "web_search=\"disabled\""}
			// Subscription-bound codex restricts the model allowlist; passing
			// an unsupported model returns 400 from the upstream API. Drop
			// --model in that case so codex picks a supported default. The
			// resolved authMethod is authoritative — see codexShouldDropModel.
			if model != "" && !codexShouldDropModel(authMethod) {
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
