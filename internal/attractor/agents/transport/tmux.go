// Package transport provides standalone transport components for agent backends.
//
// The tmux transport handles CLI-based agent calls via tmux sessions,
// managing environment construction, session naming, and credential materialization.
package transport

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// BuildTmuxAgentEnv constructs the environment variables passed to a tmux-run
// agent session. It consolidates the tool template's defaults with the provided
// runtime env (from BuildStageRuntimeEnv) and status contract env vars.
//
// The templateEnv should come from tmpl.BuildEnv() (or nil if no template).
// The runtimeEnv should come from engine.BuildStageRuntimeEnv(exec, nodeID).
// The statusContractEnv should come from engine.BuildStageStatusContract(exec.WorktreeDir, runID).EnvVars.
func BuildTmuxAgentEnv(templateEnv, runtimeEnv, statusContractEnv map[string]string) map[string]string {
	env := make(map[string]string)
	for k, v := range templateEnv {
		env[k] = v
	}
	for k, v := range runtimeEnv {
		env[k] = v
	}
	for k, v := range statusContractEnv {
		env[k] = v
	}
	return env
}

// BuildSessionName creates a unique tmux session name for a node execution.
func BuildSessionName(runID, nodeID string) string {
	name := "kilroy"
	if runID != "" {
		name += "-" + runID
	}
	name += "-" + nodeID
	// Truncate and sanitize for tmux.
	if len(name) > 128 {
		name = name[:128]
	}
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, name)
}

// ShellQuoteSimple wraps a path in single quotes for shell redirection.
func ShellQuoteSimple(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// BindResult carries everything a driver invocation needs from a resolved
// credential. This is a copy of engine.BindResult to avoid import cycles.
type BindResult struct {
	// EnvSet is the env-var → value map to add to the child env.
	EnvSet map[string]string

	// EnvScrub names env vars that must be unset / removed from the child
	// env before invocation. Used by claude_cli to ensure CLI session is
	// honored (not silently overridden by ANTHROPIC_API_KEY).
	EnvScrub []string

	// FilesToWrite maps an absolute path under the stage dir to the file
	// content to write before invocation. Used by codex_cli to write an
	// isolated auth.json so the run does not mutate ~/.codex/.
	FilesToWrite map[string][]byte

	// SDKArg is the credential value passed directly to an SDK constructor
	// (anthropic_sdk, openai_sdk, google_sdk paths). Empty for cli drivers.
	SDKArg string

	// SourceName is the credential's identity (env var name or cli session
	// tool name) — used for run-record observability.
	SourceName string

	// SourceKind is the credential's source kind ("env_var" or
	// "cli_session"). Mirrors binding.Snapshot.Source.Kind.
	SourceKind string
}

// BindFunc performs the per-driver credential binding.
// This is typically engine.Bind but can be injected for testing.
type BindFunc func(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error)

// MaterializeCredential turns a class resolution into per-driver credential
// artifacts: env vars to set, env vars to scrub from the child env, and
// any per-stage files to write.
//
// The authSnapshot is the FROZEN identity from prelaunch. We re-validate
// source liveness via bindSnapshotFn. Then dispatches to the per-driver
// materializer via bindFn.
//
// Critically, claude_cli's binder returns EnvScrub=["ANTHROPIC_API_KEY"]
// (and codex_cli scrubs OPENAI_API_KEY) so the CLI uses the logged-in
// subscription session rather than silently falling through to the env key.
func MaterializeCredential(
	driver string,
	authSnapshot binding.Snapshot,
	stageDir string,
	bindSnapshotFn func(snap binding.Snapshot) (binding.Credential, error),
	bindFn BindFunc,
) (BindResult, error) {
	if authSnapshot.Source.Kind == "" {
		return BindResult{}, fmt.Errorf("materializeCredential: empty auth snapshot — only class-resolved routes carry an auth snapshot")
	}
	cred, err := bindSnapshotFn(authSnapshot)
	if err != nil {
		return BindResult{}, err
	}
	return bindFn(driver, authSnapshot, cred, stageDir)
}

// TmuxTransport constructs tmux session configuration from execution context,
// templates, and credential snapshots. It encapsulates session naming,
// environment construction, and credential materialization.
type TmuxTransport struct{}

// NewTmuxTransport creates a new TmuxTransport instance.
func NewTmuxTransport() *TmuxTransport {
	return &TmuxTransport{}
}

// SessionConfig holds the result of building a tmux session configuration.
type SessionConfig struct {
	// SessionName is the unique tmux session name.
	SessionName string
	// Env is the consolidated environment variables for the session.
	Env map[string]string
	// EnvScrub lists env vars to unset via `env -u` wrapper.
	EnvScrub []string
	// BindResult contains credential materialization results (if any).
	BindResult *BindResult
}

// BuildSession constructs the tmux session configuration from the given
// inputs. It encapsulates:
//   - Session name allocation via BuildSessionName
//   - Environment construction via BuildTmuxAgentEnv
//   - Credential materialization via MaterializeCredential (when auth snapshot provided)
//
// The caller provides pre-built environment components (runtimeEnv, statusContractEnv)
// to avoid import cycles with the engine package.
func (t *TmuxTransport) BuildSession(
	tmpl *templates.Template,
	nodeID string,
	runID string,
	worktreeDir string,
	logsRoot string,
	driver string,
	authSnapshot *binding.Snapshot,
	runtimeEnv map[string]string,
	statusContractEnv map[string]string,
	bindSnapshotFn func(snap binding.Snapshot) (binding.Credential, error),
	bindFn BindFunc,
) (SessionConfig, error) {
	// Session name: kilroy-{runID}-{nodeID} (unique per node execution).
	sessionName := BuildSessionName(runID, nodeID)

	// Build environment variables.
	templateEnv := tmpl.BuildEnv()
	env := BuildTmuxAgentEnv(templateEnv, runtimeEnv, statusContractEnv)

	// Add worktree directory to environment.
	if worktreeDir != "" {
		env["TMUX_AGENT_WORKTREE"] = worktreeDir
	}

	// Materialize credentials when auth snapshot provided.
	var envScrub []string
	var bindResult *BindResult
	if authSnapshot != nil && authSnapshot.Source.Kind != "" {
		stageDir := StageDir(logsRoot, nodeID)
		result, err := MaterializeCredential(driver, *authSnapshot, stageDir, bindSnapshotFn, bindFn)
		if err != nil {
			return SessionConfig{}, fmt.Errorf("credential bind: %w", err)
		}
		bindResult = &result
		for k, v := range result.EnvSet {
			env[k] = v
		}
		for _, name := range result.EnvScrub {
			delete(env, name)
		}
		envScrub = append(envScrub, result.EnvScrub...)
	}

	return SessionConfig{
		SessionName: sessionName,
		Env:         env,
		EnvScrub:    envScrub,
		BindResult:  bindResult,
	}, nil
}

// StageDir returns the stage directory path for a node.
// This is a helper to avoid engine package import.
func StageDir(logsRoot, nodeID string) string {
	return filepath.Join(logsRoot, nodeID)
}
