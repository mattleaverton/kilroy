// Per-driver credential binder. Translates an auth binding Snapshot+Credential
// into the concrete artifacts a driver needs at execution time:
// env vars to set, env vars to scrub, files to write under the stage dir,
// SDK arguments to pass through. Per plan §5, materialization differs by
// driver — most importantly, claude_cli MUST scrub ANTHROPIC_API_KEY from
// the child environment so the CLI uses the logged-in session, not the
// env key (silent wrong-billing prevention).
//
// Binders are contributed per file (binder_anthropic.go, binder_openai.go,
// binder_google.go) and registered at package init. Bind dispatches by
// driver name; an unregistered driver is a typed error.

package engine

import (
	"fmt"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// BindResult carries everything a driver invocation needs from a resolved
// credential. Callers (engine API path, tmux handler) merge these into
// the child process invocation.
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

// BinderFunc is the per-driver materializer signature. stageDir is the
// per-stage logs directory under which any FilesToWrite entries should be
// rooted; the binder may use stageDir as a base path.
type BinderFunc func(snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error)

var binderRegistry = map[string]BinderFunc{}

// RegisterBinder records a per-driver binder. Called from per-driver file
// init() functions (binder_anthropic.go, etc.). Panics on double registration
// to surface accidental overrides at build time.
func RegisterBinder(driver string, f BinderFunc) {
	if _, exists := binderRegistry[driver]; exists {
		panic(fmt.Sprintf("credential binder already registered for driver %q", driver))
	}
	binderRegistry[driver] = f
}

// Bind dispatches to the registered binder for the named driver. Returns
// a typed error when no binder is registered.
func Bind(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
	f, ok := binderRegistry[driver]
	if !ok {
		return BindResult{}, fmt.Errorf("no credential binder registered for driver %q", driver)
	}
	return f(snap, cred, stageDir)
}

// RegisteredDrivers returns the set of driver names that currently have a
// binder. Used by tests and `kilroy auth check` to surface coverage.
func RegisteredDrivers() []string {
	out := make([]string, 0, len(binderRegistry))
	for k := range binderRegistry {
		out = append(out, k)
	}
	return out
}
