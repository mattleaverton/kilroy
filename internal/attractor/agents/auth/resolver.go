// Package auth provides credential resolution abstractions for agent transports.
//
// AuthResolver abstracts the credential-reading concern so tests can inject
// fake resolvers and future credential sources can be added without touching
// transport code. The freeze invariant (snapshot is authoritative at execution)
// is preserved by the BindingAuthResolver implementation.
package auth

import (
	"context"
	"fmt"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// AgentRoute identifies the agent execution context for credential resolution.
// It carries the frozen prelaunch snapshot that determines which credential
// source to materialize.
type AgentRoute struct {
	// Provider is the LLM provider (e.g., "anthropic", "openai", "google").
	Provider string

	// Driver is the driver name (e.g., "claude_cli", "anthropic_sdk", "codex_cli").
	Driver string

	// SnapshotIdentity is the frozen prelaunch snapshot. When non-empty,
	// implementations must honor it and not re-resolve from auth.toml.
	SnapshotIdentity binding.Snapshot
}

// Credential is the materialized credential result that transports consume.
// It captures API keys, environment variables, files to write, and source
// identity for observability.
type Credential struct {
	// APIKey is the credential value for API-based transports (SDK paths).
	// Empty for CLI-based transports that use session authentication.
	APIKey string

	// EnvVarsToSet maps environment variable names to values that should be
	// set in the child process environment.
	EnvVarsToSet map[string]string

	// EnvVarsToScrub lists environment variable names that must be unset
	// from the child environment before invocation. Used by CLI transports
	// (e.g., claude_cli scrubs ANTHROPIC_API_KEY) to ensure the CLI uses
	// the logged-in session rather than silently falling back to env vars.
	EnvVarsToScrub []string

	// FilesToWrite maps absolute file paths to content that should be written
	// before invocation. Used by transports like codex_cli to write isolated
	// auth.json files.
	FilesToWrite map[string][]byte

	// SourceIdentity is the credential source identifier (env var name or
	// CLI tool name) for run-record observability.
	SourceIdentity string

	// SourceKind is the credential source kind ("env_var" or "cli_session").
	SourceKind string
}

// BindSnapshotFunc materializes a frozen prelaunch credential.
// This is typically engine.BindSnapshot but can be injected for testing.
type BindSnapshotFunc func(snap binding.Snapshot) (binding.Credential, error)

// BindFunc performs the per-driver credential binding.
// This is typically engine.Bind but can be injected for testing.
type BindFunc func(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error)

// BindResult carries everything a driver invocation needs from a resolved
// credential. This mirrors the structure in transport/tmux.go to avoid
// import cycles.
type BindResult struct {
	// EnvSet is the env-var → value map to add to the child env.
	EnvSet map[string]string

	// EnvScrub names env vars that must be unset / removed from the child
	// env before invocation.
	EnvScrub []string

	// FilesToWrite maps an absolute path under the stage dir to the file
	// content to write before invocation.
	FilesToWrite map[string][]byte

	// SDKArg is the credential value passed directly to an SDK constructor.
	SDKArg string

	// SourceName is the credential's identity for observability.
	SourceName string

	// SourceKind is the credential's source kind.
	SourceKind string
}

// AuthResolver abstracts credential resolution for agent transports.
//
// Implementations resolve credentials based on the route's provider, driver,
// and frozen snapshot identity. The resolver is responsible for honoring
// the prelaunch snapshot when SnapshotIdentity is non-empty.
type AuthResolver interface {
	// ResolveCredential returns a bound credential for the given route's
	// credential intent. The result is the materialized value the
	// transport will consume (e.g., an API key string for HTTP, an
	// env-set/file-set bundle for tmux). Implementations must honor the
	// prelaunch snapshot when route.SnapshotIdentity is non-empty.
	ResolveCredential(ctx context.Context, route AgentRoute) (Credential, error)
}

// BindingAuthResolver is the default AuthResolver implementation.
// It wraps the existing internal/auth/binding machinery and the engine
// BindSnapshot/Bind helpers to provide credential resolution.
//
// This implementation preserves the freeze invariant: when a snapshot is
// provided, it materializes from that snapshot without re-reading auth.toml.
type BindingAuthResolver struct {
	bindSnapshot BindSnapshotFunc
	bind         BindFunc
}

// NewBindingAuthResolver creates a new BindingAuthResolver with the given
// bind functions. If bindSnapshot is nil, the resolver will fail when
// attempting to resolve with a snapshot. If bind is nil, driver-specific
// materialization will fail.
func NewBindingAuthResolver(bindSnapshot BindSnapshotFunc, bind BindFunc) *BindingAuthResolver {
	return &BindingAuthResolver{
		bindSnapshot: bindSnapshot,
		bind:         bind,
	}
}

// ResolveCredential implements AuthResolver.
//
// When route.SnapshotIdentity has a non-empty source kind, it materializes
// the credential from the frozen snapshot using bindSnapshot, then applies
// driver-specific materialization using bind.
//
// When route.SnapshotIdentity is empty, returns an error since class-routed
// auth requires a prelaunch snapshot.
func (r *BindingAuthResolver) ResolveCredential(ctx context.Context, route AgentRoute) (Credential, error) {
	if route.SnapshotIdentity.Source.Kind == "" {
		return Credential{}, fmt.Errorf("authresolver: empty auth snapshot — only class-routed routes carry an auth snapshot")
	}

	if r.bindSnapshot == nil {
		return Credential{}, fmt.Errorf("authresolver: no bindSnapshot function configured")
	}

	// Materialize the credential from the frozen snapshot.
	bindingCred, err := r.bindSnapshot(route.SnapshotIdentity)
	if err != nil {
		return Credential{}, fmt.Errorf("authresolver: failed to bind snapshot: %w", err)
	}

	// For API-based drivers, extract the API key directly.
	apiKey := ""
	if bindingCred.Snapshot.Method == binding.MethodAPIKey {
		apiKey = bindingCred.Value
	}

	// Apply driver-specific materialization if bind function is available.
	var bindResult *BindResult
	if r.bind != nil {
		// Stage dir is not needed for SDK paths but may be for CLI paths.
		// Pass empty string here; callers that need stage-specific files
		// should use the lower-level bind directly.
		result, err := r.bind(route.Driver, route.SnapshotIdentity, bindingCred, "")
		if err != nil {
			return Credential{}, fmt.Errorf("authresolver: bind failed for driver %s: %w", route.Driver, err)
		}
		bindResult = &result
	}

	// Build the Credential result.
	cred := Credential{
		APIKey:         apiKey,
		SourceIdentity: route.SnapshotIdentity.Source.ID(),
		SourceKind:     string(route.SnapshotIdentity.Source.Kind),
	}

	if bindResult != nil {
		cred.EnvVarsToSet = bindResult.EnvSet
		cred.EnvVarsToScrub = bindResult.EnvScrub
		cred.FilesToWrite = bindResult.FilesToWrite
		if bindResult.SourceName != "" {
			cred.SourceIdentity = bindResult.SourceName
		}
		if bindResult.SourceKind != "" {
			cred.SourceKind = bindResult.SourceKind
		}
	}

	return cred, nil
}

// StaticAuthResolver is a test helper that returns a fixed credential.
// It ignores the route and always returns the configured credential.
type StaticAuthResolver struct {
	cred Credential
	err  error
}

// NewStaticAuthResolver creates a StaticAuthResolver that always returns
// the given credential and error.
func NewStaticAuthResolver(cred Credential, err error) *StaticAuthResolver {
	return &StaticAuthResolver{cred: cred, err: err}
}

// ResolveCredential implements AuthResolver by returning the static values.
func (r *StaticAuthResolver) ResolveCredential(ctx context.Context, route AgentRoute) (Credential, error) {
	return r.cred, r.err
}

// FakeAuthResolver is a test helper that records resolved routes and returns
// configurable results. It is useful for verifying that transports call the
// resolver with the expected routes.
type FakeAuthResolver struct {
	Routes   []AgentRoute
	Creds    map[string]Credential
	Errors   map[string]error
	Default  Credential
	DefError error
}

// NewFakeAuthResolver creates a new FakeAuthResolver.
func NewFakeAuthResolver() *FakeAuthResolver {
	return &FakeAuthResolver{
		Routes: make([]AgentRoute, 0),
		Creds:  make(map[string]Credential),
		Errors: make(map[string]error),
	}
}

// ResolveCredential implements AuthResolver, recording the route and returning
// a configured result based on route key (provider+driver).
func (r *FakeAuthResolver) ResolveCredential(ctx context.Context, route AgentRoute) (Credential, error) {
	r.Routes = append(r.Routes, route)
	key := route.Provider + "/" + route.Driver
	if cred, ok := r.Creds[key]; ok {
		return cred, nil
	}
	if err, ok := r.Errors[key]; ok {
		return Credential{}, err
	}
	return r.Default, r.DefError
}

// SetCredential configures a credential to return for the given provider/driver.
func (r *FakeAuthResolver) SetCredential(provider, driver string, cred Credential) {
	r.Creds[provider+"/"+driver] = cred
}

// SetError configures an error to return for the given provider/driver.
func (r *FakeAuthResolver) SetError(provider, driver string, err error) {
	r.Errors[provider+"/"+driver] = err
}
