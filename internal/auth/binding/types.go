// Package binding integrates auth resolution into the policy/class resolver.
// Policy candidates declare what they need (Requirement); user/project config
// declares how to satisfy it (Chain + Source list); the Resolver picks the
// concrete source. See docs/plans/2026-05-02-auth-class-resolver-integration.md.
package binding

// SourceKind classifies how a credential source is resolved.
type SourceKind string

const (
	// SourceEnvVar reads a value from a named environment variable.
	SourceEnvVar SourceKind = "env_var"

	// SourceCLISession relies on a logged-in CLI tool's local session
	// (e.g. claude --print uses ~/.claude/ when ANTHROPIC_API_KEY is unset).
	SourceCLISession SourceKind = "cli_session"
)

// Method classifies how the credential will be presented to the driver.
// Mirrors auth detection vocabulary so prelaunch and detection align.
type Method string

const (
	MethodAPIKey   Method = "api_key"
	MethodCLIOAuth Method = "cli_oauth"
)

// Requirement is what a policy candidate declares it needs to run. Tool is
// optional and only meaningful for cli_oauth (e.g. tool="claude" pins the
// requirement to the Claude CLI's session, not just "any anthropic oauth").
type Requirement struct {
	Provider string `toml:"provider" json:"provider"`
	Method   Method `toml:"method"   json:"method"`
	Tool     string `toml:"tool,omitempty" json:"tool,omitempty"`
}

// Key returns the canonical "<provider>/<method>[/<tool>]" key used for
// lookups in the bindings table.
func (r Requirement) Key() string {
	if r.Tool == "" {
		return r.Provider + "/" + string(r.Method)
	}
	return r.Provider + "/" + string(r.Method) + "/" + r.Tool
}

// Source is one entry in a chain's source list. Exactly one of Name or Tool
// is populated based on Kind.
type Source struct {
	Kind SourceKind `toml:"kind" json:"kind"`

	// Name is the env var name when Kind == SourceEnvVar.
	Name string `toml:"name,omitempty" json:"name,omitempty"`

	// Tool is the CLI tool name when Kind == SourceCLISession.
	Tool string `toml:"tool,omitempty" json:"tool,omitempty"`
}

// ID returns a stable identifier for this source for use in skip reasons
// and snapshot records.
func (s Source) ID() string {
	switch s.Kind {
	case SourceEnvVar:
		return "env:" + s.Name
	case SourceCLISession:
		return "cli:" + s.Tool
	}
	return string(s.Kind)
}

// Chain is a named list of sources that satisfy a Requirement. Sources are
// tried in order; the first usable one wins.
type Chain struct {
	Name     string      `toml:"-"        json:"name"`
	Requires Requirement `toml:"requires" json:"requires"`
	Sources  []Source    `toml:"sources"  json:"sources"`
}

// Config is the user/project auth.toml shape. Bindings disambiguate when
// multiple chains satisfy the same Requirement; ChainsByName holds every
// declared chain.
type Config struct {
	// Bindings maps a Requirement.Key() to a chain name. When a Requirement
	// has a binding entry, the resolver uses that chain even if other chains
	// could satisfy it.
	Bindings map[string]string `toml:"bindings"`

	// Chains maps chain name → Chain definition.
	Chains map[string]Chain `toml:"chains"`
}

// SkippedSource records why a source was bypassed during chain resolution.
type SkippedSource struct {
	Source Source `json:"source"`
	Reason string `json:"reason"`
}

// Snapshot is what prelaunch records and execution re-reads. Identity-only —
// never holds the credential value. Authoritative when written to
// resolution.json after execution materializes the source.
type Snapshot struct {
	ChainName    string          `json:"chain_name"`
	Source       Source          `json:"source"`
	Provider     string          `json:"provider"`
	Method       Method          `json:"method"`
	FallbackRank int             `json:"fallback_rank"`
	Skipped      []SkippedSource `json:"skipped,omitempty"`
}

// Credential is the materialized form: snapshot identity plus the actual
// value (for env-var sources) or session marker (for cli_session sources).
// Never persisted to disk; only flows in-memory from the binder to the
// driver.
type Credential struct {
	Snapshot Snapshot

	// Value is the env var contents when Snapshot.Source.Kind == SourceEnvVar.
	// Empty for cli_session sources.
	Value string

	// CLITool is the session-owning binary when Kind == SourceCLISession.
	// Empty for env_var sources.
	CLITool string
}

// DetectionView is the read-only interface the resolver uses to ask "is this
// source present and usable on this machine?" Production wires this to the
// existing internal/auth detector output; tests pass a fake.
type DetectionView interface {
	// EnvVarPresent returns true if the named env var is set to a non-empty
	// value.
	EnvVarPresent(name string) bool

	// CLISessionOK returns true if the named CLI tool has a usable
	// logged-in session (auth detector reported state=ok for that tool).
	CLISessionOK(tool string) bool
}
