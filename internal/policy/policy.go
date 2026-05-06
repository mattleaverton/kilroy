// Package policy implements baked-in routing data and class resolution
// for Kilroy v2. Per plan §6: workflows ask for an abstract class
// (hard_coding, quick_easy, etc.) and the resolver picks a concrete
// (model, driver, transport, auth) tuple based on policy data plus the
// machine state surfaced by internal/auth.
//
// The base policy data ships as a TOML file embedded at build time.
// User/project override files can reorder or pin class chains at runtime,
// but they do not name raw workflow models directly; workflows still ask
// for abstract classes and the resolver still checks auth reachability.
//
//go:generate go run ./cmd/gen_classes_doc/main.go

package policy

import (
	_ "embed"
	"fmt"
	"sort"

	"github.com/BurntSushi/toml"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

//go:embed data/policy.toml
var rawPolicy []byte

// Data is the root structure decoded from the embedded TOML file.
type Data struct {
	SchemaVersion string           `toml:"schema_version"`
	PolicyVersion string           `toml:"policy_version"`
	Classes       map[string]Class `toml:"classes"`
	Aliases       []ClassAlias     `toml:"aliases"`
	Deprecated    []Deprecation    `toml:"deprecated"`

	// AppliedOverrides records runtime override provenance by class. It is
	// derived from global/project policy override files and is intentionally
	// not part of the embedded policy TOML schema.
	AppliedOverrides map[string]AppliedOverride `toml:"-"`
}

// Class is a named resolution strategy: an ordered fallback chain of
// candidate routes. The resolver walks Chain[0], Chain[1], ... until
// one passes the auth-binding checks.
type Class struct {
	Description string      `toml:"description"`
	Chain       []Candidate `toml:"chain"`
}

// Candidate is one option in a class's fallback chain. The resolver
// evaluates each candidate's Requires against the auth.binding resolver
// and picks the first whose chain has a usable source.
//
// Per plan §13.7 / 2026-05-02-auth-class-resolver-integration §1: a
// candidate declares only what credential type it needs (provider+method),
// never an env var name. Concrete sources live in the user/project
// auth.toml; the binding.Resolver consults them.
type Candidate struct {
	ModelID     string              `toml:"model_id"`
	Driver      string              `toml:"driver"`       // "claude_cli" | "codex_cli" | "anthropic_sdk" | "openai_sdk" | "google_sdk"
	Transport   string              `toml:"transport"`    // "http" | "cli_subprocess" | "tmux_pty"
	HistorySink string              `toml:"history_sink"` // "jsonl_local" | "api_stream" (renamed turn_codec in plan §9; sink kept for v2 baseline)
	Tags        []string            `toml:"tags"`         // informational: "subscription" | "api_key" | "tier:elite"
	Requires    binding.Requirement `toml:"requires"`
}

// ClassAlias redirects an old class name to a current one. The
// resolver walks aliases before looking up the class; a logged warning
// records that an alias was used.
type ClassAlias struct {
	From string `toml:"from"`
	To   string `toml:"to"`
}

// Deprecation marks a class as deprecated. The class still resolves
// (a warning is logged) until Sunset, at which point it errors.
type Deprecation struct {
	Class   string `toml:"class"`
	Since   string `toml:"since"`
	Sunset  string `toml:"sunset,omitempty"`
	Message string `toml:"message"`
}

// AppliedOverride describes the effective runtime override that modified a
// class chain. Source is "global_override" or "project_override".
type AppliedOverride struct {
	Source  string `json:"source"`
	Path    string `json:"path,omitempty"`
	Mode    string `json:"mode"`
	ModelID string `json:"model_id"`
	Driver  string `json:"driver,omitempty"`
}

// ClassNames returns the sorted set of class names from the embedded
// policy data. Used by validate (and other prelaunch callers) to surface
// unknown agent_class= references at lint time rather than runtime.
func ClassNames() ([]string, error) {
	d, err := Load()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(d.Classes))
	for name := range d.Classes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Load returns the embedded policy data, parsed once. Callers must not
// mutate the returned value.
func Load() (*Data, error) {
	var d Data
	if err := toml.Unmarshal(rawPolicy, &d); err != nil {
		return nil, fmt.Errorf("policy: parse embedded data: %w", err)
	}
	if err := validateData(&d); err != nil {
		return nil, fmt.Errorf("policy: validate: %w", err)
	}
	return &d, nil
}

// validateData performs basic structural checks on the loaded policy.
// We don't validate model IDs against external catalogs here — that's
// the resolver's job at runtime via the auth/machine-state layer.
func validateData(d *Data) error {
	if d.SchemaVersion == "" {
		return fmt.Errorf("schema_version is required")
	}
	if d.PolicyVersion == "" {
		return fmt.Errorf("policy_version is required")
	}
	if len(d.Classes) == 0 {
		return fmt.Errorf("policy must define at least one class")
	}
	for name, class := range d.Classes {
		if len(class.Chain) == 0 {
			return fmt.Errorf("class %q has empty chain (need at least one candidate)", name)
		}
		for i, c := range class.Chain {
			if c.ModelID == "" {
				return fmt.Errorf("class %q candidate %d: model_id is required", name, i)
			}
			if c.Driver == "" {
				return fmt.Errorf("class %q candidate %d (%s): driver is required", name, i, c.ModelID)
			}
			if c.Requires.Provider == "" {
				return fmt.Errorf("class %q candidate %d (%s): requires.provider is required", name, i, c.ModelID)
			}
			if c.Requires.Method == "" {
				return fmt.Errorf("class %q candidate %d (%s): requires.method is required", name, i, c.ModelID)
			}
			switch c.Requires.Method {
			case binding.MethodAPIKey:
				// Tool not required for api_key.
			case binding.MethodCLIOAuth:
				if c.Requires.Tool == "" {
					return fmt.Errorf("class %q candidate %d (%s): requires.tool required when method=cli_oauth", name, i, c.ModelID)
				}
			default:
				return fmt.Errorf("class %q candidate %d (%s): unknown requires.method %q", name, i, c.ModelID, c.Requires.Method)
			}
		}
	}
	return nil
}
