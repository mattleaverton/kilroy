// Package policy implements baked-in routing data and class resolution
// for Kilroy v2. Per plan §6: workflows ask for an abstract class
// (hard_coding, quick_easy, etc.) and the resolver picks a concrete
// (model, driver, transport, auth) tuple based on policy data plus the
// machine state surfaced by internal/auth.
//
// The policy data itself ships as a TOML file embedded at build time.
// It is read-only at runtime: no per-machine override, no remote fetch.
// Updates are normal repo PRs.

package policy

import (
	_ "embed"
	"fmt"

	"github.com/BurntSushi/toml"
)

//go:embed data/policy.toml
var rawPolicy []byte

// Data is the root structure decoded from the embedded TOML file.
type Data struct {
	SchemaVersion string             `toml:"schema_version"`
	PolicyVersion string             `toml:"policy_version"`
	Classes       map[string]Class   `toml:"classes"`
	Aliases       []ClassAlias       `toml:"aliases"`
	Deprecated    []Deprecation      `toml:"deprecated"`
}

// Class is a named resolution strategy: an ordered fallback chain of
// candidate routes. The resolver walks Chain[0], Chain[1], ... until
// one passes the machine-state checks.
type Class struct {
	Description string      `toml:"description"`
	Chain       []Candidate `toml:"chain"`
}

// Candidate is one option in a class's fallback chain. The resolver
// evaluates each candidate against MachineState and picks the first
// that's reachable.
type Candidate struct {
	ModelID     string   `toml:"model_id"`
	Driver      string   `toml:"driver"`        // "claude_cli" | "codex_cli" | "anthropic_sdk" | "openai_sdk" | "google_sdk"
	Transport   string   `toml:"transport"`     // "http" | "cli_subprocess" | "tmux_pty"
	HistorySink string   `toml:"history_sink"`  // "jsonl_local" | "api_stream"  (renamed turn_codec in plan §9; sink kept for v2 baseline)
	Tags        []string `toml:"tags"`          // informational: "subscription" | "api_key" | "tier:elite"
	Auth        AuthReq  `toml:"auth"`
}

// AuthReq describes the credential a candidate needs. The resolver
// never reads the credential value — it consults MachineState to check
// presence (env var set, CLI session probe true, etc.).
type AuthReq struct {
	Kind   string `toml:"kind"`              // "env_var" | "cli_session" | "none"
	EnvVar string `toml:"env_var,omitempty"` // when Kind == "env_var"
	CLI    string `toml:"cli,omitempty"`     // when Kind == "cli_session"
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
			if c.Auth.Kind == "" {
				return fmt.Errorf("class %q candidate %d (%s): auth.kind is required", name, i, c.ModelID)
			}
			switch c.Auth.Kind {
			case "env_var":
				if c.Auth.EnvVar == "" {
					return fmt.Errorf("class %q candidate %d (%s): auth.env_var required when kind=env_var", name, i, c.ModelID)
				}
			case "cli_session":
				if c.Auth.CLI == "" {
					return fmt.Errorf("class %q candidate %d (%s): auth.cli required when kind=cli_session", name, i, c.ModelID)
				}
			case "none":
				// no-op: local endpoints, no auth required
			default:
				return fmt.Errorf("class %q candidate %d (%s): unknown auth.kind %q", name, i, c.ModelID, c.Auth.Kind)
			}
		}
	}
	return nil
}
