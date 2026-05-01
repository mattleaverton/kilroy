// Package auth implements credential discovery for kilroy v2.
//
// Per plan §8: kilroy's auth surface is "discovery and routing, not storage
// or rotation." This package scans the local machine for credentials usable
// by kilroy workflows — env vars, CLI tool config files, OS keychain entries —
// and reports their state without ever holding the credential value itself.
//
// Each LLM CLI tool (claude, codex, gh, gemini, aider, opencode, cursor) has
// its own detector implementing the Detector interface. The orchestrator
// (list.go, to be added in a follow-up) walks all registered detectors and
// produces a unified AuthListOutput.
package auth

import (
	"time"
)

// State is the auth-source health state surfaced to callers.
type State string

const (
	StateOK        State = "ok"
	StateExpired   State = "expired"
	StateMissing   State = "missing"
	StateAmbiguous State = "ambiguous"
)

// Kind classifies how the credential is stored.
type Kind string

const (
	KindEnvVar     Kind = "env_var"
	KindCLIOAuth   Kind = "cli_oauth"
	KindCLIAPIKey  Kind = "cli_api_key"
	KindAPIKeyFile Kind = "api_key_file"
	KindKeychain   Kind = "keychain"
)

// Identity carries non-secret identifiers that disambiguate which account a
// credential corresponds to. All fields are optional; populate what's
// available without invoking the tool or exposing the secret.
type Identity struct {
	Email     string `json:"email,omitempty"`
	User      string `json:"user,omitempty"`
	Org       string `json:"org,omitempty"`
	AccountID string `json:"account_id,omitempty"`
}

// Expiry describes the validity window of OAuth-style tokens.
type Expiry struct {
	AccessTokenExpiresAt *time.Time `json:"access_token_expires_at,omitempty"`
	RefreshTokenPresent  bool       `json:"refresh_token_present"`
	Refreshable          bool       `json:"refreshable"`
}

// Source describes where the credential lives. At least one field is
// non-empty for any non-missing entry.
type Source struct {
	EnvVar          string `json:"env_var,omitempty"`
	File            string `json:"file,omitempty"`
	KeychainService string `json:"keychain_service,omitempty"`
	KeychainAccount string `json:"keychain_account,omitempty"`
}

// Profile is one of multiple accounts under the same tool.
type Profile struct {
	Name     string   `json:"name"`
	Active   bool     `json:"active"`
	Identity Identity `json:"identity,omitempty"`
	State    State    `json:"state"`
}

// Entry is a single auth source produced by a Detector.
type Entry struct {
	// ID is a stable slug, e.g. "anthropic.env.ANTHROPIC_API_KEY".
	ID string `json:"id"`

	Kind     Kind     `json:"kind"`
	Provider string   `json:"provider"`
	Tool     string   `json:"tool,omitempty"`
	State    State    `json:"state"`
	Identity Identity `json:"identity,omitempty"`
	Expiry   *Expiry  `json:"expiry,omitempty"`
	Source   Source   `json:"source"`
	Profiles []Profile `json:"profiles,omitempty"`

	// Shadows lists entry IDs that this entry takes precedence over at
	// runtime (e.g. an env_var entry shadows a cli_oauth entry for the
	// same provider). ShadowedBy is the inverse view.
	Shadows    []string `json:"shadows,omitempty"`
	ShadowedBy []string `json:"shadowed_by,omitempty"`

	Notes       []string `json:"notes,omitempty"`
	Remediation string   `json:"remediation,omitempty"`
}

// Detector scans for credentials of one tool or env-var family. Detect
// returns zero or more entries; an empty result is normal (the tool isn't
// installed / configured) and is not an error. Errors are reserved for
// genuine I/O or parse failures the caller might want to log.
type Detector interface {
	// Name is a stable identifier for this detector, used in error reporting
	// (e.g. "claude", "codex", "gh", "env_vars").
	Name() string

	// Detect performs the scan. Implementations must NOT log or return
	// credential values — only their presence, location, and state.
	Detect() ([]Entry, error)
}
