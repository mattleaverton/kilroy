// Typed errors for auth binding resolution. Every failure mode the resolver
// can produce has its own error type so callers (prelaunch, execution,
// `kilroy auth check`) can render targeted remediation.

package binding

import (
	"fmt"
	"strings"
)

// ErrNoConfig is returned by config loading when neither
// ~/.config/kilroy/auth.toml nor <project>/.kilroy/auth.toml exists.
type ErrNoConfig struct {
	UserPath    string
	ProjectPath string
}

func (e *ErrNoConfig) Error() string {
	return fmt.Sprintf("no auth config found (looked at %s and %s); run `kilroy auth init`", e.UserPath, e.ProjectPath)
}

// ErrNoChainForRequirement is returned when a Requirement does not match any
// configured chain, with no binding entry to resolve the gap.
type ErrNoChainForRequirement struct {
	Requirement Requirement
}

func (e *ErrNoChainForRequirement) Error() string {
	return fmt.Sprintf("no auth chain configured for %s; run `kilroy auth defaults` for the template, then add to your auth.toml", e.Requirement.Key())
}

// ErrAmbiguousAuthChain is returned when a Requirement has no binding entry
// and multiple chains satisfy it. The caller adds a [bindings] entry to
// disambiguate.
type ErrAmbiguousAuthChain struct {
	Requirement Requirement
	Candidates  []string
}

func (e *ErrAmbiguousAuthChain) Error() string {
	return fmt.Sprintf(
		"requirement %s matches multiple chains (%s) but [bindings] has no entry; add `bindings.%q = \"<chain-name>\"` to your auth.toml",
		e.Requirement.Key(),
		strings.Join(e.Candidates, ", "),
		e.Requirement.Key(),
	)
}

// ErrChainExhausted is returned when a chain was selected but every source
// in its source list was unusable on this machine.
type ErrChainExhausted struct {
	ChainName string
	Skipped   []SkippedSource
}

func (e *ErrChainExhausted) Error() string {
	parts := make([]string, 0, len(e.Skipped))
	for _, s := range e.Skipped {
		parts = append(parts, fmt.Sprintf("%s: %s", s.Source.ID(), s.Reason))
	}
	return fmt.Sprintf("chain %q has no usable source: %s", e.ChainName, strings.Join(parts, "; "))
}

// ErrSourceVanished is returned by Bind when a Snapshot's source was usable
// at prelaunch but is no longer present at execution time. Decisive failure;
// callers must not re-walk the chain.
type ErrSourceVanished struct {
	Snapshot Snapshot
	Reason   string
}

func (e *ErrSourceVanished) Error() string {
	return fmt.Sprintf("auth source %s vanished between prelaunch and execution: %s", e.Snapshot.Source.ID(), e.Reason)
}

// ErrUnknownChain is returned when a [bindings] entry references a chain
// name that doesn't exist in [chains.*]. Distinct from
// ErrNoChainForRequirement (which is about requirement, not chain name).
type ErrUnknownChain struct {
	Requirement Requirement
	ChainName   string
}

func (e *ErrUnknownChain) Error() string {
	return fmt.Sprintf("[bindings] for %s names chain %q but no such chain is defined", e.Requirement.Key(), e.ChainName)
}
