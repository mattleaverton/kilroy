// Resolver implements the §3 algorithm in
// docs/plans/2026-05-02-auth-class-resolver-integration.md:
// requirement → bindings lookup → chain pick → first usable source → snapshot.
// The same resolver serves prelaunch, execution, `kilroy auth check`, and
// policy reachability — single source of truth.

package binding

import (
	"fmt"
	"os"
	"sort"
)

// Resolver picks a credential source for a given Requirement against a
// loaded Config and a DetectionView of the current machine.
type Resolver struct {
	cfg    *Config
	detect DetectionView
}

// NewResolver constructs a Resolver. cfg must be non-nil; detect must be
// non-nil. Callers that need to resolve against an empty config should pass
// &Config{} explicitly so the missing-config path is at the loader, not here.
func NewResolver(cfg *Config, detect DetectionView) *Resolver {
	return &Resolver{cfg: cfg, detect: detect}
}

// Resolve walks the algorithm and returns a Snapshot or a typed error.
// Snapshot identity is enough for prelaunch to record; execution calls Bind
// later to materialize the actual credential value.
func (r *Resolver) Resolve(req Requirement) (Snapshot, error) {
	chainName, err := r.pickChain(req)
	if err != nil {
		return Snapshot{}, err
	}
	chain, ok := r.cfg.Chains[chainName]
	if !ok {
		return Snapshot{}, &ErrUnknownChain{Requirement: req, ChainName: chainName}
	}

	var skipped []SkippedSource
	for rank, src := range chain.Sources {
		if reason := r.sourceUsable(src); reason != "" {
			skipped = append(skipped, SkippedSource{Source: src, Reason: reason})
			continue
		}
		return Snapshot{
			ChainName:    chainName,
			Source:       src,
			Provider:     req.Provider,
			Method:       req.Method,
			FallbackRank: rank,
			Skipped:      skipped,
		}, nil
	}

	return Snapshot{}, &ErrChainExhausted{ChainName: chainName, Skipped: skipped}
}

// Bind materializes a Snapshot into a Credential by re-reading the named
// source freshly. If the source is no longer usable, returns ErrSourceVanished.
func (r *Resolver) Bind(snap Snapshot) (Credential, error) {
	switch snap.Source.Kind {
	case SourceEnvVar:
		val := os.Getenv(snap.Source.Name)
		if val == "" {
			return Credential{}, &ErrSourceVanished{
				Snapshot: snap,
				Reason:   fmt.Sprintf("env var %s is unset or empty", snap.Source.Name),
			}
		}
		return Credential{Snapshot: snap, Value: val}, nil
	case SourceCLISession:
		if !r.detect.CLISessionOK(snap.Source.Tool) {
			return Credential{}, &ErrSourceVanished{
				Snapshot: snap,
				Reason:   fmt.Sprintf("cli session for %s is not ok", snap.Source.Tool),
			}
		}
		return Credential{Snapshot: snap, CLITool: snap.Source.Tool}, nil
	}
	return Credential{}, fmt.Errorf("unknown source kind %q", snap.Source.Kind)
}

// pickChain implements steps 1 and 2 of the algorithm: bindings lookup, then
// requirement match across all chains, with ambiguity as an error.
func (r *Resolver) pickChain(req Requirement) (string, error) {
	if name, ok := r.cfg.Bindings[req.Key()]; ok {
		return name, nil
	}

	var matches []string
	for name, chain := range r.cfg.Chains {
		if chain.Requires == req {
			matches = append(matches, name)
		}
	}
	sort.Strings(matches)
	switch len(matches) {
	case 0:
		return "", &ErrNoChainForRequirement{Requirement: req}
	case 1:
		return matches[0], nil
	default:
		return "", &ErrAmbiguousAuthChain{Requirement: req, Candidates: matches}
	}
}

// sourceUsable returns "" when a source is usable, or a skip-reason string.
func (r *Resolver) sourceUsable(src Source) string {
	switch src.Kind {
	case SourceEnvVar:
		if !r.detect.EnvVarPresent(src.Name) {
			return "env_var_missing:" + src.Name
		}
		return ""
	case SourceCLISession:
		if !r.detect.CLISessionOK(src.Tool) {
			return "cli_session_unavailable:" + src.Tool
		}
		return ""
	}
	return "unknown_source_kind:" + string(src.Kind)
}

// EnvDetectionView is a minimal DetectionView that reports env vars from the
// current process environment and reports all CLI sessions as unavailable.
// Useful as the inner core of larger views; production wires the real auth
// detector for CLI sessions.
type EnvDetectionView struct{}

// EnvVarPresent reports whether the named env var is set to a non-empty value.
func (EnvDetectionView) EnvVarPresent(name string) bool {
	return os.Getenv(name) != ""
}

// CLISessionOK always returns false. Real production callers wrap this with
// a CLI-aware view that consults auth.ListAll output.
func (EnvDetectionView) CLISessionOK(tool string) bool { return false }
