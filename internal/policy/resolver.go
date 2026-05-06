package policy

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// ResolveRequest is the input to the resolver.
type ResolveRequest struct {
	ClassID    string // class-mode (flexible, fallback chain)
	ModelID    string // strict-mode (exact model required; no fallback)
	NodeID     string
	WorkflowID string
}

// ResolveResult is the successful outcome.
type ResolveResult struct {
	ModelID     string
	Driver      string
	Transport   string
	HistorySink string

	// AuthSnapshot is the binding.Snapshot the auth resolver picked for
	// this candidate. Provider/method/source identity for prelaunch and
	// observability; binder consumes it to materialize a Credential.
	AuthSnapshot binding.Snapshot

	RequestType   string // "class" or "strict"
	RequestValue  string
	FallbackRank  int // 0-indexed position in chain (or -1 in strict mode)
	Skipped       []SkipRecord
	PolicyVersion string
	PolicySource  string // "built_in" | "global_override" | "project_override"
	OverrideMode  string // "prefer" | "pin" when PolicySource is an override
	ResolvedAt    time.Time
}

// AuthMethod returns the credential method that satisfied the candidate's
// auth requirement (api_key or cli_oauth). Convenience for downstream
// consumers that don't unpack the AuthSnapshot.
func (r ResolveResult) AuthMethod() string {
	return string(r.AuthSnapshot.Method)
}

// AuthSource returns the concrete source identifier (env var name or CLI
// tool name) that the chain selected. Empty when no source was bound.
func (r ResolveResult) AuthSource() string {
	switch r.AuthSnapshot.Source.Kind {
	case binding.SourceEnvVar:
		return r.AuthSnapshot.Source.Name
	case binding.SourceCLISession:
		return r.AuthSnapshot.Source.Tool
	}
	return ""
}

// SkipRecord explains why one candidate was rejected.
type SkipRecord struct {
	Rank    int
	ModelID string
	Driver  string
	Reason  string // structured: "<code>:<detail>", e.g. "env_var_missing:ANTHROPIC_API_KEY"
}

// Typed errors:

// ErrUnknownClass is returned when the requested class is not in the policy.
type ErrUnknownClass struct {
	Name      string
	Available []string // sorted list for user guidance
}

func (e ErrUnknownClass) Error() string {
	return fmt.Sprintf("policy: unknown class %q; available: %v", e.Name, e.Available)
}

// ErrUnknownModel is returned in strict mode when no candidate in any class
// matches the requested model ID.
type ErrUnknownModel struct{ ModelID string }

func (e ErrUnknownModel) Error() string {
	return fmt.Sprintf("policy: unknown model %q", e.ModelID)
}

// ErrStrictModelUnreachable is returned in strict mode when the model exists
// but no matching candidate is reachable on this machine.
type ErrStrictModelUnreachable struct {
	ModelID string
	Reason  string
}

func (e ErrStrictModelUnreachable) Error() string {
	return fmt.Sprintf("policy: model %q unreachable: %s", e.ModelID, e.Reason)
}

// ErrNoViableCandidate is returned in class mode when all candidates are skipped.
type ErrNoViableCandidate struct {
	ClassID string
	Skipped []SkipRecord
}

func (e ErrNoViableCandidate) Error() string {
	return fmt.Sprintf("policy: no viable candidate for class %q (%d skipped)", e.ClassID, len(e.Skipped))
}

// ErrBothClassAndModel is returned when both ClassID and ModelID are set.
type ErrBothClassAndModel struct{}

func (e ErrBothClassAndModel) Error() string {
	return "policy: cannot specify both class_id and model_id"
}

// ErrClassSunset is returned when a class is past its sunset version.
type ErrClassSunset struct {
	Name    string
	Since   string
	Sunset  string
	Message string
}

func (e ErrClassSunset) Error() string {
	return fmt.Sprintf("policy: class %q was sunsetted at version %s: %s", e.Name, e.Sunset, e.Message)
}

// Resolve is the entry point. It dispatches to class-mode or strict-mode.
// The auth resolver is consulted to check candidate reachability — a
// candidate is reachable iff its Requires tuple resolves to a usable
// source via the user/project auth config.
func Resolve(req ResolveRequest, data *Data, authResolver *binding.Resolver) (ResolveResult, error) {
	if req.ClassID != "" && req.ModelID != "" {
		return ResolveResult{}, ErrBothClassAndModel{}
	}
	if authResolver == nil {
		return ResolveResult{}, fmt.Errorf("policy: nil auth resolver")
	}
	if req.ModelID != "" {
		return resolveStrict(req, data, authResolver)
	}
	return resolveClass(req, data, authResolver)
}

// sortedClassNames returns a sorted slice of class names from the policy.
func sortedClassNames(classes map[string]Class) []string {
	names := make([]string, 0, len(classes))
	for k := range classes {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// candidateReachability checks whether a candidate is reachable by asking
// the auth resolver for a snapshot satisfying the candidate's Requires.
// Returns (snapshot, "") on success or (zero, skipReason) when unusable.
// skipReason is a structured "<code>:<detail>" string suitable for
// logging in SkipRecord.Reason.
func candidateReachability(c Candidate, authResolver *binding.Resolver) (binding.Snapshot, string) {
	snap, err := authResolver.Resolve(c.Requires)
	if err == nil {
		return snap, ""
	}
	return binding.Snapshot{}, classifyAuthError(err, c.Requires)
}

// classifyAuthError maps a binding error to a structured skip-reason code.
func classifyAuthError(err error, req binding.Requirement) string {
	var (
		errNoChain   *binding.ErrNoChainForRequirement
		errAmbiguous *binding.ErrAmbiguousAuthChain
		errExhausted *binding.ErrChainExhausted
		errUnknown   *binding.ErrUnknownChain
	)
	switch {
	case errors.As(err, &errNoChain):
		return "auth_no_chain:" + req.Key()
	case errors.As(err, &errAmbiguous):
		return "auth_ambiguous_chain:" + req.Key()
	case errors.As(err, &errExhausted):
		return "auth_chain_exhausted:" + errExhausted.ChainName
	case errors.As(err, &errUnknown):
		return "auth_unknown_chain:" + errUnknown.ChainName
	}
	return "auth_error:" + err.Error()
}

func resolveClass(req ResolveRequest, data *Data, authResolver *binding.Resolver) (ResolveResult, error) {
	originalClassID := req.ClassID
	classID := req.ClassID

	// 1. Walk aliases; substitute To while preserving originalClassID.
	for _, alias := range data.Aliases {
		if alias.From == classID {
			classID = alias.To
			break
		}
	}

	// 2. Look up class.
	class, ok := data.Classes[classID]
	if !ok {
		return ResolveResult{}, ErrUnknownClass{
			Name:      req.ClassID,
			Available: sortedClassNames(data.Classes),
		}
	}

	// 3. Check deprecation.
	for _, dep := range data.Deprecated {
		if dep.Class == classID {
			if dep.Sunset != "" && data.PolicyVersion >= dep.Sunset {
				return ResolveResult{}, ErrClassSunset{
					Name:    classID,
					Since:   dep.Since,
					Sunset:  dep.Sunset,
					Message: dep.Message,
				}
			}
			// else: deprecation warning (no structured logger in scope)
			break
		}
	}

	policySource := PolicySourceBuiltIn
	overrideMode := ""
	if ov, ok := data.AppliedOverrides[classID]; ok {
		policySource = ov.Source
		overrideMode = ov.Mode
	}

	// 4. Walk chain, first reachable wins.
	var skipped []SkipRecord
	for i, c := range class.Chain {
		snap, skipReason := candidateReachability(c, authResolver)
		if skipReason == "" {
			return ResolveResult{
				ModelID:       c.ModelID,
				Driver:        c.Driver,
				Transport:     c.Transport,
				HistorySink:   c.HistorySink,
				AuthSnapshot:  snap,
				RequestType:   "class",
				RequestValue:  originalClassID,
				FallbackRank:  i,
				Skipped:       skipped,
				PolicyVersion: data.PolicyVersion,
				PolicySource:  policySource,
				OverrideMode:  overrideMode,
				ResolvedAt:    time.Now(),
			}, nil
		}
		skipped = append(skipped, SkipRecord{
			Rank:    i,
			ModelID: c.ModelID,
			Driver:  c.Driver,
			Reason:  skipReason,
		})
	}

	// 5. All skipped.
	return ResolveResult{}, ErrNoViableCandidate{ClassID: req.ClassID, Skipped: skipped}
}

func resolveStrict(req ResolveRequest, data *Data, authResolver *binding.Resolver) (ResolveResult, error) {
	type match struct {
		c               Candidate
		hasSubscription bool
	}

	// 1. Collect all candidates across all classes with the requested ModelID.
	var matches []match
	for _, class := range data.Classes {
		for _, c := range class.Chain {
			if c.ModelID != req.ModelID {
				continue
			}
			hasSub := false
			for _, tag := range c.Tags {
				if tag == "subscription" {
					hasSub = true
					break
				}
			}
			matches = append(matches, match{c, hasSub})
		}
	}

	// 2. No match found in any class.
	if len(matches) == 0 {
		return ResolveResult{}, ErrUnknownModel{ModelID: req.ModelID}
	}

	// Prefer subscription-tagged candidates; stable sort preserves chain order
	// within each tier.
	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].hasSubscription && !matches[j].hasSubscription
	})

	// 3. Try each match; first reachable one wins.
	var lastReason string
	for _, m := range matches {
		snap, skipReason := candidateReachability(m.c, authResolver)
		if skipReason == "" {
			return ResolveResult{
				ModelID:       m.c.ModelID,
				Driver:        m.c.Driver,
				Transport:     m.c.Transport,
				HistorySink:   m.c.HistorySink,
				AuthSnapshot:  snap,
				RequestType:   "strict",
				RequestValue:  req.ModelID,
				FallbackRank:  -1,
				Skipped:       nil,
				PolicyVersion: data.PolicyVersion,
				PolicySource:  PolicySourceBuiltIn,
				ResolvedAt:    time.Now(),
			}, nil
		}
		lastReason = skipReason
	}

	// 4. None reachable.
	return ResolveResult{}, ErrStrictModelUnreachable{ModelID: req.ModelID, Reason: lastReason}
}
