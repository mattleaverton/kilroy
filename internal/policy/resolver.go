package policy

import (
	"fmt"
	"sort"
	"time"

	"github.com/danshapiro/kilroy/internal/auth"
)

// MachineState is the snapshot the resolver consults to decide
// candidate reachability. Wrap the result of auth.ListAll.
type MachineState struct {
	Auth auth.ListOutput
}

// CollectMachineState runs the default detectors and returns a fresh
// snapshot. Tests should NOT call this; tests construct MachineState
// directly with crafted entries.
func CollectMachineState() MachineState {
	return MachineState{Auth: auth.ListAll("", auth.DefaultDetectors())}
}

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
	AuthMethod  string // candidate's Auth.Kind value
	AuthSource  string // env var name OR cli name (the concrete identifier)

	RequestType  string // "class" or "strict"
	RequestValue string
	FallbackRank int // 0-indexed position in chain (or -1 in strict mode)
	Skipped      []SkipRecord
	PolicyVersion string
	ResolvedAt    time.Time
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
func Resolve(req ResolveRequest, data *Data, state MachineState) (ResolveResult, error) {
	if req.ClassID != "" && req.ModelID != "" {
		return ResolveResult{}, ErrBothClassAndModel{}
	}
	if req.ModelID != "" {
		return resolveStrict(req, data, state)
	}
	return resolveClass(req, data, state)
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

// candidateReachability checks whether a candidate is reachable on the
// current machine. Returns (authSource, skipReason): if skipReason is "",
// the candidate is reachable and authSource is the concrete identifier.
func candidateReachability(c Candidate, state MachineState) (authSource, skipReason string) {
	switch c.Auth.Kind {
	case "env_var":
		for _, e := range state.Auth.Entries {
			if e.Kind == auth.KindEnvVar &&
				e.Source.EnvVar == c.Auth.EnvVar &&
				e.State == auth.StateOK {
				return c.Auth.EnvVar, ""
			}
		}
		return c.Auth.EnvVar, "env_var_missing:" + c.Auth.EnvVar

	case "cli_session":
		found := false
		for _, e := range state.Auth.Entries {
			if e.Tool == c.Auth.CLI &&
				(e.Kind == auth.KindCLIOAuth || e.Kind == auth.KindKeychain) {
				found = true
				if e.State == auth.StateOK {
					return c.Auth.CLI, ""
				}
			}
		}
		if !found {
			return c.Auth.CLI, "cli_not_installed:" + c.Auth.CLI
		}
		return c.Auth.CLI, "cli_no_session:" + c.Auth.CLI

	case "none":
		return "", ""

	default:
		return "", "unknown_auth_kind:" + c.Auth.Kind
	}
}

func resolveClass(req ResolveRequest, data *Data, state MachineState) (ResolveResult, error) {
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

	// 4. Walk chain, first reachable wins.
	var skipped []SkipRecord
	for i, c := range class.Chain {
		authSource, skipReason := candidateReachability(c, state)
		if skipReason == "" {
			return ResolveResult{
				ModelID:       c.ModelID,
				Driver:        c.Driver,
				Transport:     c.Transport,
				HistorySink:   c.HistorySink,
				AuthMethod:    c.Auth.Kind,
				AuthSource:    authSource,
				RequestType:   "class",
				RequestValue:  originalClassID,
				FallbackRank:  i,
				Skipped:       skipped,
				PolicyVersion: data.PolicyVersion,
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

func resolveStrict(req ResolveRequest, data *Data, state MachineState) (ResolveResult, error) {
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
		authSource, skipReason := candidateReachability(m.c, state)
		if skipReason == "" {
			return ResolveResult{
				ModelID:       m.c.ModelID,
				Driver:        m.c.Driver,
				Transport:     m.c.Transport,
				HistorySink:   m.c.HistorySink,
				AuthMethod:    m.c.Auth.Kind,
				AuthSource:    authSource,
				RequestType:   "strict",
				RequestValue:  req.ModelID,
				FallbackRank:  -1,
				Skipped:       nil,
				PolicyVersion: data.PolicyVersion,
				ResolvedAt:    time.Now(),
			}, nil
		}
		lastReason = skipReason
	}

	// 4. None reachable.
	return ResolveResult{}, ErrStrictModelUnreachable{ModelID: req.ModelID, Reason: lastReason}
}
