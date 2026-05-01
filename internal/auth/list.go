// Orchestrator that walks registered detectors and produces a unified report.
// Cross-detector shadowing (env var beats CLI OAuth for same provider) is
// applied here, since individual detectors only know about themselves.

package auth

import (
	"runtime"
	"sort"
	"time"
)

// ListOutput is the top-level shape returned by ListAll, matching plan §8.3.
type ListOutput struct {
	KilroyVersion string  `json:"kilroy_version"`
	ScannedAt     string  `json:"scanned_at"`
	Platform      string  `json:"platform"`
	Entries       []Entry `json:"entries"`
	Summary       Summary `json:"summary"`
}

// Summary aggregates entry counts by state.
type Summary struct {
	Total     int `json:"total"`
	OK        int `json:"ok"`
	Expired   int `json:"expired"`
	Missing   int `json:"missing"`
	Ambiguous int `json:"ambiguous"`
}

// DefaultDetectors returns the v2 baseline detector set. Order matters only
// for deterministic test output; cross-detector shadowing is applied
// independent of order.
func DefaultDetectors() []Detector {
	return []Detector{
		NewEnvVarDetector(),
		NewClaudeDetector(),
		NewCodexDetector(),
		NewGHDetector(),
		NewGeminiDetector(),
		NewAiderDetector(),
		NewOpenCodeDetector(),
		NewCursorDetector(),
	}
}

// ListAll runs every detector, applies cross-detector shadow rules, and
// returns the unified report. Per-detector errors are silently dropped —
// they're reserved for callers that want them.
func ListAll(kilroyVersion string, detectors []Detector) ListOutput {
	var entries []Entry
	for _, d := range detectors {
		got, _ := d.Detect()
		entries = append(entries, got...)
	}

	entries = dedupeEnvVarEntries(entries)
	applyCrossDetectorShadows(entries)

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Provider != entries[j].Provider {
			return entries[i].Provider < entries[j].Provider
		}
		// Env var entries sort first within a provider so the precedence
		// order is visually obvious in --pretty output.
		if entries[i].Kind != entries[j].Kind {
			return kindOrder(entries[i].Kind) < kindOrder(entries[j].Kind)
		}
		return entries[i].ID < entries[j].ID
	})

	return ListOutput{
		KilroyVersion: kilroyVersion,
		ScannedAt:     time.Now().UTC().Format(time.RFC3339),
		Platform:      runtime.GOOS,
		Entries:       entries,
		Summary:       summarize(entries),
	}
}

// dedupeEnvVarEntries removes redundant env_var entries emitted by per-tool
// detectors when EnvVarDetector has already emitted the canonical entry.
// Per-tool detectors emit env_var entries to indicate "this tool sees an
// env var override," but EnvVarDetector emits the same env var with an
// empty Tool field as the canonical entry. Keeping both is just noise —
// agent callers should see one entry per env var, not one per tool.
//
// The canonical entry (Tool == "") wins; tool-specific copies are dropped.
func dedupeEnvVarEntries(entries []Entry) []Entry {
	canonical := map[string]bool{} // env var name → canonical entry exists
	for _, e := range entries {
		if e.Kind == KindEnvVar && e.Tool == "" && e.Source.EnvVar != "" {
			canonical[e.Source.EnvVar] = true
		}
	}
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if e.Kind == KindEnvVar && e.Tool != "" && canonical[e.Source.EnvVar] {
			continue // drop redundant tool-specific copy
		}
		out = append(out, e)
	}
	return out
}

// applyCrossDetectorShadows is a same-provider rule: an env_var entry shadows
// any non-env_var entry for the same provider, since the env var wins at
// runtime invocation. Detectors emit Shadows/ShadowedBy only for their own
// internal pairs (e.g. ClaudeDetector emits both an env entry AND a cli_oauth
// entry); this orchestrator catches the cross-detector case (e.g. EnvVarDetector
// emits ANTHROPIC_API_KEY, ClaudeDetector emits the keychain entry — they need
// to be linked).
//
// Implementation: for each non-env_var entry, find any env_var entry with the
// same provider; if found, link them (and only if not already linked).
func applyCrossDetectorShadows(entries []Entry) {
	envByProvider := map[string][]int{} // provider → indices of env_var entries
	for i, e := range entries {
		if e.Kind == KindEnvVar {
			envByProvider[e.Provider] = append(envByProvider[e.Provider], i)
		}
	}
	for i, e := range entries {
		if e.Kind == KindEnvVar {
			continue
		}
		envIdxs, ok := envByProvider[e.Provider]
		if !ok {
			continue
		}
		for _, ei := range envIdxs {
			if ei == i {
				continue
			}
			// env shadows cli
			if !contains(entries[ei].Shadows, e.ID) {
				entries[ei].Shadows = append(entries[ei].Shadows, e.ID)
			}
			if !contains(entries[i].ShadowedBy, entries[ei].ID) {
				entries[i].ShadowedBy = append(entries[i].ShadowedBy, entries[ei].ID)
			}
		}
	}
}

func summarize(entries []Entry) Summary {
	s := Summary{Total: len(entries)}
	for _, e := range entries {
		switch e.State {
		case StateOK:
			s.OK++
		case StateExpired:
			s.Expired++
		case StateMissing:
			s.Missing++
		case StateAmbiguous:
			s.Ambiguous++
		}
	}
	return s
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func kindOrder(k Kind) int {
	switch k {
	case KindEnvVar:
		return 0
	case KindCLIOAuth:
		return 1
	case KindKeychain:
		return 2
	case KindCLIAPIKey:
		return 3
	case KindAPIKeyFile:
		return 4
	}
	return 99
}
