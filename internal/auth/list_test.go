package auth

import (
	"testing"
)

// stubDetector returns a fixed list of entries on each Detect call.
type stubDetector struct {
	name    string
	entries []Entry
}

func (s *stubDetector) Name() string                  { return s.name }
func (s *stubDetector) Detect() ([]Entry, error)      { return s.entries, nil }

// TestListAll_DedupesEnvVarEntriesAcrossDetectors verifies that when
// EnvVarDetector emits a canonical (Tool="") entry for an env var AND a
// per-tool detector also emits a tool-tagged copy of the same env var, the
// orchestrator drops the redundant tool-tagged copy. This was a real bug
// surfaced when running `kilroy auth list` against a developer machine that
// had ANTHROPIC_API_KEY set: both EnvVarDetector and ClaudeDetector emitted
// it, producing two visually-identical lines.
func TestListAll_DedupesEnvVarEntriesAcrossDetectors(t *testing.T) {
	envCanonical := Entry{
		ID:       "anthropic.env.ANTHROPIC_API_KEY",
		Kind:     KindEnvVar,
		Provider: "anthropic",
		// Tool intentionally empty — canonical env var entry.
		State:  StateOK,
		Source: Source{EnvVar: "ANTHROPIC_API_KEY"},
	}
	envToolTagged := Entry{
		ID:       "anthropic.claude.env.ANTHROPIC_API_KEY",
		Kind:     KindEnvVar,
		Provider: "anthropic",
		Tool:     "claude", // redundant tool-tagged copy
		State:    StateOK,
		Source:   Source{EnvVar: "ANTHROPIC_API_KEY"},
	}
	cliEntry := Entry{
		ID:       "anthropic.claude.cli_oauth",
		Kind:     KindCLIOAuth,
		Provider: "anthropic",
		Tool:     "claude",
		State:    StateOK,
		Source:   Source{File: "/home/u/.claude/settings.json"},
	}

	dets := []Detector{
		&stubDetector{name: "env_vars", entries: []Entry{envCanonical}},
		&stubDetector{name: "claude", entries: []Entry{envToolTagged, cliEntry}},
	}

	out := ListAll("test-version", dets)

	// Should be 2 entries: canonical env var + cli oauth. The tool-tagged
	// duplicate must have been dropped.
	if len(out.Entries) != 2 {
		t.Fatalf("expected 2 entries after dedup, got %d", len(out.Entries))
	}

	var sawEnvCanonical, sawCLI, sawDuplicate bool
	for _, e := range out.Entries {
		switch e.ID {
		case envCanonical.ID:
			sawEnvCanonical = true
		case envToolTagged.ID:
			sawDuplicate = true
		case cliEntry.ID:
			sawCLI = true
		}
	}
	if !sawEnvCanonical {
		t.Error("canonical env var entry was dropped (must be preserved)")
	}
	if !sawCLI {
		t.Error("cli_oauth entry was dropped (must be preserved)")
	}
	if sawDuplicate {
		t.Error("tool-tagged duplicate env var entry should have been dropped")
	}
}

// TestListAll_AppliesCrossDetectorShadows verifies that an env var entry
// from one detector shadows a non-env entry from a different detector when
// both target the same provider — even though neither detector emitted
// Shadows/ShadowedBy fields itself.
func TestListAll_AppliesCrossDetectorShadows(t *testing.T) {
	envEntry := Entry{
		ID:       "openai.env.OPENAI_API_KEY",
		Kind:     KindEnvVar,
		Provider: "openai",
		State:    StateOK,
		Source:   Source{EnvVar: "OPENAI_API_KEY"},
	}
	cliEntry := Entry{
		ID:       "openai.codex.cli",
		Kind:     KindCLIOAuth,
		Provider: "openai",
		Tool:     "codex",
		State:    StateOK,
	}

	out := ListAll("test-version", []Detector{
		&stubDetector{name: "env_vars", entries: []Entry{envEntry}},
		&stubDetector{name: "codex", entries: []Entry{cliEntry}},
	})

	if len(out.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out.Entries))
	}

	var env, cli *Entry
	for i := range out.Entries {
		switch out.Entries[i].Kind {
		case KindEnvVar:
			env = &out.Entries[i]
		case KindCLIOAuth:
			cli = &out.Entries[i]
		}
	}
	if env == nil || cli == nil {
		t.Fatal("missing env or cli entry after ListAll")
	}
	if len(env.Shadows) != 1 || env.Shadows[0] != cli.ID {
		t.Errorf("env entry should shadow cli entry; got Shadows=%v", env.Shadows)
	}
	if len(cli.ShadowedBy) != 1 || cli.ShadowedBy[0] != env.ID {
		t.Errorf("cli entry should be shadowed by env entry; got ShadowedBy=%v", cli.ShadowedBy)
	}
}

// TestListAll_SummaryCountsByState verifies the Summary aggregation.
func TestListAll_SummaryCountsByState(t *testing.T) {
	entries := []Entry{
		{ID: "1", Kind: KindEnvVar, Provider: "a", State: StateOK},
		{ID: "2", Kind: KindCLIOAuth, Provider: "b", State: StateOK},
		{ID: "3", Kind: KindCLIOAuth, Provider: "c", State: StateExpired},
		{ID: "4", Kind: KindCLIOAuth, Provider: "d", State: StateAmbiguous},
	}
	out := ListAll("v", []Detector{&stubDetector{name: "stub", entries: entries}})
	if got, want := out.Summary.Total, 4; got != want {
		t.Errorf("Total = %d, want %d", got, want)
	}
	if got, want := out.Summary.OK, 2; got != want {
		t.Errorf("OK = %d, want %d", got, want)
	}
	if got, want := out.Summary.Expired, 1; got != want {
		t.Errorf("Expired = %d, want %d", got, want)
	}
	if got, want := out.Summary.Ambiguous, 1; got != want {
		t.Errorf("Ambiguous = %d, want %d", got, want)
	}
	if got, want := out.Summary.Missing, 0; got != want {
		t.Errorf("Missing = %d, want %d", got, want)
	}
}
