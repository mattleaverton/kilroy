package auth

import (
	"strings"
	"testing"
)

// clearEnvVars uses t.Setenv to blank every env var in the table so tests
// are hermetic regardless of the developer's environment.
func clearEnvVars(t *testing.T) {
	t.Helper()
	for _, spec := range envVarTable {
		t.Setenv(spec.name, "")
	}
}

func TestEnvVarDetector(t *testing.T) {
	t.Run("no vars set returns 0 entries", func(t *testing.T) {
		clearEnvVars(t)
		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected 0 entries, got %d: %+v", len(entries), entries)
		}
	})

	t.Run("ANTHROPIC_API_KEY with valid prefix yields ok entry", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-ant-foo-bar-baz")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.Provider != "anthropic" {
			t.Errorf("expected provider=anthropic, got %q", e.Provider)
		}
		if e.State != StateOK {
			t.Errorf("expected state=ok, got %q", e.State)
		}
		if e.Kind != KindEnvVar {
			t.Errorf("expected kind=env_var, got %q", e.Kind)
		}
		if e.Source.EnvVar != "ANTHROPIC_API_KEY" {
			t.Errorf("expected source.env_var=ANTHROPIC_API_KEY, got %q", e.Source.EnvVar)
		}
		if e.Tool != "" {
			t.Errorf("expected tool empty, got %q", e.Tool)
		}
		if e.ID != "anthropic.env.ANTHROPIC_API_KEY" {
			t.Errorf("expected id=anthropic.env.ANTHROPIC_API_KEY, got %q", e.ID)
		}
		// Notes must contain recognized prefix, not the full value.
		found := false
		for _, n := range e.Notes {
			if strings.Contains(n, "sk-ant-") {
				found = true
			}
			if strings.Contains(n, "foo") {
				t.Errorf("note must not contain value beyond recognized prefix: %q", n)
			}
		}
		if !found {
			t.Errorf("expected a note containing the recognized prefix sk-ant-, got %v", e.Notes)
		}
	})

	t.Run("OPENAI_API_KEY with no recognized prefix yields ambiguous entry", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("OPENAI_API_KEY", "garbage-key-value")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.Provider != "openai" {
			t.Errorf("expected provider=openai, got %q", e.Provider)
		}
		if e.State != StateAmbiguous {
			t.Errorf("expected state=ambiguous, got %q", e.State)
		}
		// There must be a note mentioning the prefix mismatch.
		hasMismatchNote := false
		for _, n := range e.Notes {
			if strings.Contains(n, "does not match") {
				hasMismatchNote = true
			}
		}
		if !hasMismatchNote {
			t.Errorf("expected a prefix-mismatch note, got %v", e.Notes)
		}
	})

	t.Run("GH_TOKEN and GITHUB_TOKEN both set yields 2 entries both ok", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("GH_TOKEN", "ghp_xxx111")
		t.Setenv("GITHUB_TOKEN", "ghp_yyy222")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d: %+v", len(entries), entries)
		}
		for _, e := range entries {
			if e.Provider != "github" {
				t.Errorf("expected provider=github, got %q", e.Provider)
			}
			if e.State != StateOK {
				t.Errorf("expected state=ok for %q, got %q", e.Source.EnvVar, e.State)
			}
		}
		// Verify one entry per var.
		vars := map[string]bool{}
		for _, e := range entries {
			vars[e.Source.EnvVar] = true
		}
		if !vars["GH_TOKEN"] {
			t.Error("expected an entry for GH_TOKEN")
		}
		if !vars["GITHUB_TOKEN"] {
			t.Error("expected an entry for GITHUB_TOKEN")
		}
	})

	t.Run("empty ANTHROPIC_API_KEY yields 0 entries", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("ANTHROPIC_API_KEY", "") // explicitly empty

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected 0 entries for empty var, got %d", len(entries))
		}
	})

	t.Run("OPENAI_API_KEY with sk-proj- prefix yields ok", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("OPENAI_API_KEY", "sk-proj-abc123")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].State != StateOK {
			t.Errorf("expected state=ok, got %q", entries[0].State)
		}
	})

	t.Run("OPENAI_API_KEY with sk- prefix (non-proj) yields ok", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("OPENAI_API_KEY", "sk-legacykey")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].State != StateOK {
			t.Errorf("expected state=ok, got %q", entries[0].State)
		}
	})

	t.Run("GEMINI_API_KEY with AIza prefix yields ok google entry", func(t *testing.T) {
		clearEnvVars(t)
		t.Setenv("GEMINI_API_KEY", "AIzaTestKey123")

		d := NewEnvVarDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.Provider != "google" {
			t.Errorf("expected provider=google, got %q", e.Provider)
		}
		if e.State != StateOK {
			t.Errorf("expected state=ok, got %q", e.State)
		}
		if e.ID != "google.env.GEMINI_API_KEY" {
			t.Errorf("expected id=google.env.GEMINI_API_KEY, got %q", e.ID)
		}
	})

	t.Run("Name returns env_vars", func(t *testing.T) {
		d := NewEnvVarDetector()
		if d.Name() != "env_vars" {
			t.Errorf("expected Name()=env_vars, got %q", d.Name())
		}
	})
}
