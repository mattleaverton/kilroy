package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClaudeDetector(t *testing.T) {
	origSettingsPath := claudeSettingsPath
	origKeychainProbe := keychainProbeClaude
	t.Cleanup(func() {
		claudeSettingsPath = origSettingsPath
		keychainProbeClaude = origKeychainProbe
		os.Unsetenv("ANTHROPIC_API_KEY")
	})

	// helpers
	writeSettings := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		settingsDir := filepath.Join(dir, ".claude")
		if err := os.MkdirAll(settingsDir, 0755); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(settingsDir, "settings.json")
		if err := os.WriteFile(p, []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Test 1: No settings.json + no ANTHROPIC_API_KEY → single missing entry.
	t.Run("no_settings_no_env", func(t *testing.T) {
		dir := t.TempDir()
		claudeSettingsPath = filepath.Join(dir, ".claude", "settings.json")
		keychainProbeClaude = func(_, _ string) bool { return false }
		os.Unsetenv("ANTHROPIC_API_KEY")

		d := NewClaudeDetector()
		if d.Name() != "claude" {
			t.Fatalf("Name() = %q, want %q", d.Name(), "claude")
		}
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("want 1 entry, got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.State != StateMissing {
			t.Errorf("State = %q, want %q", e.State, StateMissing)
		}
		if e.Kind != KindCLIOAuth {
			t.Errorf("Kind = %q, want %q", e.Kind, KindCLIOAuth)
		}
		if e.Source.File != claudeSettingsPath {
			t.Errorf("Source.File = %q, want %q", e.Source.File, claudeSettingsPath)
		}
	})

	// Test 2: settings.json exists + keychain stub true → state: ok.
	t.Run("settings_keychain_ok", func(t *testing.T) {
		claudeSettingsPath = writeSettings(t)
		keychainProbeClaude = func(_, _ string) bool { return true }
		os.Unsetenv("ANTHROPIC_API_KEY")

		entries, err := NewClaudeDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("want 1 entry, got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.State != StateOK {
			t.Errorf("State = %q, want %q", e.State, StateOK)
		}
		if e.Kind != KindCLIOAuth {
			t.Errorf("Kind = %q, want %q", e.Kind, KindCLIOAuth)
		}
	})

	// Test 3: settings.json exists + keychain false + ANTHROPIC_API_KEY set →
	//   2 entries: env (ok, shadows cli) + cli_oauth (ok with note).
	t.Run("settings_no_keychain_env_set", func(t *testing.T) {
		claudeSettingsPath = writeSettings(t)
		keychainProbeClaude = func(_, _ string) bool { return false }
		t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-key-abc123")

		entries, err := NewClaudeDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("want 2 entries, got %d: %+v", len(entries), entries)
		}

		// entries[0]: env var
		envE := entries[0]
		if envE.Kind != KindEnvVar {
			t.Errorf("entries[0].Kind = %q, want %q", envE.Kind, KindEnvVar)
		}
		if envE.State != StateOK {
			t.Errorf("entries[0].State = %q, want %q", envE.State, StateOK)
		}
		if envE.ID != claudeEnvEntryID {
			t.Errorf("entries[0].ID = %q, want %q", envE.ID, claudeEnvEntryID)
		}
		if len(envE.Shadows) != 1 || envE.Shadows[0] != claudeCLIOAuthEntryID {
			t.Errorf("entries[0].Shadows = %v, want [%q]", envE.Shadows, claudeCLIOAuthEntryID)
		}

		// entries[1]: cli_oauth
		cliE := entries[1]
		if cliE.Kind != KindCLIOAuth {
			t.Errorf("entries[1].Kind = %q, want %q", cliE.Kind, KindCLIOAuth)
		}
		if cliE.State != StateOK {
			t.Errorf("entries[1].State = %q, want %q", cliE.State, StateOK)
		}
		if cliE.ID != claudeCLIOAuthEntryID {
			t.Errorf("entries[1].ID = %q, want %q", cliE.ID, claudeCLIOAuthEntryID)
		}
		if len(cliE.ShadowedBy) != 1 || cliE.ShadowedBy[0] != claudeEnvEntryID {
			t.Errorf("entries[1].ShadowedBy = %v, want [%q]", cliE.ShadowedBy, claudeEnvEntryID)
		}
		if len(cliE.Notes) == 0 {
			t.Error("entries[1].Notes should be non-empty (env var override note)")
		}
	})

	// Test 4: settings.json exists + keychain false + no env → state: ambiguous, remediation set.
	t.Run("settings_no_keychain_no_env", func(t *testing.T) {
		claudeSettingsPath = writeSettings(t)
		keychainProbeClaude = func(_, _ string) bool { return false }
		os.Unsetenv("ANTHROPIC_API_KEY")

		entries, err := NewClaudeDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("want 1 entry, got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.State != StateAmbiguous {
			t.Errorf("State = %q, want %q", e.State, StateAmbiguous)
		}
		if e.Remediation == "" {
			t.Error("Remediation should be non-empty for ambiguous state")
		}
		if e.Kind != KindCLIOAuth {
			t.Errorf("Kind = %q, want %q", e.Kind, KindCLIOAuth)
		}
	})
}
