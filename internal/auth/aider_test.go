package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withAiderPaths temporarily overrides the global and project config paths for
// the duration of a test and restores them when the test ends.
func withAiderPaths(t *testing.T, globalPath, projectPath string) {
	t.Helper()
	origGlobal := aiderGlobalPath
	origProject := aiderProjectPath
	aiderGlobalPath = globalPath
	aiderProjectPath = projectPath
	t.Cleanup(func() {
		aiderGlobalPath = origGlobal
		aiderProjectPath = origProject
	})
}

func TestAiderDetector(t *testing.T) {
	t.Run("name", func(t *testing.T) {
		d := NewAiderDetector()
		if d.Name() != "aider" {
			t.Errorf("Name() = %q, want %q", d.Name(), "aider")
		}
	})

	t.Run("no_config_files_returns_empty", func(t *testing.T) {
		tmp := t.TempDir()
		withAiderPaths(t,
			filepath.Join(tmp, "global.yml"),
			filepath.Join(tmp, "project.yml"),
		)

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries, got %d: %+v", len(entries), entries)
		}
	})

	t.Run("global_with_anthropic_key", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		withAiderPaths(t, globalFile, filepath.Join(tmp, "project.yml"))

		writeFile(t, globalFile, "anthropic-api-key: sk-ant-test123\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.Provider != "anthropic" {
			t.Errorf("Provider = %q, want %q", e.Provider, "anthropic")
		}
		if e.State != StateOK {
			t.Errorf("State = %q, want %q", e.State, StateOK)
		}
		if e.Kind != KindCLIAPIKey {
			t.Errorf("Kind = %q, want %q", e.Kind, KindCLIAPIKey)
		}
		if e.Tool != "aider" {
			t.Errorf("Tool = %q, want %q", e.Tool, "aider")
		}
		if e.Source.File != globalFile {
			t.Errorf("Source.File = %q, want %q", e.Source.File, globalFile)
		}
	})

	t.Run("global_with_multiple_keys", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		withAiderPaths(t, globalFile, filepath.Join(tmp, "project.yml"))

		writeFile(t, globalFile,
			"anthropic-api-key: sk-ant-test\n"+
				"openai-api-key: sk-openai-test\n"+
				"gemini-api-key: AIza-test\n",
		)

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d: %+v", len(entries), entries)
		}

		providers := make(map[string]bool)
		for _, e := range entries {
			if e.State != StateOK {
				t.Errorf("entry %q: State = %q, want %q", e.ID, e.State, StateOK)
			}
			if e.Kind != KindCLIAPIKey {
				t.Errorf("entry %q: Kind = %q, want %q", e.ID, e.Kind, KindCLIAPIKey)
			}
			if e.Tool != "aider" {
				t.Errorf("entry %q: Tool = %q, want %q", e.ID, e.Tool, "aider")
			}
			providers[e.Provider] = true
		}
		for _, want := range []string{"anthropic", "openai", "google"} {
			if !providers[want] {
				t.Errorf("missing provider %q in entries", want)
			}
		}
	})

	t.Run("project_shadows_global_for_same_key", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		projectFile := filepath.Join(tmp, "project.yml")
		withAiderPaths(t, globalFile, projectFile)

		writeFile(t, globalFile, "anthropic-api-key: sk-ant-global\n")
		writeFile(t, projectFile, "anthropic-api-key: sk-ant-project\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry (project shadows global), got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.Provider != "anthropic" {
			t.Errorf("Provider = %q, want %q", e.Provider, "anthropic")
		}
		if e.Source.File != projectFile {
			t.Errorf("Source.File = %q, want %q (project)", e.Source.File, projectFile)
		}
		if e.State != StateOK {
			t.Errorf("State = %q, want %q", e.State, StateOK)
		}
		// Must carry shadow note
		if len(e.Notes) == 0 {
			t.Errorf("expected Notes to contain shadow message, got none")
		} else {
			found := false
			for _, n := range e.Notes {
				if strings.Contains(n, "shadows") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected a 'shadows' note, got %v", e.Notes)
			}
		}
		// Shadows slice must reference global entry ID
		if len(e.Shadows) == 0 {
			t.Errorf("expected Shadows to be non-empty")
		}
	})

	t.Run("project_and_global_different_keys_both_emitted", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		projectFile := filepath.Join(tmp, "project.yml")
		withAiderPaths(t, globalFile, projectFile)

		writeFile(t, globalFile, "openai-api-key: sk-openai-global\n")
		writeFile(t, projectFile, "anthropic-api-key: sk-ant-project\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d: %+v", len(entries), entries)
		}
		providers := make(map[string]bool)
		for _, e := range entries {
			providers[e.Provider] = true
			if e.State != StateOK {
				t.Errorf("entry %q: State = %q, want %q", e.ID, e.State, StateOK)
			}
		}
		if !providers["anthropic"] {
			t.Error("missing anthropic entry")
		}
		if !providers["openai"] {
			t.Error("missing openai entry")
		}
	})

	t.Run("malformed_global_yaml_emits_ambiguous", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		withAiderPaths(t, globalFile, filepath.Join(tmp, "project.yml"))

		writeFile(t, globalFile, "anthropic-api-key: [unclosed bracket\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 ambiguous entry, got %d: %+v", len(entries), entries)
		}
		e := entries[0]
		if e.State != StateAmbiguous {
			t.Errorf("State = %q, want %q", e.State, StateAmbiguous)
		}
		if e.Tool != "aider" {
			t.Errorf("Tool = %q, want %q", e.Tool, "aider")
		}
		if !strings.Contains(e.Remediation, "malformed") {
			t.Errorf("Remediation %q does not contain 'malformed'", e.Remediation)
		}
		if !strings.Contains(e.Remediation, globalFile) {
			t.Errorf("Remediation %q does not contain path %q", e.Remediation, globalFile)
		}
	})

	t.Run("empty_key_value_not_emitted", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		withAiderPaths(t, globalFile, filepath.Join(tmp, "project.yml"))

		// Key present but value is empty string
		writeFile(t, globalFile, "anthropic-api-key: \"\"\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries for empty key value, got %d: %+v", len(entries), entries)
		}
	})

	t.Run("absent_key_not_emitted", func(t *testing.T) {
		tmp := t.TempDir()
		globalFile := filepath.Join(tmp, "global.yml")
		withAiderPaths(t, globalFile, filepath.Join(tmp, "project.yml"))

		// File exists but has no provider keys
		writeFile(t, globalFile, "model: claude-3-5-sonnet\n")

		entries, err := NewAiderDetector().Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries for absent keys, got %d: %+v", len(entries), entries)
		}
	})
}

// writeFile is a test helper that writes content to path, failing t on error.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("writeFile(%q): %v", path, err)
	}
}
