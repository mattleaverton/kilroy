package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeCursorConfig writes a cli-config.json with the given email into dir.
// If email is empty string, the authInfo block is present but email is "".
func writeCursorConfig(t *testing.T, dir, email string) string {
	t.Helper()
	type authInfo struct {
		Email string `json:"email"`
	}
	type cfg struct {
		AuthInfo authInfo `json:"authInfo"`
	}
	data, err := json.Marshal(cfg{AuthInfo: authInfo{Email: email}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cli-config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// stubKeychain returns a keychainProbeCursor stub where access and refresh
// results are controlled by the caller.
func stubKeychain(access, refresh bool) func(service, account string) bool {
	return func(service, account string) bool {
		switch service {
		case "cursor-access-token":
			return access
		case "cursor-refresh-token":
			return refresh
		}
		return false
	}
}

func TestCursorDetector(t *testing.T) {
	// Save and restore package-level vars after each sub-test.
	origPath := cursorCLIConfigPath
	origProbe := keychainProbeCursor
	t.Cleanup(func() {
		cursorCLIConfigPath = origPath
		keychainProbeCursor = origProbe
	})

	t.Run("config absent → missing", func(t *testing.T) {
		dir := t.TempDir()
		cursorCLIConfigPath = filepath.Join(dir, "cli-config.json") // does not exist
		keychainProbeCursor = stubKeychain(false, false)

		d := NewCursorDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].State != StateMissing {
			t.Errorf("expected state=missing, got %q", entries[0].State)
		}
	})

	t.Run("config present + valid email + access token true → ok", func(t *testing.T) {
		dir := t.TempDir()
		cursorCLIConfigPath = writeCursorConfig(t, dir, "user@example.com")
		keychainProbeCursor = stubKeychain(true, false)

		d := NewCursorDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.State != StateOK {
			t.Errorf("expected state=ok, got %q", e.State)
		}
		if e.Identity.Email != "user@example.com" {
			t.Errorf("expected email=user@example.com, got %q", e.Identity.Email)
		}
		if len(e.Notes) != 0 {
			t.Errorf("expected no notes, got %v", e.Notes)
		}
	})

	t.Run("config present + valid email + access false + refresh true → ok with note", func(t *testing.T) {
		dir := t.TempDir()
		cursorCLIConfigPath = writeCursorConfig(t, dir, "user@example.com")
		keychainProbeCursor = stubKeychain(false, true)

		d := NewCursorDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.State != StateOK {
			t.Errorf("expected state=ok, got %q", e.State)
		}
		if len(e.Notes) == 0 {
			t.Fatal("expected a note about access_token missing")
		}
		if e.Notes[0] != "access_token missing; refresh_token present" {
			t.Errorf("unexpected note: %q", e.Notes[0])
		}
	})

	t.Run("config present + valid email + both probes false → expired", func(t *testing.T) {
		dir := t.TempDir()
		cursorCLIConfigPath = writeCursorConfig(t, dir, "user@example.com")
		keychainProbeCursor = stubKeychain(false, false)

		d := NewCursorDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].State != StateExpired {
			t.Errorf("expected state=expired, got %q", entries[0].State)
		}
	})

	t.Run("config present + empty email → ambiguous", func(t *testing.T) {
		dir := t.TempDir()
		cursorCLIConfigPath = writeCursorConfig(t, dir, "")
		keychainProbeCursor = stubKeychain(false, false)

		d := NewCursorDetector()
		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.State != StateAmbiguous {
			t.Errorf("expected state=ambiguous, got %q", e.State)
		}
		if e.Remediation == "" {
			t.Error("expected non-empty remediation for ambiguous state")
		}
	})
}
