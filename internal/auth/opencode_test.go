package auth

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// newTestDetector returns an OpenCodeDetector pointing at the given DB path.
func newTestDetector(path string) *OpenCodeDetector {
	return &OpenCodeDetector{dbPath: path}
}

func createDB(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "opencode.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE account (email TEXT, url TEXT, token_expiry INT, active INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return path
}

// TestOpenCodeDetector_DBAbsent verifies that a missing DB returns 0 entries.
func TestOpenCodeDetector_DBAbsent(t *testing.T) {
	dir := t.TempDir()
	det := newTestDetector(filepath.Join(dir, "nonexistent.db"))
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for absent DB, got %d", len(entries))
	}
}

// TestOpenCodeDetector_EmptyTable verifies that an empty account table returns 0 entries.
func TestOpenCodeDetector_EmptyTable(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)
	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for empty table, got %d", len(entries))
	}
}

// TestOpenCodeDetector_ActiveFutureExpiry verifies a valid active account row returns ok.
func TestOpenCodeDetector_ActiveFutureExpiry(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	_, err = db.Exec(`INSERT INTO account (email, url, token_expiry, active) VALUES (?, ?, ?, ?)`,
		"user@anthropic.com", "https://api.anthropic.com", futureExpiry, 1)
	db.Close()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.State != StateOK {
		t.Errorf("expected state ok, got %q", e.State)
	}
	if e.Identity.Email != "user@anthropic.com" {
		t.Errorf("expected email user@anthropic.com, got %q", e.Identity.Email)
	}
	if e.Provider != "anthropic" {
		t.Errorf("expected provider anthropic, got %q", e.Provider)
	}
	if e.Tool != "opencode" {
		t.Errorf("expected tool opencode, got %q", e.Tool)
	}
}

// TestOpenCodeDetector_ActiveExpiredToken verifies that an expired token returns state expired.
func TestOpenCodeDetector_ActiveExpiredToken(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	pastExpiry := time.Now().Add(-24 * time.Hour).Unix()
	_, err = db.Exec(`INSERT INTO account (email, url, token_expiry, active) VALUES (?, ?, ?, ?)`,
		"user@openai.com", "https://api.openai.com", pastExpiry, 1)
	db.Close()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].State != StateExpired {
		t.Errorf("expected state expired, got %q", entries[0].State)
	}
	if entries[0].Provider != "openai" {
		t.Errorf("expected provider openai, got %q", entries[0].Provider)
	}
}

// TestOpenCodeDetector_InactiveRow verifies that inactive rows are ignored.
func TestOpenCodeDetector_InactiveRow(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	_, err = db.Exec(`INSERT INTO account (email, url, token_expiry, active) VALUES (?, ?, ?, ?)`,
		"user@anthropic.com", "https://api.anthropic.com", futureExpiry, 0)
	db.Close()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for inactive row, got %d", len(entries))
	}
}

// TestOpenCodeDetector_BadSchema verifies that a corrupt/bad-schema DB returns 1 ambiguous entry.
func TestOpenCodeDetector_BadSchema(t *testing.T) {
	dir := t.TempDir()
	// Write garbage bytes that look like a file but aren't a valid SQLite DB.
	path := filepath.Join(dir, "opencode.db")
	if err := os.WriteFile(path, []byte("this is not a sqlite database"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 ambiguous entry for bad schema, got %d", len(entries))
	}
	if entries[0].State != StateAmbiguous {
		t.Errorf("expected ambiguous state, got %q", entries[0].State)
	}
}
