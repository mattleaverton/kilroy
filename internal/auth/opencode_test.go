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
	if _, err := db.Exec(`CREATE TABLE account (
		id TEXT PRIMARY KEY,
		email TEXT NOT NULL,
		url TEXT NOT NULL,
		access_token TEXT NOT NULL,
		refresh_token TEXT NOT NULL,
		token_expiry INTEGER,
		time_created INTEGER NOT NULL,
		time_updated INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE account_state (
		active_account_id TEXT
	)`); err != nil {
		t.Fatalf("create account_state: %v", err)
	}
	return path
}

// setActiveAccount writes a row to account_state.active_account_id.
func setActiveAccount(t *testing.T, path, accountID string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO account_state (active_account_id) VALUES (?)`, accountID); err != nil {
		t.Fatalf("insert account_state: %v", err)
	}
}

// insertAccount inserts a row into the account table with the columns the
// detector cares about; secret-bearing columns get placeholder values.
func insertAccount(t *testing.T, path, id, email, url string, tokenExpiry int64) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	now := time.Now().Unix()
	_, err = db.Exec(`INSERT INTO account
		(id, email, url, access_token, refresh_token, token_expiry, time_created, time_updated)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, email, url, "access-placeholder", "refresh-placeholder", tokenExpiry, now, now)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
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

// TestOpenCodeDetector_ActiveFutureExpiry verifies a valid account row returns ok.
func TestOpenCodeDetector_ActiveFutureExpiry(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)

	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	insertAccount(t, path, "acct-1", "user@anthropic.com", "https://api.anthropic.com", futureExpiry)

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

	pastExpiry := time.Now().Add(-24 * time.Hour).Unix()
	insertAccount(t, path, "acct-1", "user@openai.com", "https://api.openai.com", pastExpiry)

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

// TestOpenCodeDetector_NullExpiry verifies that an account with NULL token_expiry is reported as ok.
func TestOpenCodeDetector_NullExpiry(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Now().Unix()
	_, err = db.Exec(`INSERT INTO account
		(id, email, url, access_token, refresh_token, token_expiry, time_created, time_updated)
		VALUES (?, ?, ?, ?, ?, NULL, ?, ?)`,
		"acct-1", "user@anthropic.com", "https://api.anthropic.com", "a", "r", now, now)
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
	if entries[0].State != StateOK {
		t.Errorf("expected state ok for NULL expiry, got %q", entries[0].State)
	}
	if entries[0].Expiry == nil || entries[0].Expiry.AccessTokenExpiresAt != nil {
		t.Errorf("expected no AccessTokenExpiresAt for NULL token_expiry, got %+v", entries[0].Expiry)
	}
}

// TestOpenCodeDetector_ActiveAccountAnnotation verifies that the row matched
// by account_state.active_account_id is tagged "active in opencode" while
// other rows are not.
func TestOpenCodeDetector_ActiveAccountAnnotation(t *testing.T) {
	dir := t.TempDir()
	path := createDB(t, dir)
	future := time.Now().Add(time.Hour).Unix()
	insertAccount(t, path, "acct-anth", "u@anthropic.com", "https://api.anthropic.com", future)
	insertAccount(t, path, "acct-oai", "u@openai.com", "https://api.openai.com", future)
	setActiveAccount(t, path, "acct-anth")

	det := newTestDetector(path)
	entries, err := det.Detect()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	var anth, oai *Entry
	for i := range entries {
		switch entries[i].Provider {
		case "anthropic":
			anth = &entries[i]
		case "openai":
			oai = &entries[i]
		}
	}
	if anth == nil || oai == nil {
		t.Fatalf("missing expected providers in entries: %+v", entries)
	}
	hasActive := func(e *Entry) bool {
		for _, n := range e.Notes {
			if n == "active in opencode" {
				return true
			}
		}
		return false
	}
	if !hasActive(anth) {
		t.Errorf("anthropic entry should be tagged active, got notes %+v", anth.Notes)
	}
	if hasActive(oai) {
		t.Errorf("openai entry should NOT be tagged active, got notes %+v", oai.Notes)
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
