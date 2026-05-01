package auth

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// openCodeDBPath is the default path to the OpenCode SQLite database.
// Overridable in tests.
var openCodeDBPath = filepath.Join(mustUserHomeDir(), ".local", "share", "opencode", "opencode.db")

func mustUserHomeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

// OpenCodeDetector discovers auth entries from OpenCode's SQLite database.
type OpenCodeDetector struct {
	dbPath string // empty means use openCodeDBPath
}

// NewOpenCodeDetector returns a new OpenCodeDetector using the default DB path.
func NewOpenCodeDetector() *OpenCodeDetector { return &OpenCodeDetector{} }

// Name implements Detector.
func (d *OpenCodeDetector) Name() string { return "opencode" }

// effectivePath returns the DB path to use.
func (d *OpenCodeDetector) effectivePath() string {
	if d.dbPath != "" {
		return d.dbPath
	}
	return openCodeDBPath
}

// Detect implements Detector.
func (d *OpenCodeDetector) Detect() ([]Entry, error) {
	dbPath := d.effectivePath()

	// DB absent → return empty slice (env-var detector handles fallback).
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, nil
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return ambiguousEntry(dbPath, fmt.Sprintf("cannot open OpenCode DB: %v", err)), nil
	}
	defer db.Close()

	rows, err := db.Query(`SELECT email, url, token_expiry, active FROM account`)
	if err != nil {
		return ambiguousEntry(dbPath, fmt.Sprintf("cannot query account table: %v", err)), nil
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var email, url string
		var tokenExpiry int64
		var active int
		if err := rows.Scan(&email, &url, &tokenExpiry, &active); err != nil {
			return ambiguousEntry(dbPath, fmt.Sprintf("cannot scan account row: %v", err)), nil
		}
		if active != 1 {
			continue
		}

		provider := providerFromURL(url)
		state := StateOK
		expiry := &Expiry{}

		if tokenExpiry > 0 {
			expiresAt := time.Unix(tokenExpiry, 0)
			expiry.AccessTokenExpiresAt = &expiresAt
			if time.Now().Unix() >= tokenExpiry {
				state = StateExpired
			}
		}

		entry := Entry{
			ID:       fmt.Sprintf("%s.opencode.account", provider),
			Kind:     KindCLIOAuth,
			Provider: provider,
			Tool:     "opencode",
			State:    state,
			Identity: Identity{Email: email},
			Expiry:   expiry,
			Source:   Source{File: dbPath},
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return ambiguousEntry(dbPath, fmt.Sprintf("error iterating account rows: %v", err)), nil
	}

	return entries, nil
}

// providerFromURL derives a provider name from the account URL.
func providerFromURL(url string) string {
	lurl := strings.ToLower(url)
	switch {
	case strings.Contains(lurl, "anthropic"):
		return "anthropic"
	case strings.Contains(lurl, "openai"):
		return "openai"
	default:
		return "unknown"
	}
}

// ambiguousEntry returns a single ambiguous Entry indicating the DB could not be read.
func ambiguousEntry(dbPath, note string) []Entry {
	return []Entry{
		{
			ID:          "unknown.opencode.account",
			Kind:        KindCLIOAuth,
			Provider:    "unknown",
			Tool:        "opencode",
			State:       StateAmbiguous,
			Source:      Source{File: dbPath},
			Notes:       []string{note},
			Remediation: "OpenCode DB schema may have changed; report at https://github.com/danshapiro/kilroy/issues. As a workaround, set provider env vars (ANTHROPIC_API_KEY, OPENAI_API_KEY).",
		},
	}
}
