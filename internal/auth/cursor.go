package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// cursorCLIConfigPath is the path to Cursor's CLI config JSON.
// Override in tests via this package-level variable.
var cursorCLIConfigPath = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cursor", "cli-config.json")
}()

// keychainProbeCursor probes the macOS Keychain for a service/account pair.
// Returns true if the item exists (token present). Override in tests.
var keychainProbeCursor func(service, account string) bool = defaultKeychainProbeCursor

// defaultKeychainProbeCursor probes the macOS Keychain using security(1).
// It never reads or returns the credential value — only checks presence.
func defaultKeychainProbeCursor(service, account string) bool {
	err := exec.Command(
		"security", "find-generic-password",
		"-s", service,
		"-a", account,
	).Run()
	return err == nil
}

// cursorCLIConfig mirrors the subset of cli-config.json we care about.
type cursorCLIConfig struct {
	AuthInfo struct {
		Email string `json:"email"`
	} `json:"authInfo"`
}

// CursorDetector detects Cursor authentication state.
type CursorDetector struct{}

// NewCursorDetector constructs a CursorDetector.
func NewCursorDetector() *CursorDetector { return &CursorDetector{} }

// Name returns the stable detector name.
func (d *CursorDetector) Name() string { return "cursor" }

// Detect scans for Cursor credentials and returns zero or more Entries.
func (d *CursorDetector) Detect() ([]Entry, error) {
	cfgPath := cursorCLIConfigPath

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []Entry{
				{
					ID:       "cursor.cli_oauth",
					Kind:     KindCLIOAuth,
					Provider: "cursor",
					Tool:     "cursor",
					State:    StateMissing,
					Source:   Source{File: cfgPath},
				},
			}, nil
		}
		// File present but unreadable: surface as ambiguous (not a hard error
		// that the orchestrator drops on the floor).
		return []Entry{{
			ID:          "cursor.cli_oauth",
			Kind:        KindCLIOAuth,
			Provider:    "cursor",
			Tool:        "cursor",
			State:       StateAmbiguous,
			Source:      Source{File: cfgPath},
			Notes:       []string{fmt.Sprintf("cli-config.json unreadable: %v", err)},
			Remediation: "Check file permissions on " + cfgPath,
		}}, nil
	}

	var cfg cursorCLIConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return []Entry{{
			ID:          "cursor.cli_oauth",
			Kind:        KindCLIOAuth,
			Provider:    "cursor",
			Tool:        "cursor",
			State:       StateAmbiguous,
			Source:      Source{File: cfgPath},
			Notes:       []string{fmt.Sprintf("cli-config.json malformed: %v", err)},
			Remediation: "Re-login in the Cursor app to regenerate cli-config.json",
		}}, nil
	}

	email := cfg.AuthInfo.Email

	if email == "" {
		return []Entry{
			{
				ID:          "cursor.cli_oauth",
				Kind:        KindCLIOAuth,
				Provider:    "cursor",
				Tool:        "cursor",
				State:       StateAmbiguous,
				Source:      Source{File: cfgPath},
				Remediation: "Re-login in Cursor app",
			},
		}, nil
	}

	// Email present — probe keychain.
	accessOK := keychainProbeCursor("cursor-access-token", "cursor-user")
	refreshOK := keychainProbeCursor("cursor-refresh-token", "cursor-user")

	entry := Entry{
		ID:       "cursor.cli_oauth",
		Kind:     KindCLIOAuth,
		Provider: "cursor",
		Tool:     "cursor",
		Identity: Identity{Email: email},
		Source: Source{
			File:            cfgPath,
			KeychainService: "cursor-access-token",
			KeychainAccount: "cursor-user",
		},
		Expiry: &Expiry{
			RefreshTokenPresent: refreshOK,
			Refreshable:         refreshOK,
		},
	}

	switch {
	case accessOK:
		entry.State = StateOK
	case refreshOK:
		entry.State = StateOK
		entry.Notes = []string{"access_token missing; refresh_token present"}
	default:
		entry.State = StateExpired
	}

	return []Entry{entry}, nil
}
