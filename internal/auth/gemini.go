package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Package-level path vars (overridable in tests).
var (
	geminiSettingsPath = os.Getenv("HOME") + "/.gemini/settings.json"
	geminiOAuthPath    = os.Getenv("HOME") + "/.gemini/oauth_creds.json"
	geminiKeyPath      = os.Getenv("HOME") + "/.gemini_key"
)

// GeminiDetector detects Google Gemini CLI credentials.
type GeminiDetector struct{}

// NewGeminiDetector returns a new GeminiDetector.
func NewGeminiDetector() *GeminiDetector { return &GeminiDetector{} }

// Name returns the stable detector identifier.
func (d *GeminiDetector) Name() string { return "gemini" }

// geminiSettings represents the relevant fields of ~/.gemini/settings.json.
type geminiSettings struct {
	Security struct {
		Auth struct {
			SelectedType string `json:"selectedType"`
		} `json:"auth"`
	} `json:"security"`
}

// geminiOAuthCreds represents ~/.gemini/oauth_creds.json.
type geminiOAuthCreds struct {
	AccessToken  string  `json:"access_token"`
	RefreshToken string  `json:"refresh_token"`
	ExpiryDate   float64 `json:"expiry_date"` // milliseconds since epoch
}

// Detect scans for Gemini CLI credentials and returns the discovered entries.
func (d *GeminiDetector) Detect() ([]Entry, error) {
	var entries []Entry

	// Check env-var overrides first.
	geminiAPIKey := os.Getenv("GEMINI_API_KEY")
	googleAPIKey := os.Getenv("GOOGLE_API_KEY")
	hasEnvKey := geminiAPIKey != "" || googleAPIKey != ""

	// Determine auth type from settings file.
	authType := ""
	settingsData, err := os.ReadFile(geminiSettingsPath)
	if err == nil {
		var s geminiSettings
		if jsonErr := json.Unmarshal(settingsData, &s); jsonErr == nil {
			authType = s.Security.Auth.SelectedType
		}
	}

	// Build env entry if an env var is set.
	if hasEnvKey {
		varNames := []string{}
		if geminiAPIKey != "" {
			varNames = append(varNames, "GEMINI_API_KEY")
		}
		if googleAPIKey != "" {
			varNames = append(varNames, "GOOGLE_API_KEY")
		}
		note := fmt.Sprintf("set via env: %s", strings.Join(varNames, ", "))
		envEntry := Entry{
			ID:       "google.env.GEMINI_API_KEY",
			Kind:     KindEnvVar,
			Provider: "google",
			Tool:     "gemini",
			State:    StateOK,
			Source:   Source{EnvVar: strings.Join(varNames, ",")},
			Notes:    []string{note},
		}
		entries = append(entries, envEntry)
	}

	switch authType {
	case "oauth-personal":
		entry := d.detectOAuth()
		entries = append(entries, entry)
	default:
		// api-key mode (or settings missing): check ~/.gemini_key file.
		entry := d.detectAPIKeyFile()
		if entry != nil {
			entries = append(entries, *entry)
		} else if !hasEnvKey {
			// Neither file nor env: missing.
			entries = append(entries, Entry{
				ID:          "google.gemini.api_key",
				Kind:        KindAPIKeyFile,
				Provider:    "google",
				Tool:        "gemini",
				State:       StateMissing,
				Source:      Source{File: geminiKeyPath},
				Remediation: "Set GEMINI_API_KEY env var or create ~/.gemini_key with a valid API key.",
			})
		}
	}

	return entries, nil
}

// detectOAuth inspects ~/.gemini/oauth_creds.json and returns an Entry.
func (d *GeminiDetector) detectOAuth() Entry {
	base := Entry{
		ID:       "google.gemini.oauth",
		Kind:     KindCLIOAuth,
		Provider: "google",
		Tool:     "gemini",
		Source:   Source{File: geminiOAuthPath},
	}

	data, err := os.ReadFile(geminiOAuthPath)
	if err != nil {
		if os.IsNotExist(err) {
			base.State = StateMissing
			base.Remediation = "Run `gemini auth login` to authenticate."
			return base
		}
		base.State = StateAmbiguous
		base.Notes = []string{fmt.Sprintf("could not read oauth_creds.json: %v", err)}
		return base
	}

	var creds geminiOAuthCreds
	if jsonErr := json.Unmarshal(data, &creds); jsonErr != nil {
		base.State = StateAmbiguous
		base.Notes = []string{"malformed oauth_creds.json"}
		return base
	}

	// expiry_date is in milliseconds; convert to seconds for comparison.
	expiryMs := creds.ExpiryDate
	expirySec := int64(expiryMs) / 1000
	expiryTime := time.Unix(expirySec, 0)
	hasRefresh := creds.RefreshToken != ""

	now := time.Now().Unix()

	if expirySec > now {
		// Token still valid.
		base.State = StateOK
		base.Expiry = &Expiry{
			AccessTokenExpiresAt: &expiryTime,
			RefreshTokenPresent:  hasRefresh,
			Refreshable:          hasRefresh,
		}
	} else if hasRefresh {
		// Expired but can be refreshed.
		base.State = StateOK
		base.Expiry = &Expiry{
			AccessTokenExpiresAt: &expiryTime,
			RefreshTokenPresent:  true,
			Refreshable:          true,
		}
		base.Notes = []string{"expired; refreshable"}
	} else {
		// Expired, no refresh token.
		base.State = StateExpired
		base.Expiry = &Expiry{
			AccessTokenExpiresAt: &expiryTime,
			RefreshTokenPresent:  false,
			Refreshable:          false,
		}
		base.Remediation = "Run `gemini auth login` to re-authenticate."
	}

	return base
}

// detectAPIKeyFile reads ~/.gemini_key and returns an Entry if valid.
// Returns nil if the file does not exist.
func (d *GeminiDetector) detectAPIKeyFile() *Entry {
	data, err := os.ReadFile(geminiKeyPath)
	if err != nil {
		return nil
	}

	// Look for a line like: gemini_key: AIzaSy...
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "gemini_key:") {
			value := strings.TrimSpace(strings.TrimPrefix(line, "gemini_key:"))
			if value != "" {
				return &Entry{
					ID:       "google.gemini.api_key_file",
					Kind:     KindAPIKeyFile,
					Provider: "google",
					Tool:     "gemini",
					State:    StateOK,
					Source:   Source{File: geminiKeyPath},
				}
			}
		}
	}

	// File exists but no valid key found.
	return &Entry{
		ID:          "google.gemini.api_key_file",
		Kind:        KindAPIKeyFile,
		Provider:    "google",
		Tool:        "gemini",
		State:       StateAmbiguous,
		Source:      Source{File: geminiKeyPath},
		Notes:       []string{"~/.gemini_key found but no valid gemini_key: line"},
		Remediation: "Add a line: gemini_key: AIzaSy...",
	}
}
