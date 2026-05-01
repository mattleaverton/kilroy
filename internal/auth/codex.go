package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// codexAuthPath is the path to the Codex CLI auth file. Override in tests.
var codexAuthPath = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".codex", "auth.json")
}()

// CodexDetector discovers credentials for the OpenAI Codex CLI.
type CodexDetector struct{}

// NewCodexDetector returns a new CodexDetector.
func NewCodexDetector() *CodexDetector { return &CodexDetector{} }

// Name returns the stable identifier for this detector.
func (d *CodexDetector) Name() string { return "codex" }

// codexFile is the on-disk structure of ~/.codex/auth.json.
type codexFile struct {
	AuthMode     string `json:"auth_mode"`
	Tokens       struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	} `json:"tokens"`
	LastRefresh  string `json:"last_refresh"`
	OpenAIAPIKey string `json:"OPENAI_API_KEY"`
}

// jwtOrg represents one organization entry in a JWT payload.
type jwtOrg struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Title     string `json:"title"`
	IsDefault bool   `json:"is_default"`
}

// decodeJWTExp decodes the second (payload) segment of a JWT and returns the
// numeric `exp` claim (Unix seconds). It never returns the token value.
func decodeJWTExp(token string) (int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid JWT: expected 3 segments, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, fmt.Errorf("failed to base64url-decode JWT payload: %w", err)
	}
	var claims map[string]interface{}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return 0, fmt.Errorf("failed to JSON-parse JWT payload: %w", err)
	}
	expRaw, ok := claims["exp"]
	if !ok {
		return 0, fmt.Errorf("JWT payload has no 'exp' claim")
	}
	expFloat, ok := expRaw.(float64)
	if !ok {
		return 0, fmt.Errorf("JWT 'exp' claim has unexpected type %T", expRaw)
	}
	return int64(expFloat), nil
}

// decodeJWTOrgs decodes the `organizations` array from a JWT payload.
func decodeJWTOrgs(token string) ([]jwtOrg, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT: expected 3 segments, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to base64url-decode JWT payload: %w", err)
	}
	var claims struct {
		Organizations []jwtOrg `json:"organizations"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, fmt.Errorf("failed to JSON-parse JWT payload: %w", err)
	}
	return claims.Organizations, nil
}

// parseCodexAuth parses raw auth.json bytes and returns the CLI Entry.
func parseCodexAuth(data []byte, filePath string) Entry {
	const cliID = "openai.codex.cli"

	var f codexFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Entry{
			ID:          cliID,
			Kind:        KindCLIOAuth,
			Provider:    "openai",
			Tool:        "codex",
			State:       StateAmbiguous,
			Source:      Source{File: filePath},
			Notes:       []string{"malformed JSON in auth file"},
			Remediation: "Backup and re-run: codex (regenerates auth.json)",
		}
	}

	switch f.AuthMode {
	case "chatgpt":
		return parseCodexChatGPT(f, filePath)
	case "api_key":
		return parseCodexAPIKey(f, filePath)
	default:
		return Entry{
			ID:          cliID,
			Kind:        KindCLIOAuth,
			Provider:    "openai",
			Tool:        "codex",
			State:       StateAmbiguous,
			Source:      Source{File: filePath},
			Notes:       []string{fmt.Sprintf("unknown auth_mode: %q", f.AuthMode)},
			Remediation: "Re-authenticate: codex /logout && codex",
		}
	}
}

// parseFlexibleTimestamp accepts the variety of timestamp formats codex
// has used for last_refresh across versions: RFC3339, RFC3339Nano,
// or Unix-seconds-as-string. Returns (zero, false) if none match.
func parseFlexibleTimestamp(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), true
	}
	return time.Time{}, false
}

func parseCodexChatGPT(f codexFile, filePath string) Entry {
	const cliID = "openai.codex.cli"

	entry := Entry{
		ID:       cliID,
		Kind:     KindCLIOAuth,
		Provider: "openai",
		Tool:     "codex",
		Source:   Source{File: filePath},
	}

	if f.Tokens.AccessToken == "" {
		entry.State = StateAmbiguous
		entry.Notes = []string{"missing tokens.access_token"}
		entry.Remediation = "Re-authenticate: codex /logout && codex"
		return entry
	}

	exp, err := decodeJWTExp(f.Tokens.AccessToken)
	if err != nil {
		entry.State = StateAmbiguous
		entry.Notes = []string{fmt.Sprintf("cannot decode access_token JWT: %v", err)}
		entry.Remediation = "Backup and re-run: codex (regenerates auth.json)"
		return entry
	}

	expTime := time.Unix(exp, 0)
	now := time.Now()
	refreshPresent := f.Tokens.RefreshToken != ""

	entry.Expiry = &Expiry{
		AccessTokenExpiresAt: &expTime,
		RefreshTokenPresent:  refreshPresent,
		Refreshable:          refreshPresent,
	}

	// Extract org info from JWT payload.
	orgs, _ := decodeJWTOrgs(f.Tokens.AccessToken)
	var defaultOrg *jwtOrg
	var extraOrgs []jwtOrg
	for i := range orgs {
		if orgs[i].IsDefault {
			defaultOrg = &orgs[i]
		} else {
			extraOrgs = append(extraOrgs, orgs[i])
		}
	}
	if defaultOrg != nil {
		orgName := defaultOrg.Name
		if orgName == "" {
			orgName = defaultOrg.Title
		}
		entry.Identity = Identity{Org: orgName}
	}

	// Classify state.
	if expTime.After(now) {
		entry.State = StateOK
	} else if refreshPresent {
		entry.State = StateOK
		entry.Notes = []string{"access_token expired; refresh_token available"}
	} else {
		entry.State = StateExpired
		entry.Remediation = "Run: codex (triggers re-authentication)"
	}

	// Stale-session heuristic (Inv2 §4.1): an unrefreshed token whose
	// last_refresh is >30 days old may have been revoked server-side
	// (web logout, org rotation) without us being able to detect that.
	// Surface this as ambiguous with a note so callers can decide.
	if entry.State == StateOK && f.LastRefresh != "" {
		// last_refresh format varies by codex version; try RFC3339 first,
		// then RFC3339Nano, then fall back to a Unix-second timestamp string.
		lastRefresh, ok := parseFlexibleTimestamp(f.LastRefresh)
		if ok && now.Sub(lastRefresh) > 30*24*time.Hour {
			entry.State = StateAmbiguous
			entry.Notes = append(entry.Notes,
				fmt.Sprintf("token not refreshed in >30 days (last_refresh=%s); may be revoked server-side",
					lastRefresh.UTC().Format(time.RFC3339)))
			entry.Remediation = "Verify with: codex (or codex /logout && codex)"
		}
	}

	// Non-default orgs as profiles (state mirrors JWT validity).
	orgState := entry.State
	if orgState == StateOK && !expTime.After(now) {
		// expired+refresh scenario: profiles still usable via refresh
		orgState = StateOK
	}
	for _, org := range extraOrgs {
		orgName := org.Name
		if orgName == "" {
			orgName = org.Title
		}
		entry.Profiles = append(entry.Profiles, Profile{
			Name:     orgName,
			Active:   false,
			Identity: Identity{Org: orgName},
			State:    orgState,
		})
	}

	return entry
}

func parseCodexAPIKey(f codexFile, filePath string) Entry {
	const cliID = "openai.codex.cli"

	entry := Entry{
		ID:       cliID,
		Kind:     KindCLIAPIKey,
		Provider: "openai",
		Tool:     "codex",
		Source:   Source{File: filePath},
	}
	if f.OpenAIAPIKey != "" {
		entry.State = StateOK
	} else {
		entry.State = StateMissing
		entry.Remediation = "Set OPENAI_API_KEY env var, or run: codex (re-authenticates)"
	}
	return entry
}

// Detect scans for OpenAI Codex CLI credentials and returns zero or more
// entries. It never returns credential values.
func (d *CodexDetector) Detect() ([]Entry, error) {
	const (
		cliID = "openai.codex.cli"
		envID = "openai.env.OPENAI_API_KEY"
	)

	var entries []Entry

	data, err := os.ReadFile(codexAuthPath)
	var cliEntry Entry
	switch {
	case err == nil:
		cliEntry = parseCodexAuth(data, codexAuthPath)
	case os.IsNotExist(err):
		cliEntry = Entry{
			ID:       cliID,
			Kind:     KindCLIOAuth,
			Provider: "openai",
			Tool:     "codex",
			State:    StateMissing,
			Source:   Source{File: codexAuthPath},
		}
	default:
		// File present but unreadable: surface as ambiguous so the user
		// can see why we can't route through codex.
		cliEntry = Entry{
			ID:          cliID,
			Kind:        KindCLIOAuth,
			Provider:    "openai",
			Tool:        "codex",
			State:       StateAmbiguous,
			Source:      Source{File: codexAuthPath},
			Notes:       []string{fmt.Sprintf("auth.json unreadable: %v", err)},
			Remediation: "Check file permissions on " + codexAuthPath,
		}
	}

	// Env-var override takes precedence.
	if envVal := os.Getenv("OPENAI_API_KEY"); envVal != "" {
		envEntry := Entry{
			ID:       envID,
			Kind:     KindEnvVar,
			Provider: "openai",
			Tool:     "codex",
			State:    StateOK,
			Source:   Source{EnvVar: "OPENAI_API_KEY"},
			Shadows:  []string{cliID},
		}
		cliEntry.ShadowedBy = []string{envID}
		entries = append(entries, envEntry)
	}

	entries = append(entries, cliEntry)
	return entries, nil
}
