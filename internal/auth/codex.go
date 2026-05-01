package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
			ID:       cliID,
			Kind:     KindCLIOAuth,
			Provider: "openai",
			Tool:     "codex",
			State:    StateAmbiguous,
			Source:   Source{File: filePath},
			Notes:    []string{"malformed JSON in auth file"},
		}
	}

	switch f.AuthMode {
	case "chatgpt":
		return parseCodexChatGPT(f, filePath)
	case "api_key":
		return parseCodexAPIKey(f, filePath)
	default:
		return Entry{
			ID:       cliID,
			Kind:     KindCLIOAuth,
			Provider: "openai",
			Tool:     "codex",
			State:    StateAmbiguous,
			Source:   Source{File: filePath},
			Notes:    []string{fmt.Sprintf("unknown auth_mode: %q", f.AuthMode)},
		}
	}
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
		return entry
	}

	exp, err := decodeJWTExp(f.Tokens.AccessToken)
	if err != nil {
		entry.State = StateAmbiguous
		entry.Notes = []string{fmt.Sprintf("cannot decode access_token JWT: %v", err)}
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
		return nil, fmt.Errorf("codex: reading %s: %w", codexAuthPath, err)
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
