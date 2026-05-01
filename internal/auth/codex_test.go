package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// makeTestJWT builds a minimal unsigned JWT with the given exp and optional
// organizations array. The third segment is a dummy signature.
func makeTestJWT(exp int64, orgs []jwtOrg) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	type payload struct {
		Exp           int64    `json:"exp"`
		Organizations []jwtOrg `json:"organizations,omitempty"`
	}
	p := payload{Exp: exp, Organizations: orgs}
	pb, _ := json.Marshal(p)
	return header + "." + base64.RawURLEncoding.EncodeToString(pb) + ".fakesig"
}

// writeCodexAuth writes a codex auth JSON file to dir and returns its path.
func writeCodexAuth(t *testing.T, dir string, content map[string]interface{}) string {
	t.Helper()
	path := filepath.Join(dir, "auth.json")
	data, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("marshal auth content: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	return path
}

func TestCodexDetector(t *testing.T) {
	d := NewCodexDetector()
	if d.Name() != "codex" {
		t.Fatalf("Name() = %q, want %q", d.Name(), "codex")
	}

	t.Run("no_file_no_env_missing", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "") // clear any ambient env var
		dir := t.TempDir()
		orig := codexAuthPath
		codexAuthPath = filepath.Join(dir, "auth.json") // file does not exist
		t.Cleanup(func() { codexAuthPath = orig })

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		if entries[0].State != StateMissing {
			t.Errorf("state = %q, want %q", entries[0].State, StateMissing)
		}
	})

	t.Run("chatgpt_valid_jwt_ok", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "") // clear any ambient env var
		dir := t.TempDir()
		futureExp := time.Now().Add(time.Hour).Unix()
		token := makeTestJWT(futureExp, nil)

		path := writeCodexAuth(t, dir, map[string]interface{}{
			"auth_mode": "chatgpt",
			"tokens": map[string]string{
				"access_token":  token,
				"refresh_token": "some-refresh-token",
			},
		})

		orig := codexAuthPath
		codexAuthPath = path
		t.Cleanup(func() { codexAuthPath = orig })

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		e := entries[0]
		if e.State != StateOK {
			t.Errorf("state = %q, want %q", e.State, StateOK)
		}
		if e.Expiry == nil {
			t.Fatal("Expiry is nil, want populated")
		}
		if e.Expiry.AccessTokenExpiresAt == nil {
			t.Error("AccessTokenExpiresAt is nil")
		} else {
			wantExp := time.Unix(futureExp, 0)
			if !e.Expiry.AccessTokenExpiresAt.Equal(wantExp) {
				t.Errorf("AccessTokenExpiresAt = %v, want %v", e.Expiry.AccessTokenExpiresAt, wantExp)
			}
		}
		if !e.Expiry.RefreshTokenPresent {
			t.Error("RefreshTokenPresent = false, want true")
		}
		if !e.Expiry.Refreshable {
			t.Error("Refreshable = false, want true")
		}
	})

	t.Run("chatgpt_expired_jwt_with_refresh_ok", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "") // clear any ambient env var
		dir := t.TempDir()
		pastExp := time.Now().Add(-time.Hour).Unix()
		token := makeTestJWT(pastExp, nil)

		path := writeCodexAuth(t, dir, map[string]interface{}{
			"auth_mode": "chatgpt",
			"tokens": map[string]string{
				"access_token":  token,
				"refresh_token": "present-refresh-token",
			},
		})

		orig := codexAuthPath
		codexAuthPath = path
		t.Cleanup(func() { codexAuthPath = orig })

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		e := entries[0]
		if e.State != StateOK {
			t.Errorf("state = %q, want %q", e.State, StateOK)
		}
		found := false
		for _, n := range e.Notes {
			if n == "access_token expired; refresh_token available" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected note about refresh_token, got notes: %v", e.Notes)
		}
	})

	t.Run("chatgpt_expired_jwt_no_refresh_expired", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "") // clear any ambient env var
		dir := t.TempDir()
		pastExp := time.Now().Add(-time.Hour).Unix()
		token := makeTestJWT(pastExp, nil)

		path := writeCodexAuth(t, dir, map[string]interface{}{
			"auth_mode": "chatgpt",
			"tokens": map[string]string{
				"access_token": token,
				// no refresh_token
			},
		})

		orig := codexAuthPath
		codexAuthPath = path
		t.Cleanup(func() { codexAuthPath = orig })

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		e := entries[0]
		if e.State != StateExpired {
			t.Errorf("state = %q, want %q", e.State, StateExpired)
		}
	})

	t.Run("malformed_json_ambiguous", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "") // clear any ambient env var
		dir := t.TempDir()
		path := filepath.Join(dir, "auth.json")
		if err := os.WriteFile(path, []byte("{not valid json}"), 0600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		orig := codexAuthPath
		codexAuthPath = path
		t.Cleanup(func() { codexAuthPath = orig })

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("got %d entries, want 1", len(entries))
		}
		e := entries[0]
		if e.State != StateAmbiguous {
			t.Errorf("state = %q, want %q", e.State, StateAmbiguous)
		}
	})

	t.Run("env_shadows_cli", func(t *testing.T) {
		dir := t.TempDir()
		futureExp := time.Now().Add(time.Hour).Unix()
		token := makeTestJWT(futureExp, nil)

		path := writeCodexAuth(t, dir, map[string]interface{}{
			"auth_mode": "chatgpt",
			"tokens": map[string]string{
				"access_token": token,
			},
		})

		orig := codexAuthPath
		codexAuthPath = path
		t.Cleanup(func() { codexAuthPath = orig })

		t.Setenv("OPENAI_API_KEY", "sk-test-key-value")

		entries, err := d.Detect()
		if err != nil {
			t.Fatalf("Detect() error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("got %d entries, want 2", len(entries))
		}

		envEntry := entries[0]
		cliEntry := entries[1]

		// Env entry checks.
		if envEntry.Kind != KindEnvVar {
			t.Errorf("entries[0].Kind = %q, want %q", envEntry.Kind, KindEnvVar)
		}
		if envEntry.State != StateOK {
			t.Errorf("entries[0].State = %q, want %q", envEntry.State, StateOK)
		}
		if len(envEntry.Shadows) == 0 || envEntry.Shadows[0] != "openai.codex.cli" {
			t.Errorf("entries[0].Shadows = %v, want [openai.codex.cli]", envEntry.Shadows)
		}

		// CLI entry checks.
		if cliEntry.State != StateOK {
			t.Errorf("entries[1].State = %q, want %q", cliEntry.State, StateOK)
		}
		if len(cliEntry.ShadowedBy) == 0 || cliEntry.ShadowedBy[0] != "openai.env.OPENAI_API_KEY" {
			t.Errorf("entries[1].ShadowedBy = %v, want [openai.env.OPENAI_API_KEY]", cliEntry.ShadowedBy)
		}

		_ = fmt.Sprintf("suppress unused import") // keep fmt imported for test helpers
	})
}

// TestCodexDetector_StaleLastRefresh covers the §8.4 stale-session heuristic:
// an unrefreshed token whose last_refresh is more than 30 days old may have
// been silently revoked server-side (web logout, org rotation), so we surface
// it as ambiguous rather than ok even though its JWT exp is still in the future.
func TestCodexDetector_StaleLastRefresh(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	dir := t.TempDir()
	orig := codexAuthPath
	codexAuthPath = filepath.Join(dir, "auth.json")
	t.Cleanup(func() { codexAuthPath = orig })

	// Token whose JWT exp is 1 day in the future (would normally be ok).
	futureExp := time.Now().Add(24 * time.Hour).Unix()
	jwt := makeTestJWT(futureExp, nil)

	// last_refresh 60 days ago — past the 30-day staleness threshold.
	lastRefresh := time.Now().Add(-60 * 24 * time.Hour).UTC().Format(time.RFC3339)

	writeCodexAuth(t, dir, map[string]interface{}{
		"auth_mode": "chatgpt",
		"tokens": map[string]interface{}{
			"access_token":  jwt,
			"refresh_token": "rt-stale",
		},
		"last_refresh": lastRefresh,
	})

	d := NewCodexDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d: %+v", len(entries), entries)
	}
	got := entries[0]
	if got.State != StateAmbiguous {
		t.Errorf("State = %q, want %q (stale last_refresh)", got.State, StateAmbiguous)
	}
	staleNoteFound := false
	for _, n := range got.Notes {
		if filepathContains(n, "not refreshed in >30 days") {
			staleNoteFound = true
			break
		}
	}
	if !staleNoteFound {
		t.Errorf("expected stale-token note, got Notes=%v", got.Notes)
	}
	if got.Remediation == "" {
		t.Errorf("expected remediation hint for stale token, got empty")
	}
}

// TestCodexDetector_RecentLastRefresh confirms a token refreshed within 30
// days is NOT marked stale even when last_refresh is set.
func TestCodexDetector_RecentLastRefresh(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	dir := t.TempDir()
	orig := codexAuthPath
	codexAuthPath = filepath.Join(dir, "auth.json")
	t.Cleanup(func() { codexAuthPath = orig })

	futureExp := time.Now().Add(24 * time.Hour).Unix()
	jwt := makeTestJWT(futureExp, nil)
	lastRefresh := time.Now().Add(-2 * 24 * time.Hour).UTC().Format(time.RFC3339)

	writeCodexAuth(t, dir, map[string]interface{}{
		"auth_mode": "chatgpt",
		"tokens": map[string]interface{}{
			"access_token":  jwt,
			"refresh_token": "rt-fresh",
		},
		"last_refresh": lastRefresh,
	})

	d := NewCodexDetector()
	entries, err := d.Detect()
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].State != StateOK {
		t.Errorf("State = %q, want %q (recent last_refresh)", entries[0].State, StateOK)
	}
}

// filepathContains is a small string-contains helper so the test reads
// in the same idiom as the file's existing matchers.
func filepathContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
