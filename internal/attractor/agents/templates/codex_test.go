// Tests for the Codex template's auth-method-aware --model handling.
// Per F8: subscription-bound codex restricts the model allowlist and
// 400s on unsupported models; api_key mode honors any model. The
// resolved authMethod parameter is authoritative when non-empty;
// global ~/.codex/auth.json is consulted only as a legacy fallback.
package templates

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth"
)

// writeCodexAuth writes a minimal auth.json with the given auth_mode at
// dir/auth.json and returns the path. Used to drive the legacy fallback
// path (codexShouldDropModel with empty authMethod).
func writeCodexAuth(t *testing.T, dir, authMode string) string {
	t.Helper()
	path := filepath.Join(dir, "auth.json")
	body := `{"auth_mode":"` + authMode + `","tokens":{"access_token":"x"}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	return path
}

// authMethod=cli_oauth → drop --model (subscription-bound).
func TestCodex_BuildArgs_CLIOAuth_DropsModel(t *testing.T) {
	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "cli_oauth", "")
	if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
		t.Fatalf("cli_oauth should drop --model; got args: %v", args)
	}
}

// authMethod=api_key → keep --model (env-key path supports any model).
func TestCodex_BuildArgs_APIKey_KeepsModel(t *testing.T) {
	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "api_key", "")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4-nano") {
		t.Fatalf("api_key should keep --model; got args: %v", args)
	}
}

// **The reviewer-flagged regression:** if the user's GLOBAL codex login
// is chatgpt mode but kilroy resolved an explicit api_key route (e.g.
// the binder materialized an isolated auth.json with OPENAI_API_KEY),
// we must keep --model. Earlier the template ignored the resolved
// authMethod and always read global state.
func TestCodex_BuildArgs_GlobalChatGPT_ButResolvedAPIKey_KeepsModel(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), auth.CodexAuthModeChatGPT)

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "api_key", "")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4-nano") {
		t.Fatalf("explicit api_key must beat global chatgpt — keep --model; got args: %v", args)
	}
}

// Symmetric: global api_key but resolved cli_oauth → drop --model.
func TestCodex_BuildArgs_GlobalAPIKey_ButResolvedCLIOAuth_DropsModel(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), auth.CodexAuthModeAPIKey)

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "cli_oauth", "")
	if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
		t.Fatalf("explicit cli_oauth must beat global api_key — drop --model; got args: %v", args)
	}
}

// Legacy fallback: empty authMethod, global codex is chatgpt → drop.
func TestCodex_BuildArgs_LegacyFallback_GlobalChatGPT_DropsModel(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), auth.CodexAuthModeChatGPT)

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "", "")
	if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
		t.Fatalf("legacy fallback with global chatgpt should drop --model; got args: %v", args)
	}
}

// Legacy fallback: empty authMethod, global codex is api_key → keep.
func TestCodex_BuildArgs_LegacyFallback_GlobalAPIKey_KeepsModel(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), auth.CodexAuthModeAPIKey)

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "", "")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4-nano") {
		t.Fatalf("legacy fallback with global api_key should keep --model; got args: %v", args)
	}
}

// Legacy fallback with no auth.json file at all: keep --model (don't
// crash, don't silently drop based on missing file).
func TestCodex_BuildArgs_LegacyFallback_AuthFileMissing_KeepsModel(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = filepath.Join(t.TempDir(), "absent.json")

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4", "", "")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4") {
		t.Fatalf("missing auth.json with empty authMethod should keep --model; got args: %v", args)
	}
}

// model="" → no --model flag regardless of auth state.
func TestCodex_BuildArgs_EmptyModel_NoModelFlag(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), auth.CodexAuthModeChatGPT)

	for _, am := range []string{"", "cli_oauth", "api_key"} {
		args := Codex().BuildArgs("hi", "/tmp/wd", "", am, "")
		if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
			t.Fatalf("authMethod=%q empty model: should never produce --model; got %v", am, args)
		}
	}
}
