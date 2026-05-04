// Tests for the Codex template's auth-method-aware --model handling.
// Per F8: chatgpt-mode codex restricts the model allowlist and 400s
// on unsupported models; api_key mode honors any model.
package templates

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCodexAuth(t *testing.T, dir, authMode string) string {
	t.Helper()
	path := filepath.Join(dir, "auth.json")
	body := `{"auth_mode":"` + authMode + `","tokens":{"access_token":"x"}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	return path
}

func TestCodex_BuildArgs_OmitsModelInChatGPTMode(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), "chatgpt")

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "cli_oauth")
	if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
		t.Fatalf("chatgpt mode should drop --model; got args: %v", args)
	}
}

func TestCodex_BuildArgs_KeepsModelInAPIKeyMode(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), "api_key")

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4-nano", "api_key")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4-nano") {
		t.Fatalf("api_key mode should keep --model; got args: %v", args)
	}
}

func TestCodex_BuildArgs_KeepsModelWhenAuthFileMissing(t *testing.T) {
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	// Pointing at a non-existent path must not crash and must default
	// to "keep --model" — the api_key path runs fine without auth.json
	// when the user has OPENAI_API_KEY in env.
	codexAuthPath = filepath.Join(t.TempDir(), "absent.json")

	args := Codex().BuildArgs("hi", "/tmp/wd", "gpt-5.4", "cli_oauth")
	if joined := strings.Join(args, " "); !strings.Contains(joined, "--model gpt-5.4") {
		t.Fatalf("missing auth.json should default to keep --model; got args: %v", args)
	}
}

func TestCodex_BuildArgs_NoModelStaysNoModel(t *testing.T) {
	// model="" → no --model flag regardless of auth mode.
	prev := codexAuthPath
	t.Cleanup(func() { codexAuthPath = prev })
	codexAuthPath = writeCodexAuth(t, t.TempDir(), "chatgpt")

	args := Codex().BuildArgs("hi", "/tmp/wd", "", "cli_oauth")
	if joined := strings.Join(args, " "); strings.Contains(joined, "--model") {
		t.Fatalf("empty model should never produce --model; got args: %v", args)
	}
}
