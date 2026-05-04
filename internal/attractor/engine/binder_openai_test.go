package engine

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// bytesMapKeys returns the keys of a map[string][]byte for diagnostic messages.
func bytesMapKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// TestBindCodexCLI_EnvVar verifies that an env_var credential writes an
// isolated auth.json under <stageDir>/.codex/ and sets CODEX_HOME.
func TestBindCodexCLI_EnvVar(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: "OPENAI_API_KEY",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		Value:    "sk-test-openai-abc123",
	}

	result, err := BindCodexCLI(snap, cred, stageDir)
	if err != nil {
		t.Fatalf("BindCodexCLI: unexpected error: %v", err)
	}

	// FilesToWrite must contain <stageDir>/.codex/auth.json.
	expectedPath := filepath.Join(stageDir, ".codex", "auth.json")
	data, ok := result.FilesToWrite[expectedPath]
	if !ok {
		t.Fatalf("FilesToWrite missing %q; got keys: %v", expectedPath, bytesMapKeys(result.FilesToWrite))
	}

	// Validate JSON structure.
	var authObj map[string]string
	if err := json.Unmarshal(data, &authObj); err != nil {
		t.Fatalf("auth.json is not valid JSON: %v", err)
	}
	if authObj["OPENAI_API_KEY"] != "sk-test-openai-abc123" {
		t.Errorf("auth.json OPENAI_API_KEY = %q, want %q", authObj["OPENAI_API_KEY"], "sk-test-openai-abc123")
	}
	if authObj["auth_mode"] != auth.CodexAuthModeAPIKey {
		t.Errorf("auth.json auth_mode = %q, want %q", authObj["auth_mode"], auth.CodexAuthModeAPIKey)
	}

	// CODEX_HOME must point to <stageDir>/.codex.
	expectedCodexHome := filepath.Join(stageDir, ".codex")
	if result.EnvSet["CODEX_HOME"] != expectedCodexHome {
		t.Errorf("EnvSet CODEX_HOME = %q, want %q", result.EnvSet["CODEX_HOME"], expectedCodexHome)
	}
}

// TestBindCodexCLI_CLISession verifies that a cli_session credential produces
// no file writes, no env overrides, AND scrubs OPENAI_API_KEY from the child
// env so the CLI uses the logged-in session (silent wrong-billing prevention).
func TestBindCodexCLI_CLISession(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceCLISession,
			Tool: "codex",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		CLITool:  "codex",
	}

	result, err := BindCodexCLI(snap, cred, stageDir)
	if err != nil {
		t.Fatalf("BindCodexCLI: unexpected error: %v", err)
	}
	if len(result.FilesToWrite) != 0 {
		t.Errorf("expected no FilesToWrite for cli_session, got %d entries: %v",
			len(result.FilesToWrite), bytesMapKeys(result.FilesToWrite))
	}
	if len(result.EnvSet) != 0 {
		t.Errorf("expected no EnvSet for cli_session, got %d entries", len(result.EnvSet))
	}
	scrubFound := false
	for _, name := range result.EnvScrub {
		if name == "OPENAI_API_KEY" {
			scrubFound = true
		}
	}
	if !scrubFound {
		t.Errorf("expected EnvScrub to include OPENAI_API_KEY (wrong-billing prevention), got %v", result.EnvScrub)
	}
	if result.SourceKind != "cli_session" {
		t.Errorf("SourceKind = %q, want %q", result.SourceKind, "cli_session")
	}
}

// TestBindCodexCLI_EnvVarEmptyValue verifies that an env_var credential with
// an empty value is rejected (mismatched source kind / unusable credential).
func TestBindCodexCLI_EnvVarEmptyValue(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: "OPENAI_API_KEY",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		Value:    "", // empty — should be rejected
	}

	_, err := BindCodexCLI(snap, cred, stageDir)
	if err == nil {
		t.Fatal("BindCodexCLI: expected error for empty env_var value, got nil")
	}
}

// TestBindOpenAISDK_EnvVar verifies that a valid env_var credential is
// passed through as SDKArg.
func TestBindOpenAISDK_EnvVar(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: "OPENAI_API_KEY",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		Value:    "sk-test-sdk-key-xyz",
	}

	result, err := BindOpenAISDK(snap, cred, stageDir)
	if err != nil {
		t.Fatalf("BindOpenAISDK: unexpected error: %v", err)
	}
	if result.SDKArg != "sk-test-sdk-key-xyz" {
		t.Errorf("SDKArg = %q, want %q", result.SDKArg, "sk-test-sdk-key-xyz")
	}
	if result.SourceKind != "env_var" {
		t.Errorf("SourceKind = %q, want %q", result.SourceKind, "env_var")
	}
	if result.SourceName != "OPENAI_API_KEY" {
		t.Errorf("SourceName = %q, want %q", result.SourceName, "OPENAI_API_KEY")
	}
}

// TestBindOpenAISDK_EmptyValue verifies that an env_var credential with an
// empty value is rejected.
func TestBindOpenAISDK_EmptyValue(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: "OPENAI_API_KEY",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		Value:    "",
	}

	_, err := BindOpenAISDK(snap, cred, stageDir)
	if err == nil {
		t.Fatal("BindOpenAISDK: expected error for empty credential value, got nil")
	}
}

// TestBindOpenAISDK_CLISession verifies that a cli_session credential is
// rejected for the SDK driver (SDK requires an actual API key).
func TestBindOpenAISDK_CLISession(t *testing.T) {
	stageDir := t.TempDir()
	snap := binding.Snapshot{
		Source: binding.Source{
			Kind: binding.SourceCLISession,
			Tool: "codex",
		},
	}
	cred := binding.Credential{
		Snapshot: snap,
		CLITool:  "codex",
	}

	_, err := BindOpenAISDK(snap, cred, stageDir)
	if err == nil {
		t.Fatal("BindOpenAISDK: expected error for cli_session source, got nil")
	}
}

// TestOpenAIBindersRegistered verifies that both openai-axis drivers are
// present in the global binder registry after package init.
func TestOpenAIBindersRegistered(t *testing.T) {
	drivers := RegisteredDrivers()
	want := []string{"codex_cli", "openai_sdk"}
	driverSet := make(map[string]bool, len(drivers))
	for _, d := range drivers {
		driverSet[d] = true
	}
	for _, w := range want {
		if !driverSet[w] {
			t.Errorf("driver %q not in RegisteredDrivers(); registered: %v", w, drivers)
		}
	}
}
