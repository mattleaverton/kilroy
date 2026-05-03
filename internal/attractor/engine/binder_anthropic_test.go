package engine

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// claudeCLISnap returns a Snapshot appropriate for the claude_cli driver.
func claudeCLISnap() binding.Snapshot {
	return binding.Snapshot{
		ChainName: "anthropic_claude_cli",
		Source: binding.Source{
			Kind: binding.SourceCLISession,
			Tool: "claude",
		},
		Provider:     "anthropic",
		Method:       binding.MethodCLIOAuth,
		FallbackRank: 0,
	}
}

// claudeCLICred returns a valid Credential for the claude_cli driver.
func claudeCLICred() binding.Credential {
	snap := claudeCLISnap()
	return binding.Credential{
		Snapshot: snap,
		CLITool:  "claude",
	}
}

// sdkSnap returns a Snapshot appropriate for the anthropic_sdk driver.
func sdkSnap(envName string) binding.Snapshot {
	return binding.Snapshot{
		ChainName: "anthropic_kilroy_api",
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: envName,
		},
		Provider:     "anthropic",
		Method:       binding.MethodAPIKey,
		FallbackRank: 0,
	}
}

// sdkCred returns a valid Credential for the anthropic_sdk driver.
func sdkCred(envName, value string) binding.Credential {
	snap := sdkSnap(envName)
	return binding.Credential{
		Snapshot: snap,
		Value:    value,
	}
}

// ---------------------------------------------------------------------------
// BindClaudeCLI tests
// ---------------------------------------------------------------------------

// Test 1: BindClaudeCLI scrubs ANTHROPIC_API_KEY — the load-bearing assertion.
func TestBindClaudeCLI_ScrubsAPIKey(t *testing.T) {
	result, err := BindClaudeCLI(claudeCLISnap(), claudeCLICred(), "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, key := range result.EnvScrub {
		if key == "ANTHROPIC_API_KEY" {
			return // found — test passes
		}
	}
	t.Errorf("EnvScrub does not contain ANTHROPIC_API_KEY; got %v", result.EnvScrub)
}

// Test 2: BindClaudeCLI does not set any env var.
func TestBindClaudeCLI_NoEnvSet(t *testing.T) {
	result, err := BindClaudeCLI(claudeCLISnap(), claudeCLICred(), "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.EnvSet) != 0 {
		t.Errorf("expected EnvSet to be empty, got %v", result.EnvSet)
	}
}

// Test 3a: BindClaudeCLI rejects wrong source kind.
func TestBindClaudeCLI_RejectsMismatch_WrongKind(t *testing.T) {
	cred := sdkCred("ANTHROPIC_API_KEY", "sk-test")
	_, err := BindClaudeCLI(cred.Snapshot, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for wrong source kind, got nil")
	}
	if !strings.Contains(err.Error(), "cli_session") {
		t.Errorf("error message should mention cli_session; got: %v", err)
	}
}

// Test 3b: BindClaudeCLI rejects wrong CLITool.
func TestBindClaudeCLI_RejectsMismatch_WrongTool(t *testing.T) {
	snap := claudeCLISnap()
	cred := binding.Credential{
		Snapshot: snap,
		CLITool:  "codex", // wrong tool
	}
	_, err := BindClaudeCLI(snap, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for wrong CLITool, got nil")
	}
	if !strings.Contains(err.Error(), "claude") {
		t.Errorf("error message should mention expected tool name 'claude'; got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// BindAnthropicSDK tests
// ---------------------------------------------------------------------------

// Test 4: BindAnthropicSDK with valid env_var cred returns SDKArg = cred.Value.
func TestBindAnthropicSDK_ValidEnvVarCred(t *testing.T) {
	const wantValue = "sk-ant-secret-key"
	cred := sdkCred("ANTHROPIC_API_KEY_KILROY", wantValue)
	result, err := BindAnthropicSDK(cred.Snapshot, cred, "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SDKArg != wantValue {
		t.Errorf("SDKArg = %q; want %q", result.SDKArg, wantValue)
	}
	if result.SourceKind != string(binding.SourceEnvVar) {
		t.Errorf("SourceKind = %q; want %q", result.SourceKind, binding.SourceEnvVar)
	}
	if result.SourceName != "ANTHROPIC_API_KEY_KILROY" {
		t.Errorf("SourceName = %q; want %q", result.SourceName, "ANTHROPIC_API_KEY_KILROY")
	}
}

// Test 5: BindAnthropicSDK with empty value returns an error.
func TestBindAnthropicSDK_EmptyValue(t *testing.T) {
	cred := sdkCred("ANTHROPIC_API_KEY_KILROY", "") // empty
	_, err := BindAnthropicSDK(cred.Snapshot, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for empty credential value, got nil")
	}
}

// Test 6: BindAnthropicSDK rejects a cli_session credential.
func TestBindAnthropicSDK_RejectsCLISessionCred(t *testing.T) {
	snap := claudeCLISnap()
	cred := claudeCLICred()
	_, err := BindAnthropicSDK(snap, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for cli_session credential passed to SDK binder, got nil")
	}
	if !strings.Contains(err.Error(), "env_var") {
		t.Errorf("error message should mention env_var; got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Registration tests
// ---------------------------------------------------------------------------

// Test 7: Both claude_cli and anthropic_sdk are registered at init.
func TestBindAnthropicDriversRegistered(t *testing.T) {
	drivers := RegisteredDrivers()
	driverSet := make(map[string]bool, len(drivers))
	for _, d := range drivers {
		driverSet[d] = true
	}

	for _, want := range []string{"claude_cli", "anthropic_sdk"} {
		if !driverSet[want] {
			t.Errorf("driver %q not found in RegisteredDrivers(); got %v", want, drivers)
		}
	}
}
