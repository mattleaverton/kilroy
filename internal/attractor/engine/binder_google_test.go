package engine

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// makeGoogleEnvCred is a helper that builds a Credential backed by a named
// env var source, for use across multiple test cases.
func makeGoogleEnvCred(envVarName, value string) (binding.Snapshot, binding.Credential) {
	snap := binding.Snapshot{
		ChainName: "google_kilroy_api",
		Source: binding.Source{
			Kind: binding.SourceEnvVar,
			Name: envVarName,
		},
		Provider: "google",
		Method:   binding.MethodAPIKey,
	}
	cred := binding.Credential{
		Snapshot: snap,
		Value:    value,
	}
	return snap, cred
}

// makeGeminiCLISessionCred builds a Credential backed by a cli_session source.
func makeGeminiCLISessionCred() (binding.Snapshot, binding.Credential) {
	snap := binding.Snapshot{
		ChainName: "google_gemini_cli",
		Source: binding.Source{
			Kind: binding.SourceCLISession,
			Tool: "gemini",
		},
		Provider: "google",
		Method:   binding.MethodCLIOAuth,
	}
	cred := binding.Credential{
		Snapshot: snap,
		CLITool:  "gemini",
	}
	return snap, cred
}

// --- BindGoogleSDK tests ---

// Test 1: env_var cred — SDKArg set to value, SourceName matches source name.
func TestBindGoogleSDK_EnvVar(t *testing.T) {
	snap, cred := makeGoogleEnvCred("GOOGLE_API_KEY", "goog-key-abc123")
	result, err := BindGoogleSDK(snap, cred, "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SDKArg != "goog-key-abc123" {
		t.Errorf("SDKArg = %q; want %q", result.SDKArg, "goog-key-abc123")
	}
	if result.SourceName != "GOOGLE_API_KEY" {
		t.Errorf("SourceName = %q; want %q", result.SourceName, "GOOGLE_API_KEY")
	}
	if result.SourceKind != "env_var" {
		t.Errorf("SourceKind = %q; want %q", result.SourceKind, "env_var")
	}
}

// Test 2: GOOGLE_GENERATIVE_AI_API_KEY — SourceName captures that exact name
// (multi-name observability: Google's third equivalent env var).
func TestBindGoogleSDK_GenerativeAIKeyName(t *testing.T) {
	snap, cred := makeGoogleEnvCred("GOOGLE_GENERATIVE_AI_API_KEY", "genai-key-xyz789")
	result, err := BindGoogleSDK(snap, cred, "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SourceName != "GOOGLE_GENERATIVE_AI_API_KEY" {
		t.Errorf("SourceName = %q; want %q", result.SourceName, "GOOGLE_GENERATIVE_AI_API_KEY")
	}
	if result.SDKArg != "genai-key-xyz789" {
		t.Errorf("SDKArg = %q; want %q", result.SDKArg, "genai-key-xyz789")
	}
}

// Test 3: empty value — must return an error.
func TestBindGoogleSDK_EmptyValue(t *testing.T) {
	snap, cred := makeGoogleEnvCred("GOOGLE_API_KEY", "")
	_, err := BindGoogleSDK(snap, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for empty credential value; got nil")
	}
}

// Test 4: cli_session cred — google_sdk doesn't support OAuth; must error.
func TestBindGoogleSDK_CLISessionRejectsOAuth(t *testing.T) {
	snap, cred := makeGeminiCLISessionCred()
	_, err := BindGoogleSDK(snap, cred, "/tmp/stage")
	if err == nil {
		t.Fatal("expected error for cli_session source kind; got nil")
	}
	if !strings.Contains(err.Error(), "unsupported source kind") {
		t.Errorf("error message %q does not mention unsupported source kind", err.Error())
	}
}

// --- BindGeminiCLI tests ---

// Test 5: env_var cred from GEMINI_API_KEY_KILROY — canonical GEMINI_API_KEY
// written to EnvSet; SourceName captures the original (non-canonical) name.
func TestBindGeminiCLI_EnvVar_CanonicalName(t *testing.T) {
	snap, cred := makeGoogleEnvCred("GEMINI_API_KEY_KILROY", "gem-kilroy-secret")
	result, err := BindGeminiCLI(snap, cred, "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// EnvSet must use the canonical CLI name.
	if val, ok := result.EnvSet["GEMINI_API_KEY"]; !ok || val != "gem-kilroy-secret" {
		t.Errorf("EnvSet[GEMINI_API_KEY] = %q; want %q", result.EnvSet["GEMINI_API_KEY"], "gem-kilroy-secret")
	}
	// Original name preserved in SourceName for observability.
	if result.SourceName != "GEMINI_API_KEY_KILROY" {
		t.Errorf("SourceName = %q; want %q", result.SourceName, "GEMINI_API_KEY_KILROY")
	}
	if result.SourceKind != "env_var" {
		t.Errorf("SourceKind = %q; want %q", result.SourceKind, "env_var")
	}
	// No files written, no scrubs.
	if len(result.FilesToWrite) != 0 {
		t.Errorf("FilesToWrite should be empty; got %v", result.FilesToWrite)
	}
	if len(result.EnvScrub) != 0 {
		t.Errorf("EnvScrub should be empty; got %v", result.EnvScrub)
	}
}

// Test 6: cli_session cred — no EnvSet, no FilesToWrite, correct source identity.
func TestBindGeminiCLI_CLISession(t *testing.T) {
	snap, cred := makeGeminiCLISessionCred()
	result, err := BindGeminiCLI(snap, cred, "/tmp/stage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.EnvSet) != 0 {
		t.Errorf("EnvSet should be empty for cli_session; got %v", result.EnvSet)
	}
	if len(result.FilesToWrite) != 0 {
		t.Errorf("FilesToWrite should be empty for cli_session; got %v", result.FilesToWrite)
	}
	if result.SourceKind != "cli_session" {
		t.Errorf("SourceKind = %q; want %q", result.SourceKind, "cli_session")
	}
	if result.SourceName != "gemini" {
		t.Errorf("SourceName = %q; want %q", result.SourceName, "gemini")
	}
	if result.SDKArg != "" {
		t.Errorf("SDKArg should be empty for cli_session; got %q", result.SDKArg)
	}
}

// Test 7: both binders registered — RegisteredDrivers includes gemini_cli and google_sdk.
func TestBindGoogle_BothDriversRegistered(t *testing.T) {
	drivers := RegisteredDrivers()
	driverSet := make(map[string]bool, len(drivers))
	for _, d := range drivers {
		driverSet[d] = true
	}
	for _, want := range []string{"gemini_cli", "google_sdk"} {
		if !driverSet[want] {
			t.Errorf("driver %q not found in RegisteredDrivers(); got %v", want, drivers)
		}
	}
}
