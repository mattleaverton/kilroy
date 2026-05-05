package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// TestAuthResolver_BindingAuthResolver_HappyPath tests the BindingAuthResolver
// happy path with a mocked bindSnapshot function.
func TestAuthResolver_BindingAuthResolver_HappyPath(t *testing.T) {
	ctx := context.Background()

	// Create a mock bindSnapshot that returns a credential.
	mockBindSnapshot := func(snap binding.Snapshot) (binding.Credential, error) {
		return binding.Credential{
			Snapshot: snap,
			Value:    "test-api-key-12345",
		}, nil
	}

	// Create the resolver.
	resolver := NewBindingAuthResolver(mockBindSnapshot, nil)

	// Create a route with a valid snapshot.
	route := AgentRoute{
		Provider: "anthropic",
		Driver:   "anthropic_sdk",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceEnvVar,
				Name: "ANTHROPIC_API_KEY",
			},
			Method: binding.MethodAPIKey,
		},
	}

	// Resolve the credential.
	cred, err := resolver.ResolveCredential(ctx, route)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify the credential.
	if cred.APIKey != "test-api-key-12345" {
		t.Errorf("expected APIKey to be 'test-api-key-12345', got: %s", cred.APIKey)
	}
	if cred.SourceIdentity != "env:ANTHROPIC_API_KEY" {
		t.Errorf("expected SourceIdentity to be 'env:ANTHROPIC_API_KEY', got: %s", cred.SourceIdentity)
	}
	if cred.SourceKind != "env_var" {
		t.Errorf("expected SourceKind to be 'env_var', got: %s", cred.SourceKind)
	}
}

// TestAuthResolver_BindingAuthResolver_WithBindFunc tests the BindingAuthResolver
// when a bind function is provided for driver-specific materialization.
func TestAuthResolver_BindingAuthResolver_WithBindFunc(t *testing.T) {
	ctx := context.Background()

	// Create a mock bindSnapshot.
	mockBindSnapshot := func(snap binding.Snapshot) (binding.Credential, error) {
		return binding.Credential{
			Snapshot: snap,
			Value:    "test-api-key",
		}, nil
	}

	// Create a mock bind function.
	mockBind := func(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
		return BindResult{
			EnvSet:       map[string]string{"CUSTOM_VAR": "custom_value"},
			EnvScrub:     []string{"SCRUB_ME"},
			FilesToWrite: map[string][]byte{"/tmp/test.txt": []byte("test")},
			SourceName:   "custom-source",
			SourceKind:   "env_var",
		}, nil
	}

	// Create the resolver with both functions.
	resolver := NewBindingAuthResolver(mockBindSnapshot, mockBind)

	// Create a route.
	route := AgentRoute{
		Provider: "openai",
		Driver:   "openai_sdk",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceEnvVar,
				Name: "OPENAI_API_KEY",
			},
			Method: binding.MethodAPIKey,
		},
	}

	// Resolve the credential.
	cred, err := resolver.ResolveCredential(ctx, route)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify the credential includes bind result data.
	if len(cred.EnvVarsToSet) != 1 || cred.EnvVarsToSet["CUSTOM_VAR"] != "custom_value" {
		t.Errorf("expected EnvVarsToSet to have CUSTOM_VAR, got: %v", cred.EnvVarsToSet)
	}
	if len(cred.EnvVarsToScrub) != 1 || cred.EnvVarsToScrub[0] != "SCRUB_ME" {
		t.Errorf("expected EnvVarsToScrub to have SCRUB_ME, got: %v", cred.EnvVarsToScrub)
	}
	if len(cred.FilesToWrite) != 1 {
		t.Errorf("expected FilesToWrite to have 1 entry, got: %d", len(cred.FilesToWrite))
	}
}

// TestAuthResolver_BindingAuthResolver_EmptySnapshot tests that the resolver
// returns an error when given an empty snapshot.
func TestAuthResolver_BindingAuthResolver_EmptySnapshot(t *testing.T) {
	ctx := context.Background()

	resolver := NewBindingAuthResolver(nil, nil)

	// Create a route with an empty snapshot.
	route := AgentRoute{
		Provider:         "anthropic",
		Driver:           "claude_cli",
		SnapshotIdentity: binding.Snapshot{},
	}

	// Resolve should fail.
	_, err := resolver.ResolveCredential(ctx, route)
	if err == nil {
		t.Fatal("expected error for empty snapshot, got nil")
	}

	expectedMsg := "empty auth snapshot"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error to contain %q, got: %v", expectedMsg, err)
	}
}

// TestAuthResolver_BindingAuthResolver_NilBindSnapshot tests that the resolver
// returns an error when bindSnapshot is nil.
func TestAuthResolver_BindingAuthResolver_NilBindSnapshot(t *testing.T) {
	ctx := context.Background()

	resolver := NewBindingAuthResolver(nil, nil)

	// Create a route with a valid snapshot.
	route := AgentRoute{
		Provider: "anthropic",
		Driver:   "claude_cli",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceCLISession,
				Tool: "claude",
			},
			Method: binding.MethodCLIOAuth,
		},
	}

	// Resolve should fail.
	_, err := resolver.ResolveCredential(ctx, route)
	if err == nil {
		t.Fatal("expected error for nil bindSnapshot, got nil")
	}

	expectedMsg := "no bindSnapshot function"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error to contain %q, got: %v", expectedMsg, err)
	}
}

// TestAuthResolver_BindingAuthResolver_BindSnapshotError tests that the resolver
// propagates errors from bindSnapshot.
func TestAuthResolver_BindingAuthResolver_BindSnapshotError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("source vanished")
	mockBindSnapshot := func(snap binding.Snapshot) (binding.Credential, error) {
		return binding.Credential{}, expectedErr
	}

	resolver := NewBindingAuthResolver(mockBindSnapshot, nil)

	route := AgentRoute{
		Provider: "google",
		Driver:   "google_sdk",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceEnvVar,
				Name: "GOOGLE_API_KEY",
			},
			Method: binding.MethodAPIKey,
		},
	}

	_, err := resolver.ResolveCredential(ctx, route)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !contains(err.Error(), "source vanished") {
		t.Errorf("expected error to contain 'source vanished', got: %v", err)
	}
}

// TestAuthResolver_StaticAuthResolver tests the StaticAuthResolver helper.
func TestAuthResolver_StaticAuthResolver(t *testing.T) {
	ctx := context.Background()

	// Test with success case.
	expectedCred := Credential{
		APIKey:         "static-key",
		SourceIdentity: "static-source",
		SourceKind:     "env_var",
	}
	resolver := NewStaticAuthResolver(expectedCred, nil)

	// Route doesn't matter for static resolver.
	route := AgentRoute{
		Provider: "any",
		Driver:   "any",
	}

	cred, err := resolver.ResolveCredential(ctx, route)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cred.APIKey != expectedCred.APIKey {
		t.Errorf("expected APIKey %q, got %q", expectedCred.APIKey, cred.APIKey)
	}

	// Test with error case.
	expectedErr := errors.New("static error")
	errorResolver := NewStaticAuthResolver(Credential{}, expectedErr)

	_, err = errorResolver.ResolveCredential(ctx, route)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

// TestAuthResolver_FakeAuthResolver tests the FakeAuthResolver helper.
func TestAuthResolver_FakeAuthResolver(t *testing.T) {
	ctx := context.Background()

	resolver := NewFakeAuthResolver()

	// Configure specific credentials.
	anthropicCred := Credential{APIKey: "anthropic-key"}
	openaiCred := Credential{APIKey: "openai-key"}

	resolver.SetCredential("anthropic", "claude_cli", anthropicCred)
	resolver.SetCredential("openai", "codex_cli", openaiCred)

	// Test resolving anthropic.
	anthropicRoute := AgentRoute{Provider: "anthropic", Driver: "claude_cli"}
	cred, err := resolver.ResolveCredential(ctx, anthropicRoute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.APIKey != "anthropic-key" {
		t.Errorf("expected anthropic-key, got %s", cred.APIKey)
	}

	// Test resolving openai.
	openaiRoute := AgentRoute{Provider: "openai", Driver: "codex_cli"}
	cred, err = resolver.ResolveCredential(ctx, openaiRoute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.APIKey != "openai-key" {
		t.Errorf("expected openai-key, got %s", cred.APIKey)
	}

	// Test that routes were recorded.
	if len(resolver.Routes) != 2 {
		t.Errorf("expected 2 routes recorded, got %d", len(resolver.Routes))
	}

	// Test default/error case.
	resolver.DefError = errors.New("not found")
	unknownRoute := AgentRoute{Provider: "unknown", Driver: "unknown"}
	_, err = resolver.ResolveCredential(ctx, unknownRoute)
	if err != resolver.DefError {
		t.Errorf("expected default error, got %v", err)
	}
}

// TestAuthResolver_FakeAuthResolver_SetError tests the FakeAuthResolver error configuration.
func TestAuthResolver_FakeAuthResolver_SetError(t *testing.T) {
	ctx := context.Background()

	resolver := NewFakeAuthResolver()

	expectedErr := errors.New("auth failed")
	resolver.SetError("anthropic", "claude_cli", expectedErr)

	route := AgentRoute{Provider: "anthropic", Driver: "claude_cli"}
	_, err := resolver.ResolveCredential(ctx, route)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

// TestAuthResolver_BindingAuthResolver_BindFuncError tests that the resolver
// propagates errors from the bind function (no silent fallback).
func TestAuthResolver_BindingAuthResolver_BindFuncError(t *testing.T) {
	ctx := context.Background()

	// Create a mock bindSnapshot that returns a credential.
	mockBindSnapshot := func(snap binding.Snapshot) (binding.Credential, error) {
		return binding.Credential{
			Snapshot: snap,
			Value:    "test-api-key",
		}, nil
	}

	// Create a mock bind function that returns an error.
	expectedErr := errors.New("env scrub failed: ANTHROPIC_API_KEY")
	mockBind := func(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (BindResult, error) {
		return BindResult{}, expectedErr
	}

	// Create the resolver.
	resolver := NewBindingAuthResolver(mockBindSnapshot, mockBind)

	// Create a route.
	route := AgentRoute{
		Provider: "anthropic",
		Driver:   "claude_cli",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceCLISession,
				Tool: "claude",
			},
			Method: binding.MethodCLIOAuth,
		},
	}

	// Resolve should fail with the bind error, not silently succeed.
	_, err := resolver.ResolveCredential(ctx, route)
	if err == nil {
		t.Fatal("expected error when bind function fails, got nil")
	}

	if !contains(err.Error(), "env scrub failed") {
		t.Errorf("expected error to contain 'env scrub failed', got: %v", err)
	}

	if !contains(err.Error(), "claude_cli") {
		t.Errorf("expected error to contain driver name 'claude_cli', got: %v", err)
	}
}

// TestAuthResolver_CLIAuthSession tests that CLI session credentials are handled correctly.
func TestAuthResolver_CLIAuthSession(t *testing.T) {
	ctx := context.Background()

	mockBindSnapshot := func(snap binding.Snapshot) (binding.Credential, error) {
		// For CLI sessions, Value is empty but CLITool is set.
		return binding.Credential{
			Snapshot: snap,
			CLITool:  "claude",
		}, nil
	}

	resolver := NewBindingAuthResolver(mockBindSnapshot, nil)

	route := AgentRoute{
		Provider: "anthropic",
		Driver:   "claude_cli",
		SnapshotIdentity: binding.Snapshot{
			Source: binding.Source{
				Kind: binding.SourceCLISession,
				Tool: "claude",
			},
			Method: binding.MethodCLIOAuth,
		},
	}

	cred, err := resolver.ResolveCredential(ctx, route)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// For CLI sessions, APIKey should be empty.
	if cred.APIKey != "" {
		t.Errorf("expected empty APIKey for CLI session, got: %s", cred.APIKey)
	}

	if cred.SourceKind != "cli_session" {
		t.Errorf("expected SourceKind 'cli_session', got: %s", cred.SourceKind)
	}
}

// Helper function.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) > 0 &&
		(s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
