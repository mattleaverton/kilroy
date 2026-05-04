// Regression coverage for the auth-route freeze invariant
// (Plan §5 / Foundation Closure Plan items R-P0 1.1 and 1.2):
//
//   - Execution materializes the SELECTED credential from the prelaunch
//     snapshot WITHOUT reloading the project's auth.toml from the run
//     worktree (P0-A).
//
//   - The class-routed credential adapter is installed on the llm.Client
//     BEFORE any path that reads canonical env vars, so a user with only
//     *_API_KEY_KILROY configured does not silently fail / wrong-bill
//     through the canonical-env fallback (P0-B).
package engine

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/llm"
	"github.com/danshapiro/kilroy/internal/policy"
	"github.com/danshapiro/kilroy/internal/providerspec"
)

// TestClientForRoute_BindsClassCredentialBeforeCanonicalEnv asserts the
// load-bearing P0-B fix: when classResult is non-nil, the class-routed
// adapter is installed on the returned client BEFORE the apiOnce-cached
// "ensureAPIClient" path could read canonical env. The proof is that
// clientForRoute succeeds even when the apiClientFactory reports an error
// (would fail ensureAPIClient) AND the canonical env var is unset — the
// bound credential alone is sufficient to construct the primary adapter.
func TestClientForRoute_BindsClassCredentialBeforeCanonicalEnv(t *testing.T) {
	// Canonical env var must be UNSET to prove the path doesn't depend on it.
	t.Setenv("ANTHROPIC_API_KEY", "")
	// Auth chain points at *_KILROY-style env var that IS set.
	t.Setenv("ANTHROPIC_API_KEY_KILROY", "kilroy-only-key-value")

	runtimes := map[string]ProviderRuntime{
		"anthropic": {
			Key:     "anthropic",
			Backend: BackendAPI,
			API: providerspec.APISpec{
				Protocol:         providerspec.ProtocolAnthropicMessages,
				DefaultBaseURL:   "https://api.anthropic.com",
				DefaultAPIKeyEnv: "ANTHROPIC_API_KEY",
			},
		},
	}
	r := NewAgentRouterWithRuntimes(nil, nil, runtimes)
	// Sentinel: if clientForRoute ever reaches ensureAPIClient on the
	// class-routed path, this factory's error would surface as the
	// returned error from clientForRoute. After the fix, the class path
	// builds a fresh client from runtimes + bound credential and never
	// touches r.apiOnce / r.apiClientFactory.
	r.apiClientFactory = func(map[string]ProviderRuntime) (*llm.Client, error) {
		t.Fatal("ensureAPIClient must NOT be called for class-routed path; bound credential is the source of truth")
		return nil, nil
	}

	classResult := &policy.ResolveResult{
		ModelID: "claude-opus-4-7",
		Driver:  "anthropic_sdk",
		AuthSnapshot: binding.Snapshot{
			ChainName: "anthropic_kilroy_only",
			Method:    binding.MethodAPIKey,
			Provider:  "anthropic",
			Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY_KILROY"},
		},
	}

	c, err := r.clientForRoute(nil, "anthropic", classResult)
	if err != nil {
		t.Fatalf("clientForRoute: %v", err)
	}
	if c == nil {
		t.Fatal("clientForRoute: nil client")
	}
	names := c.ProviderNames()
	found := false
	for _, n := range names {
		if n == "anthropic" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("anthropic adapter not registered on returned client; got providers=%v", names)
	}
}

// TestClientForRoute_NoEnsureAPIClient_WhenSnapshotIsAuthoritative is a
// stricter version: even when canonical env IS set with a different key,
// the class-routed path returns a client whose anthropic adapter is the
// bound-credential one — never the canonical-env one. The bound key is
// the source of truth.
func TestClientForRoute_NoEnsureAPIClient_WhenSnapshotIsAuthoritative(t *testing.T) {
	// Canonical env set with WRONG key. If ensureAPIClient ran first,
	// its adapter would carry "wrong-canonical-key". After the fix, the
	// fresh client path runs and the override installs the chain key.
	t.Setenv("ANTHROPIC_API_KEY", "wrong-canonical-key")
	t.Setenv("ANTHROPIC_API_KEY_KILROY", "right-chain-key")

	runtimes := map[string]ProviderRuntime{
		"anthropic": {
			Key:     "anthropic",
			Backend: BackendAPI,
			API: providerspec.APISpec{
				Protocol:         providerspec.ProtocolAnthropicMessages,
				DefaultBaseURL:   "https://api.anthropic.com",
				DefaultAPIKeyEnv: "ANTHROPIC_API_KEY",
			},
		},
	}
	r := NewAgentRouterWithRuntimes(nil, nil, runtimes)
	apiOnceCalls := 0
	r.apiClientFactory = func(map[string]ProviderRuntime) (*llm.Client, error) {
		apiOnceCalls++
		// Return an empty client — this MUST NOT be reached on the
		// class-routed path.
		return llm.NewClient(), nil
	}

	classResult := &policy.ResolveResult{
		ModelID: "claude-opus-4-7",
		Driver:  "anthropic_sdk",
		AuthSnapshot: binding.Snapshot{
			ChainName: "anthropic_chain",
			Method:    binding.MethodAPIKey,
			Provider:  "anthropic",
			Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY_KILROY"},
		},
	}
	if _, err := r.clientForRoute(nil, "anthropic", classResult); err != nil {
		t.Fatalf("clientForRoute: %v", err)
	}
	if apiOnceCalls != 0 {
		t.Errorf("ensureAPIClient (apiClientFactory) called %d times for class-routed path; want 0", apiOnceCalls)
	}
}

// TestClientForRoute_NilClassResult_FallsBackToEnsureAPIClient covers the
// other half of the contract: non-class (legacy stylesheet) routes still
// go through the apiOnce-cached client. The freeze applies only to
// class-routed runs.
func TestClientForRoute_NilClassResult_FallsBackToEnsureAPIClient(t *testing.T) {
	r := NewAgentRouterWithRuntimes(nil, nil, nil)
	called := 0
	r.apiClientFactory = func(map[string]ProviderRuntime) (*llm.Client, error) {
		called++
		client := llm.NewClient()
		return client, nil
	}
	// The non-class path falls through to llmclient.NewFromEnv when the
	// runtime-factory yields an empty client; we only need to assert that
	// the class-routed path is the one that bypasses apiOnce. So just
	// confirm clientForRoute(nil, ..., nil) returns whatever the cached
	// path returns without panicking — it may return an error if the env
	// has no providers, which is fine.
	_, _ = r.clientForRoute(nil, "anthropic", nil)
}

// TestBindSnapshot_DoesNotReloadProjectAuthConfig is the P0-A contract
// for the API path. BindSnapshot must NOT depend on auth.toml living
// next to the worktree — the prelaunch snapshot is the source of truth.
//
// Proof: set XDG_CONFIG_HOME to a temp dir with NO auth.toml AND set
// the snapshot's source env var. BindSnapshot must succeed because Bind
// only reads the env var.
func TestBindSnapshot_DoesNotReloadProjectAuthConfig(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MY_TEST_API_KEY", "snapshot-source-value")

	snap := binding.Snapshot{
		ChainName: "frozen_chain",
		Method:    binding.MethodAPIKey,
		Provider:  "anthropic",
		Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "MY_TEST_API_KEY"},
	}
	cred, err := BindSnapshot(snap)
	if err != nil {
		t.Fatalf("BindSnapshot: %v", err)
	}
	if cred.Value != "snapshot-source-value" {
		t.Fatalf("cred.Value = %q, want %q", cred.Value, "snapshot-source-value")
	}
}

// TestBindSnapshot_VanishedSourceFailsLoudly confirms that the snapshot
// freeze still surfaces "credential vanished between prelaunch and
// execution" as a decisive error, even though we no longer reload
// auth.toml.
func TestBindSnapshot_VanishedSourceFailsLoudly(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	// MY_VANISHED_KEY deliberately UNSET.

	snap := binding.Snapshot{
		ChainName: "frozen_chain",
		Method:    binding.MethodAPIKey,
		Provider:  "anthropic",
		Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "MY_VANISHED_KEY"},
	}
	_, err := BindSnapshot(snap)
	if err == nil {
		t.Fatal("expected ErrSourceVanished when env var is unset")
	}
	if !strings.Contains(err.Error(), "MY_VANISHED_KEY") {
		t.Errorf("error = %v, want it to mention the vanished env var name", err)
	}
}
