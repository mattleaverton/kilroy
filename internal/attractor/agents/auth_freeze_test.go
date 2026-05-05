// Regression coverage for the P0-A invariant on the tmux/CLI path:
// materializeCredential must consult ONLY the prelaunch snapshot when
// re-checking the selected source's liveness. It must NOT reload the
// worktree's .kilroy/auth.toml — drift between prelaunch (run from
// source workspace) and execution (run from worktree, possibly with no
// project auth.toml checked in) cannot silently re-route.
package agents

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// TestMaterializeCredential_DoesNotReloadProjectAuthFromWorktree is the
// load-bearing regression test for P0-A on the tmux path. Setup:
//   - $XDG_CONFIG_HOME points at a directory with no kilroy/auth.toml
//   - $HOME points at a directory with no .config/kilroy/auth.toml
//   - The "worktree" (exec.WorktreeDir) is a tempdir with no .kilroy/auth.toml
//   - The frozen snapshot's source env var IS set
//
// Prior behavior: materializeCredential called engine.DefaultBindingResolver
// against exec.WorktreeDir, which ran binding.LoadConfig and returned
// ErrNoConfig — failing execution even though the snapshot was valid.
//
// New behavior: materializeCredential calls engine.BindSnapshot, which
// validates source liveness via the live process env and auth detector
// only. No auth.toml reload. The bind succeeds.
func TestMaterializeCredential_DoesNotReloadProjectAuthFromWorktree(t *testing.T) {
	// Strip every layer the binding-config loader could see: user, project,
	// xdg. If materializeCredential were still calling DefaultBindingResolver
	// against the worktree, this setup would force ErrNoConfig.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("FROZEN_API_KEY", "snapshot-survived-config-strip")

	stageDir := t.TempDir()

	// Frozen snapshot: anthropic_sdk + env_var source. The driver's binder
	// (BindAnthropicSDK) accepts env_var snapshots and produces an SDKArg
	// equal to the credential value.
	snap := binding.Snapshot{
		ChainName: "frozen_chain",
		Method:    binding.MethodAPIKey,
		Provider:  "anthropic",
		Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "FROZEN_API_KEY"},
	}
	route := engine.AgentRoute{
		NodeID:   "agent",
		Source:   "policy_class:hard_coding",
		Class:    "hard_coding",
		Provider: "anthropic",
		Model:    "claude-opus-4-7",
		Driver:   "anthropic_sdk",
		Backend:  engine.BackendAPI,
		ClassResult: &policy.ResolveResult{
			ModelID:      "claude-opus-4-7",
			Driver:       "anthropic_sdk",
			AuthSnapshot: snap,
		},
	}

	bindResult, err := transport.MaterializeCredential(route.Driver, snap, stageDir, engine.BindSnapshot, testBindWrapper)
	if err != nil {
		t.Fatalf("materializeCredential: %v", err)
	}
	if bindResult.SDKArg != "snapshot-survived-config-strip" {
		t.Errorf("BindResult.SDKArg = %q, want snapshot-survived-config-strip — frozen snapshot must survive worktree without auth.toml",
			bindResult.SDKArg)
	}
	if bindResult.SourceName != "FROZEN_API_KEY" {
		t.Errorf("BindResult.SourceName = %q, want FROZEN_API_KEY", bindResult.SourceName)
	}
}

// TestMaterializeCredential_VanishedSourceFailsLoudly is the negative
// case: a snapshot whose env var got unset between prelaunch and
// execution must produce a decisive error, even though we no longer
// reload auth.toml from the worktree.
func TestMaterializeCredential_VanishedSourceFailsLoudly(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	// VANISHED_KEY deliberately not set.

	stageDir := t.TempDir()

	snap := binding.Snapshot{
		ChainName: "frozen_chain",
		Method:    binding.MethodAPIKey,
		Provider:  "anthropic",
		Source:    binding.Source{Kind: binding.SourceEnvVar, Name: "VANISHED_KEY"},
	}
	route := engine.AgentRoute{
		NodeID:   "agent",
		Provider: "anthropic",
		Driver:   "anthropic_sdk",
		Backend:  engine.BackendAPI,
		ClassResult: &policy.ResolveResult{
			Driver:       "anthropic_sdk",
			AuthSnapshot: snap,
		},
	}

	if _, err := transport.MaterializeCredential(route.Driver, snap, stageDir, engine.BindSnapshot, testBindWrapper); err == nil {
		t.Fatal("expected error for vanished env_var source, got nil")
	}
}

// testBindWrapper wraps engine.Bind to return transport.BindResult.
func testBindWrapper(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (transport.BindResult, error) {
	engResult, err := engine.Bind(driver, snap, cred, stageDir)
	if err != nil {
		return transport.BindResult{}, err
	}
	return transport.BindResult{
		EnvSet:       engResult.EnvSet,
		EnvScrub:     engResult.EnvScrub,
		FilesToWrite: engResult.FilesToWrite,
		SDKArg:       engResult.SDKArg,
		SourceName:   engResult.SourceName,
		SourceKind:   engResult.SourceKind,
	}, nil
}
