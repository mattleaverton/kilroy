// Coverage for prelaunch snapshot freeze (Fix R1):
//   - prelaunch writes prelaunch_snapshots.json keyed by node_id
//   - ResolveAgentClass reads that snapshot and skips live resolution
//   - When env/config drift would change the route, the frozen snapshot wins
//   - Missing snapshot file → fall back to live resolution

package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// TestValidatePreLaunch_WritesPrelaunchSnapshots verifies that prelaunch
// produces prelaunch_snapshots.json with the resolved route per agent
// node. Plan §5: prelaunch is the authoritative snapshot.
func TestValidatePreLaunch_WritesPrelaunchSnapshots(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "hard_coding",
	})
	logsRoot := t.TempDir()

	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Chain: []policy.Candidate{{
					ModelID:  "claude-opus-4-7",
					Driver:   "anthropic_sdk",
					Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				}},
			},
		},
	}

	_, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: stateResolver(t, _stateChains),
	})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}

	snapPath := filepath.Join(logsRoot, "prelaunch_snapshots.json")
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read prelaunch_snapshots.json: %v", err)
	}
	var store preLaunchSnapshotStore
	if err := json.Unmarshal(raw, &store); err != nil {
		t.Fatalf("decode snapshots: %v", err)
	}
	if store.SchemaVersion != "1" {
		t.Errorf("schema_version = %q, want 1", store.SchemaVersion)
	}
	snap, ok := store.Snapshots["agent"]
	if !ok {
		t.Fatalf("no snapshot for node 'agent'; got %v", store.Snapshots)
	}
	if snap.ClassName != "hard_coding" {
		t.Errorf("class_name = %q, want hard_coding", snap.ClassName)
	}
	if snap.Driver != "anthropic_sdk" {
		t.Errorf("driver = %q, want anthropic_sdk", snap.Driver)
	}
	if snap.Auth.ChainName != "anthropic_api_key" {
		t.Errorf("auth.chain_name = %q, want anthropic_api_key", snap.Auth.ChainName)
	}
	if snap.Auth.Source.Kind != "env_var" {
		t.Errorf("auth.source.kind = %q, want env_var", snap.Auth.Source.Kind)
	}
	if snap.Auth.Source.Name != "ANTHROPIC_API_KEY" {
		t.Errorf("auth.source.name = %q, want ANTHROPIC_API_KEY", snap.Auth.Source.Name)
	}
}

// TestResolveAgentClass_PrefersFrozenSnapshot verifies that when a
// prelaunch snapshot exists, ResolveAgentClass uses it INSTEAD of
// running policy.Resolve. The test injects a snapshot pointing to one
// route, then calls ResolveAgentClass with PolicyDeps that would
// resolve to a different route — the snapshot must win.
func TestResolveAgentClass_PrefersFrozenSnapshot(t *testing.T) {
	logsRoot := t.TempDir()

	// Write a snapshot saying "agent resolves to claude_cli / cli_session".
	snap := preLaunchNodeSnap{
		ClassName:     "hard_coding",
		ModelID:       "claude-opus-4-7",
		Driver:        "claude_cli",
		Transport:     "cli_subprocess",
		HistorySink:   "jsonl_local",
		FallbackRank:  0,
		PolicyVersion: "test-snap",
		Auth: snapAuth{
			ChainName: "anthropic_claude_cli",
			Method:    "cli_oauth",
			Provider:  "anthropic",
			Source:    snapAuthSource{Kind: "cli_session", Tool: "claude"},
		},
	}
	if err := writePreLaunchSnapshots(logsRoot, map[string]preLaunchNodeSnap{"agent": snap}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Build PolicyDeps that would resolve to a DIFFERENT route if used —
	// anthropic_sdk via env_var. If the snapshot is honored, these stubs
	// should never run.
	resolveCalled := false
	loadCalled := false

	deps := PolicyDeps{
		Load: func() (*policy.Data, error) {
			loadCalled = true
			return &policy.Data{
				SchemaVersion: "1",
				PolicyVersion: "live",
				Classes: map[string]policy.Class{
					"hard_coding": {Chain: []policy.Candidate{{
						ModelID:  "DIFFERENT-MODEL",
						Driver:   "anthropic_sdk",
						Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
					}}},
				},
			}, nil
		},
		Resolver: func(string) (*binding.Resolver, error) {
			resolveCalled = true
			r, _ := prelaunchResolverAnthropicEnv(t)("")
			return r, nil
		},
	}

	exec := &Execution{
		Graph:    model.NewGraph("test"),
		LogsRoot: logsRoot,
		Engine: &Engine{
			LogsRoot: logsRoot,
			Options:  RunOptions{RunID: "test-snap-prefer"},
		},
	}
	node := model.NewNode("agent")
	node.Attrs["agent_class"] = "hard_coding"

	cls, ok, err := ResolveAgentClass(node, exec, deps)
	if err != nil {
		t.Fatalf("ResolveAgentClass: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if cls.Driver != "claude_cli" {
		t.Errorf("driver = %q, want claude_cli (from snapshot)", cls.Driver)
	}
	if cls.Model != "claude-opus-4-7" {
		t.Errorf("model = %q, want claude-opus-4-7 (from snapshot)", cls.Model)
	}
	if loadCalled {
		t.Error("policy.Load should NOT be called when snapshot is present")
	}
	if resolveCalled {
		t.Error("auth resolver should NOT be called when snapshot is present")
	}
}

// TestResolveAgentClass_NoSnapshot_FallsBackToLiveResolution verifies that
// when no snapshot file exists (e.g., test or ad-hoc invocation),
// ResolveAgentClass falls back to running the resolver fresh.
func TestResolveAgentClass_NoSnapshot_FallsBackToLiveResolution(t *testing.T) {
	logsRoot := t.TempDir()
	// No snapshot file; both deps should be called.
	loadCalled := false
	resolveCalled := false
	deps := PolicyDeps{
		Load: func() (*policy.Data, error) {
			loadCalled = true
			return &policy.Data{
				SchemaVersion: "1",
				PolicyVersion: "live",
				Classes: map[string]policy.Class{
					"hard_coding": {Chain: []policy.Candidate{{
						ModelID:  "claude-opus-4-7",
						Driver:   "anthropic_sdk",
						Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
					}}},
				},
			}, nil
		},
		Resolver: func(string) (*binding.Resolver, error) {
			resolveCalled = true
			r, _ := prelaunchResolverAnthropicEnv(t)("")
			return r, nil
		},
	}

	exec := &Execution{
		Graph:    model.NewGraph("test"),
		LogsRoot: logsRoot,
		Engine: &Engine{
			LogsRoot: logsRoot,
			Options:  RunOptions{RunID: "test-no-snap"},
		},
	}
	node := model.NewNode("agent")
	node.Attrs["agent_class"] = "hard_coding"

	cls, ok, err := ResolveAgentClass(node, exec, deps)
	if err != nil {
		t.Fatalf("ResolveAgentClass: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if cls.Driver != "anthropic_sdk" {
		t.Errorf("driver = %q, want anthropic_sdk (live resolution)", cls.Driver)
	}
	if !loadCalled {
		t.Error("policy.Load SHOULD be called when no snapshot is present")
	}
	if !resolveCalled {
		t.Error("auth resolver SHOULD be called when no snapshot is present")
	}
}
