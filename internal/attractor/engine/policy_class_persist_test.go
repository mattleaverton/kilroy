// Coverage for Block 4 Step 5: ResolveAgentClass writes the full resolution
// (including skipped candidates and policy version) to a per-step
// resolution.json artifact matching plan §6.4.
package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/policy"
)

func TestResolveAgentClass_PersistsResolutionJSON(t *testing.T) {
	logsRoot := t.TempDir()

	// Two-candidate chain: first candidate gated on a missing env var, so
	// it gets skipped; second candidate (CLI session, present) wins. This
	// way resolution.json must capture both the resolved tuple AND the
	// skip record with its structured reason.
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-2.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Description: "test class",
				Chain: []policy.Candidate{
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "anthropic_sdk",
						Transport:   "http",
						HistorySink: "anthropic-sse",
						Auth: policy.AuthReq{
							Kind:   "env_var",
							EnvVar: "ANTHROPIC_API_KEY",
						},
					},
					{
						ModelID:     "claude-sonnet-4-6",
						Driver:      "claude_cli",
						Transport:   "cli_subprocess",
						HistorySink: "claude-cli-jsonl",
						Auth: policy.AuthReq{
							Kind: "cli_session",
							CLI:  "claude",
						},
					},
				},
			},
		},
	}
	state := policy.MachineState{
		Auth: auth.ListOutput{
			Entries: []auth.Entry{
				{
					ID:    "anthropic.cli.claude",
					Tool:  "claude",
					Kind:  auth.KindCLIOAuth,
					State: auth.StateOK,
				},
			},
		},
	}

	exec := &Execution{
		Graph:    model.NewGraph("dogfood-graph"),
		LogsRoot: logsRoot,
		Engine: &Engine{
			LogsRoot: logsRoot,
			Graph:    model.NewGraph("dogfood-graph"),
			Options:  RunOptions{RunID: "test-step5"},
		},
	}

	node := model.NewNode("agent")
	node.Attrs["class"] = "hard_coding"

	cls, ok, err := ResolveAgentClass(node, exec, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err != nil {
		t.Fatalf("ResolveAgentClass: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true on a class hit")
	}
	if cls.Driver != "claude_cli" {
		t.Fatalf("driver = %q, want claude_cli (chain should have skipped the missing-env-var candidate)", cls.Driver)
	}

	path := filepath.Join(logsRoot, "agent", "resolution.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read resolution.json: %v", err)
	}

	var got resolutionRecord
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal resolution.json: %v", err)
	}

	if got.SchemaVersion != "1" {
		t.Errorf("schema_version = %q, want 1", got.SchemaVersion)
	}
	if got.NodeID != "agent" {
		t.Errorf("node_id = %q, want agent", got.NodeID)
	}
	if got.WorkflowID != "dogfood-graph" {
		t.Errorf("workflow_id = %q, want dogfood-graph", got.WorkflowID)
	}
	if got.Resolution.Requested.Type != "class" {
		t.Errorf("requested.type = %q, want class", got.Resolution.Requested.Type)
	}
	if got.Resolution.Requested.Value != "hard_coding" {
		t.Errorf("requested.value = %q, want hard_coding", got.Resolution.Requested.Value)
	}
	if got.Resolution.Resolved.ModelID != "claude-sonnet-4-6" {
		t.Errorf("resolved.model_id = %q, want claude-sonnet-4-6", got.Resolution.Resolved.ModelID)
	}
	if got.Resolution.Resolved.Driver != "claude_cli" {
		t.Errorf("resolved.driver = %q, want claude_cli", got.Resolution.Resolved.Driver)
	}
	if got.Resolution.Resolved.AuthMethod != "cli_session" {
		t.Errorf("resolved.auth_method = %q, want cli_session", got.Resolution.Resolved.AuthMethod)
	}
	if got.Resolution.Resolved.TurnCodec != "claude-cli-jsonl" {
		t.Errorf("resolved.turn_codec = %q, want claude-cli-jsonl", got.Resolution.Resolved.TurnCodec)
	}
	if got.Resolution.FallbackRank != 1 {
		t.Errorf("fallback_rank = %d, want 1 (first candidate skipped)", got.Resolution.FallbackRank)
	}
	if got.Resolution.PolicyVersion != "test-2.0" {
		t.Errorf("policy_version = %q, want test-2.0", got.Resolution.PolicyVersion)
	}
	if got.Resolution.ResolvedAt == "" {
		t.Error("resolved_at is empty, want RFC3339Nano timestamp")
	}
	if len(got.Resolution.Skipped) != 1 {
		t.Fatalf("skipped len = %d, want 1", len(got.Resolution.Skipped))
	}
	skip := got.Resolution.Skipped[0]
	if skip.Rank != 0 {
		t.Errorf("skipped[0].rank = %d, want 0", skip.Rank)
	}
	if skip.ModelID != "claude-opus-4-7" {
		t.Errorf("skipped[0].model_id = %q, want claude-opus-4-7", skip.ModelID)
	}
	if skip.Driver != "anthropic_sdk" {
		t.Errorf("skipped[0].driver = %q, want anthropic_sdk", skip.Driver)
	}
	if skip.Reason != "env_var_missing:ANTHROPIC_API_KEY" {
		t.Errorf("skipped[0].reason = %q, want env_var_missing:ANTHROPIC_API_KEY", skip.Reason)
	}
}

// TestResolveAgentClass_PersistResolution_NoLogsRootIsSafe verifies that
// when LogsRoot is empty (no logs to persist into), the helper still
// returns the in-memory result without panicking and without a stray write.
func TestResolveAgentClass_PersistResolution_NoLogsRootIsSafe(t *testing.T) {
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Chain: []policy.Candidate{
					{
						ModelID: "claude-opus-4-7",
						Driver:  "claude_cli",
						Auth:    policy.AuthReq{Kind: "cli_session", CLI: "claude"},
					},
				},
			},
		},
	}
	state := policy.MachineState{
		Auth: auth.ListOutput{
			Entries: []auth.Entry{{
				ID: "x", Tool: "claude", Kind: auth.KindCLIOAuth, State: auth.StateOK,
			}},
		},
	}

	node := model.NewNode("agent")
	node.Attrs["class"] = "hard_coding"

	cls, ok, err := ResolveAgentClass(node, nil, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err != nil {
		t.Fatalf("ResolveAgentClass with nil exec: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if cls.Driver != "claude_cli" {
		t.Errorf("driver = %q, want claude_cli", cls.Driver)
	}
}
