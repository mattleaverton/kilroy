// Tests for RunDB lifecycle hooks. Focus: provider_selections must
// reflect the class-resolved route, not the static stylesheet defaults
// that an agent_class= node may also carry.
package engine

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
)

// recordingRunDBWriter captures RecordProviderSelection calls for
// assertions; all other RunDBWriter methods no-op.
type recordingRunDBWriter struct {
	provider            []providerSelectionCall
	capturedParentRunID string
}

type providerSelectionCall struct {
	runID, nodeID, provider, model, backend string
	attempt                                 int
}

func (r *recordingRunDBWriter) RecordRunStart(runID, graphName, goal, status, logsRoot, worktreeDir, runBranch, repoPath, dotSource string, inputs map[string]any, labels map[string]string, invocation []string, config map[string]any, parentRunID string) error {
	r.capturedParentRunID = parentRunID
	return nil
}
func (r *recordingRunDBWriter) RecordRunComplete(runID, status, failureReason, finalSHA string, warnings []string) error {
	return nil
}
func (r *recordingRunDBWriter) RecordNodeStart(runID, nodeID string, attempt int, handlerType string) (int64, error) {
	return 0, nil
}
func (r *recordingRunDBWriter) RecordNodeComplete(id int64, status, failureReason, failureClass, preferredLabel, notes string, contextUpdates map[string]any) error {
	return nil
}
func (r *recordingRunDBWriter) RecordEdgeDecision(runID, fromNode, toNode, edgeLabel, condition, reason string) error {
	return nil
}
func (r *recordingRunDBWriter) RecordProviderSelection(runID, nodeID string, attempt int, provider, model, backend string) error {
	r.provider = append(r.provider, providerSelectionCall{runID, nodeID, provider, model, backend, attempt})
	return nil
}
func (r *recordingRunDBWriter) RecordNodeDiff(runID, nodeID string, attempt int, beforeSHA, afterSHA string, filesChanged, insertions, deletions int) error {
	return nil
}
func (r *recordingRunDBWriter) RecordNodeArtifact(nodeExecID int64, name, contentType string, content []byte, truncated bool) error {
	return nil
}

// TestRundbRecordProviderIfAgent_ClassRouted_UsesResolvedRoute verifies
// that for nodes with agent_class=, the recorded provider/model/backend
// reflect the prelaunch-resolved route — not the contradictory static
// stylesheet defaults the node also carries. This is what makes the DB
// row a true audit of what executed.
func TestRundbRecordProviderIfAgent_ClassRouted_UsesResolvedRoute(t *testing.T) {
	logsRoot := t.TempDir()

	// Frozen prelaunch snapshot: class-resolved to openai_sdk + gpt-5 + api,
	// even though the node's static attrs say anthropic + claude-sonnet-4.6.
	snap := preLaunchNodeSnap{
		ClassName: "hard_coding",
		ModelID:   "gpt-5",
		Driver:    "openai_sdk",
		Backend:   BackendAPI,
		Auth: snapAuth{
			ChainName: "openai_api_key",
			Method:    "api_key",
			Provider:  "openai",
			Source:    snapAuthSource{Kind: "env_var", Name: "OPENAI_API_KEY"},
		},
	}
	if err := writePreLaunchSnapshots(logsRoot, map[string]preLaunchNodeSnap{"agent": snap}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	graph := model.NewGraph("test")
	node := model.NewNode("agent")
	node.Attrs["agent_class"] = "hard_coding"
	node.Attrs["llm_provider"] = "anthropic"
	node.Attrs["llm_model"] = "claude-sonnet-4.6"
	if err := graph.AddNode(node); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	rec := &recordingRunDBWriter{}
	e := &Engine{
		Graph:    graph,
		LogsRoot: logsRoot,
		Options:  RunOptions{RunID: "run-class-routed"},
		RunDB:    rec,
	}

	e.rundbRecordProviderIfAgent("agent", 1)

	if len(rec.provider) != 1 {
		t.Fatalf("expected 1 RecordProviderSelection call, got %d", len(rec.provider))
	}
	got := rec.provider[0]
	if got.provider != "openai" {
		t.Errorf("provider = %q, want openai (resolved, not static anthropic)", got.provider)
	}
	if got.model != "gpt-5" {
		t.Errorf("model = %q, want gpt-5 (resolved, not static claude-sonnet-4.6)", got.model)
	}
	if got.backend != "api" {
		t.Errorf("backend = %q, want api (SDK driver)", got.backend)
	}
	if got.runID != "run-class-routed" || got.nodeID != "agent" || got.attempt != 1 {
		t.Errorf("identity mismatch: runID=%q nodeID=%q attempt=%d", got.runID, got.nodeID, got.attempt)
	}
}

// TestRundbRecordProviderIfAgent_NonClass_UsesStaticAttrs confirms the
// pre-existing legacy behavior is unchanged for nodes that are routed
// via llm_provider= / agent_tool= directly. No agent_class means no
// snapshot lookup — the recorded row mirrors node.Attrs.
func TestRundbRecordProviderIfAgent_NonClass_UsesStaticAttrs(t *testing.T) {
	logsRoot := t.TempDir()

	graph := model.NewGraph("test")
	node := model.NewNode("agent")
	node.Attrs["llm_provider"] = "anthropic"
	node.Attrs["llm_model"] = "claude-sonnet-4-6"
	node.Attrs["agent_tool"] = "claude"
	if err := graph.AddNode(node); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	rec := &recordingRunDBWriter{}
	e := &Engine{
		Graph:    graph,
		LogsRoot: logsRoot,
		Options:  RunOptions{RunID: "run-non-class"},
		RunDB:    rec,
	}

	e.rundbRecordProviderIfAgent("agent", 1)

	if len(rec.provider) != 1 {
		t.Fatalf("expected 1 RecordProviderSelection call, got %d", len(rec.provider))
	}
	got := rec.provider[0]
	if got.provider != "anthropic" {
		t.Errorf("provider = %q, want anthropic", got.provider)
	}
	if got.model != "claude-sonnet-4-6" {
		t.Errorf("model = %q, want claude-sonnet-4-6", got.model)
	}
	// Legacy non-class path uses agent_tool as the backend literal.
	if got.backend != "claude" {
		t.Errorf("backend = %q, want claude (legacy agent_tool literal)", got.backend)
	}
}

// TestRundbRecordRunStart_ParentRunID verifies that rundbRecordRunStart
// passes ParentRunID from RunOptions through to RecordRunStart.
func TestRundbRecordRunStart_ParentRunID(t *testing.T) {
	logsRoot := t.TempDir()

	graph := model.NewGraph("test")
	rec := &recordingRunDBWriter{}
	e := &Engine{
		Graph:    graph,
		LogsRoot: logsRoot,
		Options:  RunOptions{RunID: "child-run", ParentRunID: "parent-xyz"},
		RunDB:    rec,
	}

	e.rundbRecordRunStart()

	if rec.capturedParentRunID != "parent-xyz" {
		t.Errorf("capturedParentRunID = %q, want parent-xyz", rec.capturedParentRunID)
	}
}
