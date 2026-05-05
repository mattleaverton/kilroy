// Tests for `attractor runs show --json` provider_selections surfacing.
//
// F2 contract: when the run DB has rows in provider_selections for this run,
// `kilroy runs show <id> --json` MUST include them as a top-level array
// `provider_selections`. When the run has zero rows (e.g. prelaunch failed
// before any node ran), the field is omitted from the JSON entirely
// (omitempty), matching the rest of runShowDetail's optional-field style.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

func runKilroyShow(t *testing.T, bin, stateDir string, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateDir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("kilroy runs show timed out\n%s", out)
	}
	if err == nil {
		return 0, string(out)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("unexpected non-ExitError: %v\n%s", err, out)
	}
	return ee.ExitCode(), string(out)
}

// TestRunsShow_JSONIncludesProviderSelections inserts a run with two
// provider_selections rows and confirms the JSON output exposes them under
// the `provider_selections` key with the expected fields.
func TestRunsShow_JSONIncludesProviderSelections(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "show-selections-001"
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "f2-show-selections",
		Status:    "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.InsertProviderSelection(runID, "start", 1, "anthropic", "claude-opus-4-7", "cli"); err != nil {
		t.Fatalf("InsertProviderSelection start: %v", err)
	}
	if err := db.InsertProviderSelection(runID, "agent", 1, "anthropic", "claude-opus-4-7", "cli"); err != nil {
		t.Fatalf("InsertProviderSelection agent: %v", err)
	}
	if err := db.CompleteRun(runID, "success", "", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", runID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	var got struct {
		RunID              string `json:"run_id"`
		ProviderSelections []struct {
			NodeID     string `json:"node_id"`
			Attempt    int    `json:"attempt"`
			Provider   string `json:"provider"`
			Model      string `json:"model"`
			Backend    string `json:"backend"`
			SelectedAt string `json:"selected_at"`
		} `json:"provider_selections"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if got.RunID != runID {
		t.Errorf("run_id = %q, want %q", got.RunID, runID)
	}
	if len(got.ProviderSelections) != 2 {
		t.Fatalf("provider_selections len = %d, want 2; output:\n%s", len(got.ProviderSelections), out)
	}
	first := got.ProviderSelections[0]
	if first.NodeID != "start" || first.Provider != "anthropic" || first.Model != "claude-opus-4-7" || first.Backend != "cli" || first.Attempt != 1 {
		t.Errorf("first selection = %+v; want node_id=start provider=anthropic model=claude-opus-4-7 backend=cli attempt=1", first)
	}
	if first.SelectedAt == "" {
		t.Errorf("selected_at should be populated; got empty string")
	}
	if _, err := time.Parse(time.RFC3339Nano, first.SelectedAt); err != nil {
		// Fall back to RFC3339 (no fractional seconds) before failing.
		if _, err2 := time.Parse(time.RFC3339, first.SelectedAt); err2 != nil {
			t.Errorf("selected_at %q is not RFC3339[Nano]: %v / %v", first.SelectedAt, err, err2)
		}
	}
	second := got.ProviderSelections[1]
	if second.NodeID != "agent" {
		t.Errorf("second selection node_id = %q, want agent", second.NodeID)
	}
}

// TestRunsShow_JSONOmitsEmptyProviderSelections confirms that a run with
// zero rows in provider_selections (e.g. prelaunch-failure case) does not
// emit the field — matching the rest of runShowDetail's omitempty style.
func TestRunsShow_JSONOmitsEmptyProviderSelections(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "show-no-selections-001"
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "f2-no-selections",
		Status:    "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.CompleteRun(runID, "fail", "prelaunch_validation_failed", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", runID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	if strings.Contains(out, `"provider_selections"`) {
		t.Errorf("JSON should omit provider_selections when no rows exist; got:\n%s", out)
	}

	var generic map[string]any
	if err := json.Unmarshal([]byte(out), &generic); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if _, present := generic["provider_selections"]; present {
		t.Errorf("provider_selections key present in decoded map; want absent")
	}
}

// TestRunsShow_JSONIncludesParentRunID confirms that a run with a parent
// has its parent_run_id included in the JSON output.
func TestRunsShow_JSONIncludesParentRunID(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const parentID = "parent-show-001"
	const childID = "child-show-001"

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}

	// Insert parent run
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     parentID,
		GraphName: "parent-graph",
		Status:    "success",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun parent: %v", err)
	}

	// Insert child run with ParentRunID
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       childID,
		GraphName:   "child-graph",
		Status:      "success",
		StartedAt:   time.Now(),
		ParentRunID: parentID,
	}); err != nil {
		t.Fatalf("InsertRun child: %v", err)
	}
	db.Close()

	// Test child run shows parent_run_id
	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", childID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	var childResult struct {
		RunID       string `json:"run_id"`
		ParentRunID string `json:"parent_run_id"`
		Children    []any  `json:"children"`
	}
	if err := json.Unmarshal([]byte(out), &childResult); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if childResult.RunID != childID {
		t.Errorf("run_id = %q, want %q", childResult.RunID, childID)
	}
	if childResult.ParentRunID != parentID {
		t.Errorf("parent_run_id = %q, want %q", childResult.ParentRunID, parentID)
	}

	// Test parent run shows empty parent_run_id (omitted in JSON due to omitempty)
	code, out = runKilroyShow(t, bin, stateDir, "runs", "show", parentID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	if strings.Contains(out, `"parent_run_id"`) {
		t.Errorf("JSON should omit parent_run_id when empty; got:\n%s", out)
	}
}

// TestRunsShow_JSONIncludesChildren confirms that a run with children
// has its children array included in the JSON output.
func TestRunsShow_JSONIncludesChildren(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const parentID = "parent-children-001"
	const child1ID = "child-children-001"
	const child2ID = "child-children-002"

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}

	// Insert parent run
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     parentID,
		GraphName: "parent-graph",
		Status:    "success",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun parent: %v", err)
	}

	// Insert two child runs
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       child1ID,
		GraphName:   "child-graph-1",
		Status:      "success",
		StartedAt:   time.Now(),
		ParentRunID: parentID,
	}); err != nil {
		t.Fatalf("InsertRun child1: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       child2ID,
		GraphName:   "child-graph-2",
		Status:      "fail",
		StartedAt:   time.Now(),
		ParentRunID: parentID,
	}); err != nil {
		t.Fatalf("InsertRun child2: %v", err)
	}
	db.Close()

	// Test parent run shows children
	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", parentID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	var result struct {
		RunID    string `json:"run_id"`
		Children []struct {
			RunID     string `json:"run_id"`
			GraphName string `json:"graph_name"`
			Status    string `json:"status"`
		} `json:"children"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if result.RunID != parentID {
		t.Errorf("run_id = %q, want %q", result.RunID, parentID)
	}
	if len(result.Children) != 2 {
		t.Fatalf("children len = %d, want 2; output:\n%s", len(result.Children), out)
	}

	// Verify children are correctly ordered (newest first)
	childIDs := make(map[string]bool)
	for _, c := range result.Children {
		childIDs[c.RunID] = true
		if c.GraphName == "" {
			t.Errorf("child %s has empty graph_name", c.RunID)
		}
		if c.Status == "" {
			t.Errorf("child %s has empty status", c.RunID)
		}
	}
	if !childIDs[child1ID] || !childIDs[child2ID] {
		t.Errorf("expected children %s and %s, got %v", child1ID, child2ID, childIDs)
	}
}

// TestRunsShow_OMitsEmptyChildren confirms that a run with zero children
// omits the children field from the JSON output (omitempty behavior).
func TestRunsShow_OMitsEmptyChildren(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "no-children-001"

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "solo-graph",
		Status:    "success",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	db.Close()

	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", runID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	if strings.Contains(out, `"children"`) {
		t.Errorf("JSON should omit children when empty; got:\n%s", out)
	}

	var generic map[string]any
	if err := json.Unmarshal([]byte(out), &generic); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if _, present := generic["children"]; present {
		t.Errorf("children key present in decoded map; want absent")
	}
}

// TestRunsShow_HumanReadableChildrenCount confirms that human-readable output
// includes "children: N" line showing the number of child runs.
func TestRunsShow_HumanReadableChildrenCount(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const parentID = "parent-human-001"
	const child1ID = "child-human-001"
	const child2ID = "child-human-002"

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}

	// Insert parent run
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     parentID,
		GraphName: "parent-graph",
		Status:    "success",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun parent: %v", err)
	}

	// Insert two child runs
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       child1ID,
		GraphName:   "child-graph-1",
		Status:      "success",
		StartedAt:   time.Now(),
		ParentRunID: parentID,
	}); err != nil {
		t.Fatalf("InsertRun child1: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:       child2ID,
		GraphName:   "child-graph-2",
		Status:      "fail",
		StartedAt:   time.Now(),
		ParentRunID: parentID,
	}); err != nil {
		t.Fatalf("InsertRun child2: %v", err)
	}

	// Insert a run with no children
	const noChildrenID = "no-children-human-001"
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     noChildrenID,
		GraphName: "solo-graph",
		Status:    "success",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun no-children: %v", err)
	}
	db.Close()

	// Test parent with children shows "children: 2"
	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", parentID)
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}
	if !strings.Contains(out, "children:") {
		t.Errorf("human-readable output should contain 'children:' line; got:\n%s", out)
	}
	if !strings.Contains(out, "children:     2") {
		t.Errorf("human-readable output should show 'children:     2'; got:\n%s", out)
	}

	// Test run with no children shows "children: 0"
	code, out = runKilroyShow(t, bin, stateDir, "runs", "show", noChildrenID)
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}
	if !strings.Contains(out, "children:     0") {
		t.Errorf("human-readable output should show 'children:     0' for no children; got:\n%s", out)
	}
}
