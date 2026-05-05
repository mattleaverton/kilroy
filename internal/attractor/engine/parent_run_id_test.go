// Tests for parent_run_id round-tripping through manifest.json so a child
// run preserves its lineage to the dispatching parent across resume.
package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
)

func TestEngine_WriteManifest_IncludesParentRunID(t *testing.T) {
	logsRoot := t.TempDir()
	graph := model.NewGraph("test-graph")
	graph.Attrs["goal"] = "verify parent_run_id"

	e := &Engine{
		Graph:       graph,
		LogsRoot:    logsRoot,
		WorktreeDir: filepath.Join(logsRoot, "worktree"),
		RunBranch:   "attractor/run/child-run",
		Options: RunOptions{
			RunID:       "child-run",
			ParentRunID: "parent-xyz",
			RepoPath:    t.TempDir(),
		},
	}

	if err := e.writeManifest(""); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(logsRoot, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest.json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal manifest.json: %v", err)
	}
	got, _ := m["parent_run_id"].(string)
	if got != "parent-xyz" {
		t.Fatalf("manifest parent_run_id = %q, want %q", got, "parent-xyz")
	}
}

func TestEngine_WriteManifest_OmitsEmptyParentRunID(t *testing.T) {
	logsRoot := t.TempDir()
	e := &Engine{
		Graph:       model.NewGraph("test"),
		LogsRoot:    logsRoot,
		WorktreeDir: filepath.Join(logsRoot, "worktree"),
		Options:     RunOptions{RunID: "solo-run"},
	}
	if err := e.writeManifest(""); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(logsRoot, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest.json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal manifest.json: %v", err)
	}
	if v, ok := m["parent_run_id"]; ok {
		t.Fatalf("expected no parent_run_id key in manifest for top-level run, got %v", v)
	}
}

// TestResume_RestoresParentRunID writes a manifest carrying parent_run_id
// and asserts that loadManifest — the deserialization step Resume uses —
// recovers the field so resumeFromLogsRoot can wire it onto RunOptions and
// preserve the child run's lineage.
func TestResume_RestoresParentRunID(t *testing.T) {
	logsRoot := t.TempDir()
	repo := t.TempDir()
	written := manifest{
		RunID:       "child-run",
		RepoPath:    repo,
		RunBranch:   "attractor/run/child-run",
		ParentRunID: "parent-xyz",
	}
	if err := writeJSON(filepath.Join(logsRoot, "manifest.json"), written); err != nil {
		t.Fatalf("write manifest.json: %v", err)
	}

	loaded, err := loadManifest(filepath.Join(logsRoot, "manifest.json"))
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if loaded.ParentRunID != "parent-xyz" {
		t.Fatalf("loaded ParentRunID = %q, want %q", loaded.ParentRunID, "parent-xyz")
	}

	opts := RunOptions{ParentRunID: strings.TrimSpace(loaded.ParentRunID)}
	if opts.ParentRunID != "parent-xyz" {
		t.Fatalf("RunOptions.ParentRunID = %q, want %q", opts.ParentRunID, "parent-xyz")
	}
}
