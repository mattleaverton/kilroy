package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// fakeRunDBWriter records RecordRunComplete calls for assertion.
// All other RunDBWriter methods are no-ops.
type fakeRunDBWriter struct {
	completed []completedCall
}

type completedCall struct {
	runID, status, failureReason, finalSHA string
}

func (f *fakeRunDBWriter) RecordRunStart(runID, graphName, goal, status, logsRoot, worktreeDir, runBranch, repoPath, dotSource string, inputs map[string]any, labels map[string]string, invocation []string, config map[string]any) error {
	return nil
}

func (f *fakeRunDBWriter) RecordRunComplete(runID, status, failureReason, finalSHA string, warnings []string) error {
	f.completed = append(f.completed, completedCall{runID, status, failureReason, finalSHA})
	return nil
}

func (f *fakeRunDBWriter) RecordNodeStart(runID, nodeID string, attempt int, handlerType string) (int64, error) {
	return 0, nil
}

func (f *fakeRunDBWriter) RecordNodeComplete(id int64, status, failureReason, failureClass, preferredLabel, notes string, contextUpdates map[string]any) error {
	return nil
}

func (f *fakeRunDBWriter) RecordEdgeDecision(runID, fromNode, toNode, edgeLabel, condition, reason string) error {
	return nil
}

func (f *fakeRunDBWriter) RecordProviderSelection(runID, nodeID string, attempt int, provider, model, backend string) error {
	return nil
}

func (f *fakeRunDBWriter) RecordNodeDiff(runID, nodeID string, attempt int, beforeSHA, afterSHA string, filesChanged, insertions, deletions int) error {
	return nil
}

func (f *fakeRunDBWriter) RecordNodeArtifact(nodeExecID int64, name, contentType string, content []byte, truncated bool) error {
	return nil
}

func TestPersistBootstrapFailure_WritesFinalJSON(t *testing.T) {
	logsRoot := t.TempDir()
	persistBootstrapFailure(logsRoot, "run-abc", nil, errors.New("validation failed: terminal_condition_edge"))

	finalPath := filepath.Join(logsRoot, "final.json")
	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("final.json should exist: %v", err)
	}

	var final runtime.FinalOutcome
	if err := json.Unmarshal(data, &final); err != nil {
		t.Fatalf("final.json should be valid JSON FinalOutcome: %v", err)
	}
	if final.Status != runtime.FinalFail {
		t.Errorf("status = %q, want %q", final.Status, runtime.FinalFail)
	}
	if final.RunID != "run-abc" {
		t.Errorf("run_id = %q, want %q", final.RunID, "run-abc")
	}
	if final.FailureReason != "validation_failed" {
		t.Errorf("failure_reason = %q, want %q", final.FailureReason, "validation_failed")
	}
}

func TestPersistBootstrapFailure_AppendsRunFailedEvent(t *testing.T) {
	logsRoot := t.TempDir()
	persistBootstrapFailure(logsRoot, "run-xyz", nil, errors.New("validation failed: missing class"))

	progPath := filepath.Join(logsRoot, "progress.ndjson")
	data, err := os.ReadFile(progPath)
	if err != nil {
		t.Fatalf("progress.ndjson should exist: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		t.Fatal("progress.ndjson is empty")
	}

	var ev map[string]any
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		t.Fatalf("progress event should be valid JSON: %v", err)
	}
	if ev["event"] != "run_failed" {
		t.Errorf("event = %v, want run_failed", ev["event"])
	}
	if ev["status"] != "fail" {
		t.Errorf("status = %v, want fail", ev["status"])
	}
	if ev["reason"] != "validation_failed" {
		t.Errorf("reason = %v, want validation_failed", ev["reason"])
	}
}

func TestPersistBootstrapFailure_RecordsDBComplete(t *testing.T) {
	logsRoot := t.TempDir()
	db := &fakeRunDBWriter{}
	persistBootstrapFailure(logsRoot, "run-dbtest", db, errors.New("validation failed: bad edge"))

	if len(db.completed) != 1 {
		t.Fatalf("expected 1 RecordRunComplete call, got %d", len(db.completed))
	}
	got := db.completed[0]
	if got.runID != "run-dbtest" {
		t.Errorf("runID = %q, want %q", got.runID, "run-dbtest")
	}
	if got.status != "fail" {
		t.Errorf("status = %q, want fail", got.status)
	}
	if got.failureReason != "validation_failed" {
		t.Errorf("failureReason = %q, want validation_failed", got.failureReason)
	}
}

func TestClassifyBootstrapFailureReason(t *testing.T) {
	tests := []struct {
		err  string
		want string
	}{
		{"validation failed: x", "validation_failed"},
		{"preflight aborted: declined", "preflight_failed"},
		{"PreflightFailure: foo", "preflight_failed"},
		{"some other launch problem", "launch_failed"},
	}
	for _, tt := range tests {
		got := classifyBootstrapFailureReason(errors.New(tt.err))
		if got != tt.want {
			t.Errorf("classifyBootstrapFailureReason(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func TestPersistBootstrapFailure_EmptyLogsRoot_NoOp(t *testing.T) {
	// Should not panic and should not call DB.
	db := &fakeRunDBWriter{}
	persistBootstrapFailure("", "run-id", db, errors.New("validation failed"))
	if len(db.completed) != 0 {
		t.Errorf("expected 0 DB calls when logsRoot empty, got %d", len(db.completed))
	}
}
