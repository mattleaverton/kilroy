// Tests for `kilroy runs prune` default-safe behavior around in-flight runs.
// Verifies that status='running' rows are excluded by default and only deleted with --include-running.
package main

import (
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

func TestRunsPrune_DefaultDoesNotDeleteRunning(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db := openTestRunDB(t)

	startedAt := time.Now().Add(-48 * time.Hour)
	if err := db.InsertRun(rundb.RunRecord{RunID: "running-1", GraphName: "g", Status: "running", StartedAt: startedAt}); err != nil {
		t.Fatalf("InsertRun running: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{RunID: "fail-1", GraphName: "g", Status: "fail", StartedAt: startedAt}); err != nil {
		t.Fatalf("InsertRun fail: %v", err)
	}

	// Cutoff 24h ago; both rows match the --before filter.
	cutoff := time.Now().Add(-24 * time.Hour).Format("2006-01-02T15:04")
	attractorRunsPrune([]string{"--before", cutoff, "--yes"})

	db2, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("reopen rundb: %v", err)
	}
	defer db2.Close()

	running, _ := db2.GetRun("running-1")
	if running == nil {
		t.Fatal("running run was deleted by default — must require --include-running")
	}
	if running.Status != "running" {
		t.Errorf("status = %q, want \"running\"", running.Status)
	}
	if r, _ := db2.GetRun("fail-1"); r != nil {
		t.Error("terminal (fail) run was not deleted")
	}
}

func TestRunsPrune_IncludeRunning_Deletes(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db := openTestRunDB(t)

	startedAt := time.Now().Add(-48 * time.Hour)
	if err := db.InsertRun(rundb.RunRecord{RunID: "running-2", GraphName: "g", Status: "running", StartedAt: startedAt}); err != nil {
		t.Fatalf("InsertRun running: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{RunID: "fail-2", GraphName: "g", Status: "fail", StartedAt: startedAt}); err != nil {
		t.Fatalf("InsertRun fail: %v", err)
	}

	cutoff := time.Now().Add(-24 * time.Hour).Format("2006-01-02T15:04")
	attractorRunsPrune([]string{"--before", cutoff, "--include-running", "--yes"})

	db2, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("reopen rundb: %v", err)
	}
	defer db2.Close()

	if r, _ := db2.GetRun("running-2"); r != nil {
		t.Error("running run was not deleted with --include-running")
	}
	if r, _ := db2.GetRun("fail-2"); r != nil {
		t.Error("terminal (fail) run was not deleted")
	}
}
