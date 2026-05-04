// Tests that --orphans --dry-run lists only true orphans (terminal status with
// missing logs_root) rather than every row matching the rest of the filter.
package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

// captureStdout runs fn while os.Stdout is redirected to a pipe and returns
// what fn wrote to stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan struct{})
	var buf bytes.Buffer
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()
	fn()
	_ = w.Close()
	os.Stdout = orig
	<-done
	_ = r.Close()
	return buf.String()
}

// TestRunsPrune_OrphansDryRun_OnlyListsTrueOrphans verifies that
// `kilroy runs prune --orphans --dry-run` lists only rows with terminal status
// whose logs_root directory is missing on disk — matching the semantics of
// the --yes path. Before the fix the dry-run listed every row matching the
// other filters, regardless of disk state.
func TestRunsPrune_OrphansDryRun_OnlyListsTrueOrphans(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db := openTestRunDB(t)

	existingDir := t.TempDir()
	_ = db.InsertRun(rundb.RunRecord{RunID: "term-exists-001", Status: "success", LogsRoot: existingDir, StartedAt: time.Now()})
	_ = db.InsertRun(rundb.RunRecord{RunID: "orphan-fail-001", Status: "fail", LogsRoot: "/definitely/not/a/real/path/logs-A", StartedAt: time.Now()})
	_ = db.InsertRun(rundb.RunRecord{RunID: "orphan-canceled-001", Status: "canceled", LogsRoot: "/definitely/not/a/real/path/logs-B", StartedAt: time.Now()})
	_ = db.InsertRun(rundb.RunRecord{RunID: "running-missing-001", Status: "running", LogsRoot: "/definitely/not/a/real/path/logs-C", StartedAt: time.Now()})

	out := captureStdout(t, func() {
		if !pruneFromDB(time.Time{}, "", "", "", true /* orphansOnly */, true /* dryRun */) {
			t.Fatal("pruneFromDB returned false")
		}
	})

	for _, want := range []string{"orphan-fail-001", "orphan-canceled-001"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing expected orphan %q. Output:\n%s", want, out)
		}
	}
	for _, banned := range []string{"term-exists-001", "running-missing-001"} {
		if strings.Contains(out, banned) {
			t.Errorf("dry-run output contained non-orphan %q. Output:\n%s", banned, out)
		}
	}
	if !strings.Contains(out, "2 run(s) matched") {
		t.Errorf("dry-run summary did not report 2 matched rows. Output:\n%s", out)
	}

	// Sanity: nothing was actually deleted.
	for _, id := range []string{"term-exists-001", "orphan-fail-001", "orphan-canceled-001", "running-missing-001"} {
		if r, _ := db.GetRun(id); r == nil {
			t.Errorf("dry-run deleted %q", id)
		}
	}
}

// TestRunsPrune_OrphansWet_DeletesOnlyOrphans drives pruneFromDB end-to-end
// in --yes mode and confirms only true orphans (terminal + missing logs_root)
// are removed.
func TestRunsPrune_OrphansWet_DeletesOnlyOrphans(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	db := openTestRunDB(t)

	existingDir := t.TempDir()
	_ = db.InsertRun(rundb.RunRecord{RunID: "term-exists-002", Status: "success", LogsRoot: existingDir, StartedAt: time.Now()})
	_ = db.InsertRun(rundb.RunRecord{RunID: "orphan-fail-002", Status: "fail", LogsRoot: "/definitely/not/a/real/path/logs-D", StartedAt: time.Now()})
	_ = db.InsertRun(rundb.RunRecord{RunID: "running-missing-002", Status: "running", LogsRoot: "/definitely/not/a/real/path/logs-E", StartedAt: time.Now()})

	_ = captureStdout(t, func() {
		if !pruneFromDB(time.Time{}, "", "", "", true /* orphansOnly */, false /* dryRun */) {
			t.Fatal("pruneFromDB returned false")
		}
	})

	if r, _ := db.GetRun("term-exists-002"); r == nil {
		t.Error("term-exists-002 was unexpectedly deleted")
	}
	if r, _ := db.GetRun("running-missing-002"); r == nil {
		t.Error("running-missing-002 was unexpectedly deleted (status=running is not terminal)")
	}
	if r, _ := db.GetRun("orphan-fail-002"); r != nil {
		t.Error("orphan-fail-002 should have been deleted")
	}
}
