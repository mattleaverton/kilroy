package main

// Tests for `attractor runs wait` exit-code contract.
//
// Contract (documented in runsUsage() and the skill doc):
//   0 — run reached status "success"
//   1 — run reached a non-success terminal status (fail, canceled, error, …)
//       OR any invocation error (bad args, unknown run ID, DB error)
//   2 — timeout elapsed while the run was still in a non-terminal state
//
// These tests verify the timeout path (exit 2) for both the explicit-id form
// and the --latest --label form, because the dogfood observation at §13.4
// specifically called out the --latest --label interaction as suspect.
//
// Audit finding: the code is correct — os.Exit(2) is called unconditionally
// at line 1017 when the deadline fires. The observation of "exit 0 after
// timeout" was a harness artifact (the exit code was not captured from the
// runs-wait subprocess directly).

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

// insertStuckRun writes a run with status="running" that will never advance.
// Uses the redirected DB path (XDG_STATE_HOME must already be set via t.Setenv).
func insertStuckRun(t *testing.T, runID string, labels map[string]string) {
	t.Helper()
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	defer db.Close()
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "timeout-contract-test",
		Status:    "running",
		Labels:    labels,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
}

// runKilroyWait invokes the kilroy binary with the given args, inheriting the
// current process env (which includes any t.Setenv overrides), and returns
// the exit code and combined output.  The test-level context deadline is set
// to 30 s to prevent the test from blocking forever if the code regresses.
func runKilroyWait(t *testing.T, bin string, stateDir string, args ...string) (exitCode int, output string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	// Pass through the whole env but ensure the redirected state dir wins.
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateDir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("test outer deadline exceeded — runs wait did not return within 30s\n%s", out)
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

// TestRunsWait_TimeoutExitsWithCode2_ExplicitID is the primary regression lock
// for the §13.4 audit.  A run stuck in "running" must cause `runs wait` to
// exit 2 (not 0) once the --timeout elapses.
func TestRunsWait_TimeoutExitsWithCode2_ExplicitID(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "wait-timeout-explicit-001"
	insertStuckRun(t, runID, nil)

	code, out := runKilroyWait(t, bin, stateDir,
		"runs", "wait", runID,
		"--timeout", "600ms", "--interval", "100ms",
	)

	if code != 2 {
		t.Errorf("exit code = %d, want 2 (timeout); output:\n%s", code, out)
	}
	if !strings.Contains(out, "timeout waiting") {
		t.Errorf("expected 'timeout waiting' in stderr; output:\n%s", out)
	}
}

// TestRunsWait_TimeoutExitsWithCode2_LatestLabel tests the --latest --label
// path specifically, which was the exact invocation cited in the §13.4
// dogfood observation.
func TestRunsWait_TimeoutExitsWithCode2_LatestLabel(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "wait-timeout-label-001"
	insertStuckRun(t, runID, map[string]string{"task": "timeout-contract"})

	code, out := runKilroyWait(t, bin, stateDir,
		"runs", "wait",
		"--latest", "--label", "task=timeout-contract",
		"--timeout", "600ms", "--interval", "100ms",
	)

	if code != 2 {
		t.Errorf("exit code = %d, want 2 (timeout); output:\n%s", code, out)
	}
	if !strings.Contains(out, "timeout waiting") {
		t.Errorf("expected 'timeout waiting' in stderr; output:\n%s", out)
	}
}

// TestRunsWait_SuccessExitsZero confirms the success branch (exit 0) for a
// run already in terminal state "success" when wait is called.
func TestRunsWait_SuccessExitsZero(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	// Insert a run that is already success.
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	const runID = "wait-success-001"
	if err := db.InsertRun(rundb.RunRecord{
		RunID: runID, GraphName: "contract-test", Status: "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.CompleteRun(runID, "success", "", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyWait(t, bin, stateDir,
		"runs", "wait", runID,
	)
	if code != 0 {
		t.Errorf("exit code = %d, want 0 (success); output:\n%s", code, out)
	}
}

// TestRunsWait_FailExitsOne confirms the fail branch (exit 1) for a run
// already in terminal state "fail".
func TestRunsWait_FailExitsOne(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	const runID = "wait-fail-001"
	if err := db.InsertRun(rundb.RunRecord{
		RunID: runID, GraphName: "contract-test", Status: "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.CompleteRun(runID, "fail", "tool-exited-nonzero", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyWait(t, bin, stateDir,
		"runs", "wait", runID,
	)
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (fail); output:\n%s", code, out)
	}
}
