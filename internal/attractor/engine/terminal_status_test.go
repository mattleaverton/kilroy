// Tests for the terminal_status="fail" attribute and shape=Mcircle alias.
// Reaching such a terminal records FinalFail at the run level.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestTerminalStatus_FailAttribute_RecordsFinalFail(t *testing.T) {
	requireIntegration(t)
	repo := initTestRepo(t)
	dot := []byte(`
digraph G {
  start  [shape=Mdiamond]
  pivot  [shape=parallelogram, tool_command="echo no"]
  done   [shape=Msquare]
  failed [shape=Msquare, terminal_status="fail", failure_reason="manual"]
  start -> pivot
  pivot -> done   [condition="outcome=success"]
  pivot -> failed [condition="outcome!=success"]
}
`)
	logsRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo, LogsRoot: logsRoot})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	// pivot succeeds (echo exits 0); routes to done → FinalSuccess.
	if res.FinalStatus != runtime.FinalSuccess {
		t.Fatalf("baseline: got %q want %q", res.FinalStatus, runtime.FinalSuccess)
	}
	final, err := os.ReadFile(filepath.Join(logsRoot, "final.json"))
	if err != nil {
		t.Fatalf("read final.json: %v", err)
	}
	if !strings.Contains(string(final), `"status": "success"`) {
		t.Fatalf("final.json: expected success, got: %s", final)
	}
}

func TestTerminalStatus_FailAttribute_OnFailureRecordsFinalFail(t *testing.T) {
	requireIntegration(t)
	repo := initTestRepo(t)
	dot := []byte(`
digraph G {
  start  [shape=Mdiamond]
  pivot  [shape=parallelogram, max_retries="0", tool_command="exit 1"]
  done   [shape=Msquare]
  failed [shape=Msquare, terminal_status="fail", failure_reason="agent_did_not_write_result"]
  start -> pivot
  pivot -> done   [condition="outcome=success"]
  pivot -> failed [condition="outcome!=success"]
}
`)
	logsRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo, LogsRoot: logsRoot})
	if err != nil {
		t.Fatalf("Run() error (failed terminal is reached cleanly, no error): %v", err)
	}
	if res.FinalStatus != runtime.FinalFail {
		t.Fatalf("FinalStatus: got %q want %q", res.FinalStatus, runtime.FinalFail)
	}
	final, err := os.ReadFile(filepath.Join(logsRoot, "final.json"))
	if err != nil {
		t.Fatalf("read final.json: %v", err)
	}
	if !strings.Contains(string(final), `"status": "fail"`) {
		t.Fatalf("final.json: expected fail, got: %s", final)
	}
	if !strings.Contains(string(final), `agent_did_not_write_result`) {
		t.Fatalf("final.json: expected failure_reason carried from terminal, got: %s", final)
	}
}

func TestTerminalStatus_McircleShape_RecordsFinalFail(t *testing.T) {
	requireIntegration(t)
	repo := initTestRepo(t)
	dot := []byte(`
digraph G {
  start  [shape=Mdiamond]
  pivot  [shape=parallelogram, max_retries="0", tool_command="exit 1"]
  done   [shape=Msquare]
  failed [shape=Mcircle]
  start -> pivot
  pivot -> done   [condition="outcome=success"]
  pivot -> failed [condition="outcome!=success"]
}
`)
	logsRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo, LogsRoot: logsRoot})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if res.FinalStatus != runtime.FinalFail {
		t.Fatalf("FinalStatus: got %q want %q", res.FinalStatus, runtime.FinalFail)
	}
}
