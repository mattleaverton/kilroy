package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFanInDeterministic_RetryTargetFallbackIsBlocked verifies that the
// engine's retry_target fallback (engine.go:645) does NOT fire for deterministic
// fan-in failures, matching the safety guard in resolveNextHop (next_hop.go:48).
//
// Regression: the fallback previously bypassed the fan-in deterministic guard
// because it checked resolveRetryTarget directly after resolveNextHop returned nil,
// without re-checking whether the nil was an intentional fan-in block.
func TestFanInDeterministic_RetryTargetFallbackIsBlocked(t *testing.T) {
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph G {
  graph [goal="probe", retry_target="impl_setup", default_max_retry=0]
  start [shape=Mdiamond]
  impl_setup [shape=parallelogram, tool_command="echo setup-ok"]
  par [shape=component]
  a [shape=parallelogram, tool_command="echo fail-a >&2; exit 1"]
  b [shape=parallelogram, tool_command="echo fail-b >&2; exit 1"]
  join [shape=tripleoctagon]
  exit [shape=Msquare]

  start -> impl_setup
  impl_setup -> par
  par -> a
  par -> b
  a -> join
  b -> join
  join -> exit [condition="outcome=success"]
}
`)

	logsRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := runForTest(t, ctx, dot, RunOptions{
		RepoPath: repo,
		RunID:    "fanin-fallback-blocked",
		LogsRoot: logsRoot,
	})
	if err == nil {
		t.Fatal("expected terminal failure for deterministic fan-in, got nil")
	}

	progressPath := filepath.Join(logsRoot, "progress.ndjson")
	events := mustReadProgressEventsFile(t, progressPath)

	// The retry_target fallback must NOT fire for deterministic fan-in failures.
	for _, ev := range events {
		event := anyToString(ev["event"])
		nodeID := anyToString(ev["node_id"])
		if event == "no_matching_fail_edge_fallback" && nodeID == "join" {
			t.Fatalf("retry_target fallback bypassed fan-in deterministic guard: retry_target=%s", anyToString(ev["retry_target"]))
		}
	}
}
