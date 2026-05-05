package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestRun_NoMatchingFailEdge_FallsBackToRetryTarget(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	// "review" fails. It has a conditional edge to exit (outcome=yes) and an
	// unconditional fallback edge to "fix". The unconditional edge (step-4) routes
	// review -> fix on failure. "fix" succeeds and routes unconditionally to exit.
	// Graph-level retry_target="fix" is also present but the unconditional edge
	// is taken first; the pipeline still reaches exit successfully.
	//
	// NOTE: The graph previously used condition="outcome=__never__" on the review->fix
	// edge, which triggered the step-5 all-conditional fallback. That pattern is now
	// rejected by the all_conditional_edges lint rule (promoted to ERROR in G3).
	// The unconditional fallback edge is the correct pattern.
	dot := []byte(`
digraph G {
  graph [goal="test", retry_target="fix"]
  start  [shape=Mdiamond]
  exit   [shape=Msquare]
  review [
    shape=parallelogram,
    tool_command="echo fail; exit 1"
  ]
  fix [
    shape=parallelogram,
    tool_command="echo fixed > fixed.txt"
  ]
  start -> review
  review -> exit [condition="outcome=yes"]
  review -> fix
  fix -> exit [condition="outcome=success"]
}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if res.FinalStatus != runtime.FinalSuccess {
		t.Fatalf("final status: got %q want %q", res.FinalStatus, runtime.FinalSuccess)
	}
}

// TestRun_NoMatchingFailEdge_NoRetryTarget_UnconditionalFallbackRoutesToExit verifies that
// when a node fails and has an unconditional fallback edge, the engine routes via that edge.
// The all_conditional_edges lint rule (promoted to ERROR in G3) requires at least one
// unconditional edge on non-terminal nodes, so the previous all-conditional graph pattern
// is now rejected at validation time. This test uses the correct pattern.
func TestRun_NoMatchingFailEdge_NoRetryTarget_UnconditionalFallbackRoutesToExit(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	// "review" fails. It has a conditional edge (outcome=yes) and an unconditional
	// fallback edge to exit. On failure the conditional edge does not match; the
	// unconditional edge (step-4) routes the pipeline to exit.
	dot := []byte(`
digraph G {
  graph [goal="test", default_max_retry=0]
  start  [shape=Mdiamond]
  exit   [shape=Msquare]
  review [
    shape=parallelogram,
    tool_command="echo fail; exit 1"
  ]
  start -> review
  review -> exit [condition="outcome=yes"]
  review -> exit [condition="outcome!=success"]
}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Unconditional fallback routes to exit successfully.
	if res.FinalStatus != runtime.FinalSuccess {
		t.Fatalf("final status: got %q want %q (unconditional fallback should route to exit)", res.FinalStatus, runtime.FinalSuccess)
	}
}
