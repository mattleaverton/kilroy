// Contract tests for the implement and fix workflows: their "retry once"
// edge label must match real engine behavior — at most one retry of the
// agent body, after which the run terminates failed.

package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestImplementWorkflow_RetryOnce_FailsAfterTwoAttempts asserts that the
// implement workflow's verify→agent edge ("retry once") is enforced by a
// real visit cap, not just by the label. The workflow's graph.dot must
// declare max_node_visits, and the engine must abort after the agent has
// been executed twice (original + one retry) when verify keeps failing.
func TestImplementWorkflow_RetryOnce_FailsAfterTwoAttempts(t *testing.T) {
	requireIntegration(t)
	assertWorkflowDeclaresVisitCap(t, "implement")

	dot := []byte(`
digraph implement {
  graph [max_node_visits="3"]
  start         [shape=Mdiamond]
  stage_context [shape=parallelogram, max_retries="0", tool_command="exit 0"]
  agent         [shape=parallelogram, max_retries="0", tool_command="echo x >> agent_calls.txt"]
  verify        [shape=parallelogram, max_retries="0", tool_command="echo verify_fail >&2; exit 1"]
  summary       [shape=parallelogram, max_retries="0", tool_command="exit 0"]
  done          [shape=Msquare]
  failed        [shape=Msquare, terminal_status="fail"]
  start         -> stage_context
  stage_context -> agent          [condition="outcome=success"]
  stage_context -> summary        [condition="outcome!=success"]
  agent         -> verify         [condition="outcome=success"]
  agent         -> summary        [condition="outcome!=success"]
  verify        -> summary        [condition="outcome=success"]
  verify        -> agent          [condition="outcome!=success"]
  summary       -> done           [condition="outcome=success"]
  summary       -> failed         [condition="outcome!=success"]
}
`)

	runRetryOnceFixture(t, "impl-retry-once", dot)
}

// TestFixWorkflow_RetryOnce_FailsAfterTwoAttempts is the same shape as the
// implement variant, mirroring the fix workflow topology (with the extra
// `diff` capture step on the success path).
func TestFixWorkflow_RetryOnce_FailsAfterTwoAttempts(t *testing.T) {
	requireIntegration(t)
	assertWorkflowDeclaresVisitCap(t, "fix")

	dot := []byte(`
digraph fix {
  graph [max_node_visits="3"]
  start         [shape=Mdiamond]
  stage_context [shape=parallelogram, max_retries="0", tool_command="exit 0"]
  agent         [shape=parallelogram, max_retries="0", tool_command="echo x >> agent_calls.txt"]
  verify        [shape=parallelogram, max_retries="0", tool_command="echo verify_fail >&2; exit 1"]
  diff          [shape=parallelogram, max_retries="0", tool_command="exit 0"]
  summary       [shape=parallelogram, max_retries="0", tool_command="exit 0"]
  done          [shape=Msquare]
  failed        [shape=Msquare, terminal_status="fail"]
  start         -> stage_context
  stage_context -> agent          [condition="outcome=success"]
  stage_context -> summary        [condition="outcome!=success"]
  agent         -> verify         [condition="outcome=success"]
  agent         -> summary        [condition="outcome!=success"]
  verify        -> diff           [condition="outcome=success"]
  verify        -> agent          [condition="outcome!=success"]
  diff          -> summary
  summary       -> done           [condition="outcome=success"]
  summary       -> failed         [condition="outcome!=success"]
}
`)

	runRetryOnceFixture(t, "fix-retry-once", dot)
}

// assertWorkflowDeclaresVisitCap is the regression guard that ties this
// test to the real workflow file. If someone removes max_node_visits from
// the workflow's graph.dot, the contract is broken even if the engine
// semantic test below would still pass against the inlined fixture.
func assertWorkflowDeclaresVisitCap(t *testing.T, workflow string) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "workflows", workflow, "graph.dot")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(raw), `max_node_visits="3"`) {
		t.Fatalf(`workflows/%s/graph.dot must declare max_node_visits="3" to enforce the "retry once" contract`, workflow)
	}
}

// runRetryOnceFixture executes a workflow-shaped fixture with verify wired
// to always fail and an agent that simply tallies its invocations. It
// asserts the engine halts via the stuck-cycle visit cap after exactly two
// agent executions.
func runRetryOnceFixture(t *testing.T, runID string, dot []byte) {
	t.Helper()
	repo := initTestRepo(t)
	logsRoot := t.TempDir()
	worktreeDir := filepath.Join(logsRoot, "worktree")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := runForTest(t, ctx, dot, RunOptions{
		RepoPath:    repo,
		RunID:       runID,
		LogsRoot:    logsRoot,
		WorktreeDir: worktreeDir,
	})
	if err == nil {
		t.Fatalf("expected run to fail after the retry cap was hit, got success")
	}
	if !strings.Contains(err.Error(), "stuck in a cycle") {
		t.Fatalf("expected stuck-cycle error, got: %v", err)
	}

	counterPath := filepath.Join(worktreeDir, "agent_calls.txt")
	counterBytes, readErr := os.ReadFile(counterPath)
	if readErr != nil {
		t.Fatalf("read agent counter %s: %v", counterPath, readErr)
	}
	lines := splitNonEmpty(string(counterBytes))
	if len(lines) != 2 {
		t.Fatalf("agent ran %d time(s), want exactly 2 (original + 1 retry); contents=%q", len(lines), string(counterBytes))
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
