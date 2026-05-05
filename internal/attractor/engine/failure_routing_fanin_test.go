package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

type subgraphFailureFixtureHandler struct{}

func (h *subgraphFailureFixtureHandler) Execute(ctx context.Context, exec *Execution, node *model.Node) (runtime.Outcome, error) {
	_ = ctx
	_ = exec
	_ = node
	return runtime.Outcome{
		Status:        runtime.StatusFail,
		FailureReason: "provider timeout",
		ContextUpdates: map[string]any{
			"failure_class": failureClassTransientInfra,
		},
	}, nil
}

func TestFailureRouting_FanInAllFail_DoesNotFollowUnconditionalEdge(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph G {
  graph [goal="fanin-fail-routing", default_max_retry=2]
  start [shape=Mdiamond]
  par [shape=component]
  a [shape=parallelogram, tool_command="echo fail-a >&2; exit 1"]
  b [shape=parallelogram, tool_command="echo fail-b >&2; exit 1"]
  c [shape=parallelogram, tool_command="echo fail-c >&2; exit 1"]
  join [shape=tripleoctagon, max_retries=2]
  verify [shape=parallelogram, tool_command="echo verify > verify.txt"]
  exit [shape=Msquare]

  start -> par
  par -> a
  par -> b
  par -> c
  a -> join
  b -> join
  c -> join
  join -> verify
  verify -> exit [condition="outcome=success"]
}
`)

	logsRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{
		RepoPath: repo,
		RunID:    "fanin-all-fail-no-fallthrough",
		LogsRoot: logsRoot,
	})
	if err == nil {
		t.Fatalf("expected terminal failure at join with no fail path; got success result=%+v", res)
	}

	joinStatusBytes, readErr := os.ReadFile(filepath.Join(logsRoot, "join", "status.json"))
	if readErr != nil {
		t.Fatalf("read join/status.json: %v", readErr)
	}
	joinOut, decodeErr := runtime.DecodeOutcomeJSON(joinStatusBytes)
	if decodeErr != nil {
		t.Fatalf("decode join/status.json: %v", decodeErr)
	}
	if joinOut.Status != runtime.StatusFail {
		t.Fatalf("join status: got %q want %q", joinOut.Status, runtime.StatusFail)
	}
	if !strings.Contains(strings.ToLower(joinOut.FailureReason), "all parallel branches failed") {
		t.Fatalf("join failure_reason: got %q, want phrase %q", joinOut.FailureReason, "all parallel branches failed")
	}

	if _, statErr := os.Stat(filepath.Join(logsRoot, "verify", "status.json")); !os.IsNotExist(statErr) {
		t.Fatalf("verify node should not execute after fan-in all-fail; stat err=%v", statErr)
	}
	progressPath := filepath.Join(logsRoot, "progress.ndjson")
	if fanInEdgeWasSelected(t, progressPath, "join", "verify") {
		t.Fatalf("unexpected edge_selected join->verify after fan-in all-fail")
	}
	if got := countProgressEventsForNode(t, progressPath, "stage_attempt_start", "join"); got != 1 {
		t.Fatalf("join attempt count: got %d want %d", got, 1)
	}
	if hasProgressEventForNode(t, progressPath, "stage_retry_sleep", "join") {
		t.Fatalf("unexpected stage_retry_sleep for deterministic join failure")
	}
	blocked, blockedClass := findRetryBlockedClassForNode(t, progressPath, "join")
	if !blocked {
		t.Fatalf("expected stage_retry_blocked for join deterministic failure")
	}
	if blockedClass != failureClassDeterministic {
		t.Fatalf("stage_retry_blocked failure_class: got %q want %q", blockedClass, failureClassDeterministic)
	}

	final := mustReadFinalOutcome(t, filepath.Join(logsRoot, "final.json"))
	if final.Status != runtime.FinalFail {
		t.Fatalf("final status: got %q want %q", final.Status, runtime.FinalFail)
	}
	if strings.TrimSpace(final.FailureReason) == "" {
		t.Fatalf("expected non-empty final failure_reason")
	}
}

func TestFailureRouting_FanInAllFail_DeterministicBlocksRetryTarget(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph G {
  graph [goal="fanin-deterministic-retry-target", retry_target="impl_setup", default_max_retry=0]
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
	res, err := runForTest(t, ctx, dot, RunOptions{
		RepoPath: repo,
		RunID:    "fanin-deterministic-blocks-retry",
		LogsRoot: logsRoot,
	})
	if err == nil {
		t.Fatalf("expected terminal failure; got success result=%+v", res)
	}

	// Verify the run did NOT follow retry_target back to impl_setup after fan-in failure.
	// Check both edge_selected and no_matching_fail_edge_fallback events — the latter
	// is what the retry_target fallback emits (distinct from the edge_selected path).
	progressPath := filepath.Join(logsRoot, "progress.ndjson")
	if fanInEdgeWasSelected(t, progressPath, "join", "impl_setup") {
		t.Fatalf("engine followed retry_target from join to impl_setup despite deterministic failure")
	}
	for _, ev := range mustReadProgressEventsFile(t, progressPath) {
		if anyToString(ev["event"]) == "no_matching_fail_edge_fallback" && anyToString(ev["node_id"]) == "join" {
			t.Fatalf("retry_target fallback bypassed fan-in deterministic guard: retry_target=%s", anyToString(ev["retry_target"]))
		}
	}

	// Verify final.json was written with failure status.
	final := mustReadFinalOutcome(t, filepath.Join(logsRoot, "final.json"))
	if final.Status != runtime.FinalFail {
		t.Fatalf("final status: got %q want %q", final.Status, runtime.FinalFail)
	}
}

func TestSubgraphContext_PreservesFailureReasonAcrossNodes(t *testing.T) {
	requireIntegration(t)
	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	dot := []byte(`
digraph G {
  graph [goal="subgraph failure context fixture"]
  start [shape=Mdiamond]
  exit [shape=Msquare]
  a [shape=diamond, type="subgraph_failure_fixture"]
  cond [shape=diamond]
  start -> a
  a -> cond [condition="outcome=fail"]
  a -> exit
  cond -> exit [condition="outcome=fail"]
  cond -> exit
}
`)
	eng := newReliabilityFixtureEngine(t, repo, logsRoot, "subgraph-failure-context-fixture", dot)
	eng.Registry.Register("subgraph_failure_fixture", &subgraphFailureFixtureHandler{})

	if _, err := runSubgraphUntil(context.Background(), eng, "a", "exit"); err != nil {
		t.Fatalf("runSubgraphUntil: %v", err)
	}
	if got := eng.Context.GetString("failure_reason", ""); got == "" {
		t.Fatal("failure_reason missing in context")
	}
	if got := eng.Context.GetString("failure_class", ""); got == "" {
		t.Fatal("failure_class missing in context")
	}
}

func fanInEdgeWasSelected(t *testing.T, progressPath, from, to string) bool {
	t.Helper()
	for _, ev := range mustReadProgressEventsFile(t, progressPath) {
		if strings.TrimSpace(anyToString(ev["event"])) != "edge_selected" {
			continue
		}
		if strings.TrimSpace(anyToString(ev["from_node"])) == strings.TrimSpace(from) &&
			strings.TrimSpace(anyToString(ev["to_node"])) == strings.TrimSpace(to) {
			return true
		}
	}
	return false
}

func countProgressEventsForNode(t *testing.T, progressPath, eventName, nodeID string) int {
	t.Helper()
	count := 0
	for _, ev := range mustReadProgressEventsFile(t, progressPath) {
		if strings.TrimSpace(anyToString(ev["event"])) != strings.TrimSpace(eventName) {
			continue
		}
		if strings.TrimSpace(anyToString(ev["node_id"])) != strings.TrimSpace(nodeID) {
			continue
		}
		count++
	}
	return count
}

func hasProgressEventForNode(t *testing.T, progressPath, eventName, nodeID string) bool {
	t.Helper()
	return countProgressEventsForNode(t, progressPath, eventName, nodeID) > 0
}

func findRetryBlockedClassForNode(t *testing.T, progressPath, nodeID string) (bool, string) {
	t.Helper()
	for _, ev := range mustReadProgressEventsFile(t, progressPath) {
		if strings.TrimSpace(anyToString(ev["event"])) != "stage_retry_blocked" {
			continue
		}
		if strings.TrimSpace(anyToString(ev["node_id"])) != strings.TrimSpace(nodeID) {
			continue
		}
		return true, strings.TrimSpace(anyToString(ev["failure_class"]))
	}
	return false, ""
}

func mustReadProgressEventsFile(t *testing.T, progressPath string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(progressPath)
	if err != nil {
		t.Fatalf("read progress %s: %v", progressPath, err)
	}
	events := make([]map[string]any, 0)
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("decode progress row %q: %v", line, err)
		}
		events = append(events, ev)
	}
	return events
}

func mustReadFinalOutcome(t *testing.T, path string) runtime.FinalOutcome {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final outcome %s: %v", path, err)
	}
	var out runtime.FinalOutcome
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode final outcome %s: %v", path, err)
	}
	return out
}
