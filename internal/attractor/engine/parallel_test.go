package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestRun_ParallelFanOutAndFanIn_FastForwardsWinner(t *testing.T) {
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph P {
  graph [goal="test"]
  start [shape=Mdiamond]
  par [shape=component]
  a [shape=box, llm_provider=openai, llm_model=gpt-5.2, prompt="a"]
  b [shape=box, llm_provider=openai, llm_model=gpt-5.2, prompt="b"]
  join [shape=tripleoctagon]
  exit [shape=Msquare]

  start -> par
  par -> a
  par -> b
  a -> join
  b -> join
  join -> exit
}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	assertExists(t, filepath.Join(res.LogsRoot, "par", "parallel_results.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "join", "status.json"))

	// Winner should deterministically be branch "a" (lexical tie-break).
	b, err := os.ReadFile(filepath.Join(res.LogsRoot, "join", "status.json"))
	if err != nil {
		t.Fatalf("read join status.json: %v", err)
	}
	out, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode join status.json: %v", err)
	}
	best, ok := out.ContextUpdates["parallel.fan_in.best_id"]
	if !ok {
		t.Fatalf("missing parallel.fan_in.best_id in context updates")
	}
	if strings.TrimSpace(strings.ToLower(fmt.Sprint(best))) != "a" {
		t.Fatalf("best branch: got %v, want a", best)
	}

	// Base + 5 node commits (start, par, a, join, exit) => 6 total.
	count := strings.TrimSpace(runCmdOut(t, repo, "git", "rev-list", "--count", res.RunBranch))
	if count != "6" {
		t.Fatalf("commit count: got %s, want 6 (base+5 nodes on winning path)", count)
	}
}

func TestRun_ParallelFanOut_Component_ConvergesOnBoxJoinWithoutFastForward(t *testing.T) {
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph P {
  graph [goal="test box join"]
  start [shape=Mdiamond]
  par [shape=component]
  a [shape=parallelogram, tool_command="echo a > a.txt; exit 0"]
  b [shape=parallelogram, tool_command="echo b > b.txt; exit 0"]
  synth [shape=parallelogram, tool_command="echo synth > synth.txt; exit 0"]
  exit [shape=Msquare]

  start -> par
  par -> a
  par -> b
  a -> synth
  b -> synth
  synth -> exit
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

	// Parallel fan-out results should exist for the explicit component node.
	resultsPath := filepath.Join(res.LogsRoot, "par", "parallel_results.json")
	assertExists(t, resultsPath)

	// The join node should run on the main worktree, without fast-forwarding branch changes.
	files := runCmdOut(t, repo, "git", "ls-tree", "-r", "--name-only", res.FinalCommitSHA)
	if !strings.Contains(files, "synth.txt") {
		t.Fatalf("missing synth.txt in final commit; files:\n%s", files)
	}
	if strings.Contains(files, "a.txt") || strings.Contains(files, "b.txt") {
		t.Fatalf("unexpected branch artifacts in final commit (should not fast-forward):\n%s", files)
	}
	if got := strings.TrimSpace(runCmdOut(t, repo, "git", "show", res.FinalCommitSHA+":synth.txt")); got != "synth" {
		t.Fatalf("synth.txt: got %q want %q", got, "synth")
	}

	// Branch artifacts should exist in their isolated branch worktrees.
	b, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("read parallel_results.json: %v", err)
	}
	var results []parallelBranchResult
	if err := json.Unmarshal(b, &results); err != nil {
		t.Fatalf("unmarshal parallel_results.json: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 branch results, got %d", len(results))
	}

	seenA := false
	seenB := false
	for _, r := range results {
		switch strings.TrimSpace(r.BranchKey) {
		case "a":
			seenA = true
			data, err := os.ReadFile(filepath.Join(r.WorktreeDir, "a.txt"))
			if err != nil {
				t.Fatalf("read a.txt from branch worktree: %v (worktree=%s)", err, r.WorktreeDir)
			}
			if strings.TrimSpace(string(data)) != "a" {
				t.Fatalf("a.txt contents: got %q want %q", strings.TrimSpace(string(data)), "a")
			}
		case "b":
			seenB = true
			data, err := os.ReadFile(filepath.Join(r.WorktreeDir, "b.txt"))
			if err != nil {
				t.Fatalf("read b.txt from branch worktree: %v (worktree=%s)", err, r.WorktreeDir)
			}
			if strings.TrimSpace(string(data)) != "b" {
				t.Fatalf("b.txt contents: got %q want %q", strings.TrimSpace(string(data)), "b")
			}
		}
	}
	if !seenA || !seenB {
		t.Fatalf("missing expected branch keys; seenA=%v seenB=%v results=%+v", seenA, seenB, results)
	}
}

func TestFanIn_RunScopedPromotion_DefaultNoneDoesNotPromote(t *testing.T) {
	res := runParallelPromotionFixture(t, nil)

	if got := strings.TrimSpace(anyToString(res.joinOutcome.ContextUpdates["input_lineage.run_head_revision"])); got != "" {
		t.Fatalf("default no-promotion should not set input_lineage.run_head_revision, got %q", got)
	}

	promoted := runScopedPath(res.worktreeDir, res.runID, "postmortem_latest.md")
	if fileExists(promoted) {
		t.Fatalf("default fan-in promotion must not materialize run-scoped file: %s", promoted)
	}

	lineage, err := LoadInputSnapshotLineage(res.logsRoot)
	if err != nil {
		t.Fatalf("LoadInputSnapshotLineage: %v", err)
	}
	head := lineage.Revisions[lineage.RunHead]
	if _, ok := head.FileDigest["postmortem_latest.md"]; ok {
		t.Fatalf("default no-promotion should not add postmortem_latest.md to run head digest")
	}
}

func TestFanIn_RunScopedPromotion_ExplicitListPromotesDeterministically(t *testing.T) {
	resA := runParallelPromotionFixture(t, []string{"postmortem_latest.md", "review_final.md"})
	resB := runParallelPromotionFixture(t, []string{"postmortem_latest.md", "review_final.md"})

	promoted := runScopedPath(resA.worktreeDir, resA.runID, "postmortem_latest.md")
	if !fileExists(promoted) {
		t.Fatalf("expected promoted run-scoped file in winner worktree: %s", promoted)
	}
	if got := strings.TrimSpace(readFile(t, promoted)); got != "merged-content" {
		t.Fatalf("postmortem_latest.md contents: got %q want %q", got, "merged-content")
	}

	headA := strings.TrimSpace(anyToString(resA.joinOutcome.ContextUpdates["input_lineage.run_head_revision"]))
	headB := strings.TrimSpace(anyToString(resB.joinOutcome.ContextUpdates["input_lineage.run_head_revision"]))
	if headA == "" || headB == "" {
		t.Fatalf("expected non-empty input_lineage.run_head_revision context update; got %q / %q", headA, headB)
	}
	if headA != headB {
		t.Fatalf("promotion merge must be deterministic: headA=%q headB=%q", headA, headB)
	}

	lineage, err := LoadInputSnapshotLineage(resA.logsRoot)
	if err != nil {
		t.Fatalf("LoadInputSnapshotLineage: %v", err)
	}
	head := lineage.Revisions[lineage.RunHead]
	if strings.TrimSpace(head.FileDigest["postmortem_latest.md"]) == "" ||
		strings.TrimSpace(head.FileDigest["review_final.md"]) == "" {
		t.Fatalf("expected promoted files in final run head digest: %+v", head.FileDigest)
	}
}

func TestFanIn_RunScopedPromotion_UnresolvedGlobIsBestEffort(t *testing.T) {
	res := runParallelPromotionFixture(t, []string{"does-not-exist-*.md"})
	if res.finalStatus != runtime.FinalSuccess {
		t.Fatalf("unresolved promotion globs should be best-effort, got final status %q", res.finalStatus)
	}
}

type parallelPromotionFixtureResult struct {
	runID       string
	logsRoot    string
	worktreeDir string
	finalStatus runtime.FinalStatus
	joinOutcome runtime.Outcome
}

func runParallelPromotionFixture(t *testing.T, promote []string) parallelPromotionFixtureResult {
	t.Helper()

	repo := initTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(".ai/runs/\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	runCmd(t, repo, "git", "add", ".gitignore")
	runCmd(t, repo, "git", "commit", "-m", "ignore run-scoped scratch")

	logsRoot := t.TempDir()
	runID := "lineage-promotion-run"

	cfg := newInputMaterializationRunConfigForTest(t, repo)
	cfg.Inputs.Materialize.FanIn.PromoteRunScoped = append([]string{}, promote...)

	dot := []byte(fmt.Sprintf(`
digraph P {
  graph [goal="fanin run-scoped promotion fixture"]
  start [shape=Mdiamond]
  par [shape=component]
  a [shape=parallelogram, tool_command="mkdir -p .ai/runs/%s && printf 'merged-content' > .ai/runs/%s/postmortem_latest.md && printf 'review-content' > .ai/runs/%s/review_final.md"]
  b [shape=parallelogram, tool_command="mkdir -p .ai/runs/%s && printf 'merged-content' > .ai/runs/%s/postmortem_latest.md && printf 'review-content' > .ai/runs/%s/review_final.md"]
  join [shape=tripleoctagon]
  exit [shape=Msquare]
  start -> par
  par -> a
  par -> b
  a -> join
  b -> join
  join -> exit
}
`, runID, runID, runID, runID, runID, runID))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{
		RunID:       runID,
		LogsRoot:    logsRoot,
		DisableCXDB: true,
	})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}
	if res == nil {
		t.Fatalf("RunWithConfig returned nil result")
	}

	joinStatusPath := filepath.Join(res.LogsRoot, "join", "status.json")
	b, err := os.ReadFile(joinStatusPath)
	if err != nil {
		t.Fatalf("read %s: %v", joinStatusPath, err)
	}
	joinOutcome, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode join status.json: %v", err)
	}

	return parallelPromotionFixtureResult{
		runID:       runID,
		logsRoot:    res.LogsRoot,
		worktreeDir: res.WorktreeDir,
		finalStatus: res.FinalStatus,
		joinOutcome: joinOutcome,
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
