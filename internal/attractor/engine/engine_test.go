package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRun_CreatesWorktreeAndCommitsPerNode(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	dot := []byte(`
digraph T {
  graph [goal="test"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do nothing"]
  start -> a
  a -> exit [condition="outcome=success"]
}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{RepoPath: repo})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if res.RunID == "" {
		t.Fatalf("RunID empty")
	}
	if !strings.Contains(res.RunBranch, res.RunID) {
		t.Fatalf("RunBranch %q missing run id", res.RunBranch)
	}

	// Verify expected artifacts exist.
	assertExists(t, filepath.Join(res.LogsRoot, "manifest.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "checkpoint.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "final.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "start", "status.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "a", "prompt.md"))
	assertExists(t, filepath.Join(res.LogsRoot, "a", "response.md"))
	assertExists(t, filepath.Join(res.LogsRoot, "a", "status.json"))
	assertExists(t, filepath.Join(res.LogsRoot, "exit", "status.json"))
	assertExists(t, res.WorktreeDir)

	// Verify commits were created on the run branch (start, a, exit).
	count := strings.TrimSpace(runCmdOut(t, repo, "git", "rev-list", "--count", res.RunBranch))
	if count == "" {
		t.Fatalf("rev-list count empty")
	}
	// Branch includes the base commit + 3 per-node commits.
	if count != "4" {
		t.Fatalf("commit count: got %s, want 4 (base+3 nodes)", count)
	}

	// checkpoint.json records the final git commit SHA (metaspec).
	cp, err := os.ReadFile(filepath.Join(res.LogsRoot, "checkpoint.json"))
	if err != nil {
		t.Fatalf("read checkpoint.json: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(cp, &m)
	if strings.TrimSpace(fmt.Sprint(m["git_commit_sha"])) != strings.TrimSpace(res.FinalCommitSHA) {
		t.Fatalf("checkpoint git_commit_sha: got %v want %s", m["git_commit_sha"], res.FinalCommitSHA)
	}

	// Commit message format: attractor(<run_id>): <node_id> (<status>)
	msg := strings.TrimSpace(runCmdOut(t, repo, "git", "log", "-1", "--format=%s", res.FinalCommitSHA))
	wantPrefix := "attractor(" + res.RunID + "): "
	if !strings.HasPrefix(msg, wantPrefix) || !strings.Contains(msg, "(success)") {
		t.Fatalf("commit msg: %q", msg)
	}
}

func TestReliabilityHelpers_CompileSmoke(t *testing.T) {
	_ = runStatusIngestionFixture
	_ = runHeartbeatFixture
	_ = runParallelWatchdogFixture
	_ = runCanceledSubgraphFixture
	_ = runDeterministicSubgraphCycleFixture
	_ = runStatusIngestionProgressFixture
	_ = runSubgraphCycleProgressFixture
	_ = runSubgraphCancelProgressFixture
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func runCmd(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, string(out))
	}
}

func runCmdOut(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, string(out))
	}
	return string(out)
}
