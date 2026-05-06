package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunInPlaceExecutesInWorkspaceWithoutGitWorktree(t *testing.T) {
	bin := buildKilroyBinary(t)
	repo := initTestRepo(t)
	graph := filepath.Join(t.TempDir(), "graph.dot")
	marker := "in-place-marker.txt"
	if err := os.WriteFile(graph, []byte(`digraph in_place {
  start [shape=Mdiamond]
  stage [shape=parallelogram, tool_command="pwd > cwd.txt && printf ok > `+marker+`"]
  done [shape=Msquare]
  start -> stage
  stage -> done [condition="outcome=success"]
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	logsRoot := filepath.Join(t.TempDir(), "logs")
	code, out := runKilroyInDir(t, repo, bin,
		"run", "--sync", "--in-place", "--graph", graph, "--workspace", repo,
		"--logs-root", logsRoot, "--no-cxdb",
	)
	if code != 0 {
		t.Fatalf("kilroy run --in-place exit %d, want 0\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(repo, marker)); err != nil {
		t.Fatalf("expected marker in source repo, not an isolated worktree: %v\n%s", err, out)
	}
	cwdRaw, err := os.ReadFile(filepath.Join(repo, "cwd.txt"))
	if err != nil {
		t.Fatalf("read cwd marker: %v", err)
	}
	if got := strings.TrimSpace(string(cwdRaw)); got != repo {
		t.Fatalf("tool command cwd = %q, want repo %q", got, repo)
	}
	if _, err := os.Stat(filepath.Join(logsRoot, "worktree", marker)); err == nil {
		t.Fatalf("marker unexpectedly written to isolated worktree under logs_root")
	}
}
