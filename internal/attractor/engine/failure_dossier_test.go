package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRun_FailureDossierCapturedAndInjectedIntoPrompt(t *testing.T) {
	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	dot := []byte(`
digraph G {
  graph [goal="failure dossier capture"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]

  check_toolchain [shape=parallelogram, tool_command="cd missing/bootstrap && rustc --version"]
  postmortem [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="Analyze the failure and route."]

  start -> check_toolchain
  check_toolchain -> postmortem [condition="outcome=fail"]
  check_toolchain -> postmortem
  postmortem -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := runForTest(t, ctx, dot, RunOptions{
		RepoPath: repo,
		RunID:    "test-failure-dossier",
		LogsRoot: logsRoot,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	logsDossierPath := filepath.Join(res.LogsRoot, failureDossierFileName)
	b, err := os.ReadFile(logsDossierPath)
	if err != nil {
		t.Fatalf("read failure dossier: %v", err)
	}
	var dossier failureDossier
	if err := json.Unmarshal(b, &dossier); err != nil {
		t.Fatalf("decode failure dossier: %v", err)
	}

	if dossier.FailedNodeID != "check_toolchain" {
		t.Fatalf("failed_node_id: got %q want %q", dossier.FailedNodeID, "check_toolchain")
	}
	if dossier.FailureClass != failureClassDeterministic {
		t.Fatalf("failure_class: got %q want %q", dossier.FailureClass, failureClassDeterministic)
	}
	if dossier.Tool == nil {
		t.Fatal("tool dossier is nil")
	}
	if strings.TrimSpace(dossier.Tool.Command) == "" {
		t.Fatal("tool command should be populated in failure dossier")
	}
	if got := strings.TrimSpace(dossier.Tool.WorkingDir); got == "" || !filepath.IsAbs(got) {
		t.Fatalf("tool working_dir should be absolute, got %q", got)
	}
	if !strings.Contains(dossier.Tool.StderrExcerpt, "No such file or directory") {
		t.Fatalf("expected stderr excerpt to include missing path signal, got %q", dossier.Tool.StderrExcerpt)
	}
	if len(dossier.MissingPaths) == 0 {
		t.Fatal("expected at least one missing path in failure dossier")
	}
	pathFact, ok := findFailurePathFact(dossier.MissingPaths, "missing/bootstrap")
	if !ok {
		t.Fatalf("missing path facts do not include %q: %+v", "missing/bootstrap", dossier.MissingPaths)
	}
	if !pathFact.InsideRepo {
		t.Fatalf("missing path should be marked inside_repo=true: %+v", pathFact)
	}
	if pathFact.ExistsNow {
		t.Fatalf("missing path should not exist now: %+v", pathFact)
	}

	worktreeRelPath := failureDossierRunScopedRelativePath(res.RunID)
	worktreeDossierPath := filepath.Join(res.WorktreeDir, filepath.FromSlash(worktreeRelPath))
	if _, err := os.Stat(worktreeDossierPath); err != nil {
		t.Fatalf("worktree failure dossier missing: %v", err)
	}

	promptPath := filepath.Join(res.LogsRoot, "postmortem", "prompt.md")
	promptBytes, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatalf("read postmortem prompt: %v", err)
	}
	prompt := string(promptBytes)
	if !strings.Contains(prompt, "Failure dossier contract") {
		t.Fatalf("postmortem prompt missing failure dossier preamble:\n%s", prompt)
	}
	if !strings.Contains(prompt, worktreeRelPath) {
		t.Fatalf("postmortem prompt missing worktree failure dossier path %q:\n%s", worktreeRelPath, prompt)
	}
	if !strings.Contains(prompt, logsDossierPath) {
		t.Fatalf("postmortem prompt missing logs failure dossier path %q:\n%s", logsDossierPath, prompt)
	}
}

func TestExtractMissingExecutables(t *testing.T) {
	text := `
bash: line 1: wasm-pack: command not found
npm: command not found
bash: line 2: wasm-pack: command not found
`
	got := extractMissingExecutables(text)
	want := []string{"npm", "wasm-pack"}
	if !slices.Equal(got, want) {
		t.Fatalf("extractMissingExecutables: got=%v want=%v", got, want)
	}
}

func findFailurePathFact(facts []failureDossierPathFact, wantPath string) (failureDossierPathFact, bool) {
	for _, fact := range facts {
		if strings.TrimSpace(fact.Path) == strings.TrimSpace(wantPath) {
			return fact, true
		}
	}
	return failureDossierPathFact{}, false
}
