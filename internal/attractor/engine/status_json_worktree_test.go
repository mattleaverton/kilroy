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

func TestRunWithConfig_CLIBackend_WorktreeStatusJSON_IsTreatedAsStageStatus(t *testing.T) {
	requireIntegration(t)
	cleanupStrayEngineArtifacts(t)
	t.Cleanup(func() { cleanupStrayEngineArtifacts(t) })

	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail

# Simulate an agent that writes status.json in its working directory (the worktree).
cat > status.json <<'JSON'
{"status":"fail","failure_reason":"synthetic failure"}
JSON

echo '{"type":"start"}'
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="test"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]

  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do the thing"]
  fix [shape=parallelogram, tool_command="echo fixed > fixed.txt"]

  start -> a
  a -> fix [condition="outcome=fail"]
  a -> exit [condition="outcome=success"]
  a -> fix
  fix -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{RunID: "test-status-json-worktree", LogsRoot: logsRoot, AllowTestShim: true})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	// The stage status should reflect the worktree-root status.json written by the CLI backend.
	b, err := os.ReadFile(filepath.Join(res.LogsRoot, "a", "status.json"))
	if err != nil {
		t.Fatalf("read a/status.json: %v", err)
	}
	out, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode a/status.json: %v", err)
	}
	if out.Status != runtime.StatusFail {
		t.Fatalf("a status: got %q want %q (out=%+v)", out.Status, runtime.StatusFail, out)
	}
	if strings.TrimSpace(out.FailureReason) == "" {
		t.Fatalf("a failure_reason should be non-empty (out=%+v)", out)
	}

	// Ensure the fail edge executed and produced the expected committed artifact.
	if got := strings.TrimSpace(runCmdOut(t, repo, "git", "show", res.FinalCommitSHA+":fixed.txt")); got != "fixed" {
		t.Fatalf("fixed.txt: got %q want %q", got, "fixed")
	}
}

func TestRunWithConfig_CLIBackend_RunScopedStatusJSON_IsTreatedAsStageStatus(t *testing.T) {
	requireIntegration(t)
	cleanupStrayEngineArtifacts(t)
	t.Cleanup(func() { cleanupStrayEngineArtifacts(t) })

	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail

# Simulate an agent that writes status.json to the run-scoped .ai path.
[[ -n "${KILROY_RUN_ID:-}" ]] || { echo "missing KILROY_RUN_ID" >&2; exit 51; }
mkdir -p ".ai/runs/${KILROY_RUN_ID}"
cat > ".ai/runs/${KILROY_RUN_ID}/status.json" <<'JSON'
{"status":"success","notes":"written to run-scoped .ai/runs/<run_id>/status.json"}
JSON

echo '{"type":"start"}'
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="test run-scoped .ai/runs/<run_id>/status.json fallback"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]

  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do the thing"]

  start -> a
  a -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{RunID: "test-run-scoped-status-json", LogsRoot: logsRoot, AllowTestShim: true})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	// The stage status should reflect the run-scoped .ai/runs/<run_id>/status.json
	// written by the CLI backend.
	b, err := os.ReadFile(filepath.Join(res.LogsRoot, "a", "status.json"))
	if err != nil {
		t.Fatalf("read a/status.json: %v", err)
	}
	out, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode a/status.json: %v", err)
	}
	if out.Status != runtime.StatusSuccess {
		t.Fatalf("a status: got %q want %q (out=%+v)", out.Status, runtime.StatusSuccess, out)
	}
}

func TestRunWithConfig_CLIBackend_StatusContractPath_HandlesNestedCD(t *testing.T) {
	requireIntegration(t)
	cleanupStrayEngineArtifacts(t)
	t.Cleanup(func() { cleanupStrayEngineArtifacts(t) })

	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail

out=""
schema=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      out="$2"
      shift 2
      ;;
    --output-schema)
      schema="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [[ -n "$schema" ]]; then
  [[ -f "$schema" ]] || { echo "missing schema: $schema" >&2; exit 2; }
fi
if [[ -n "$out" ]]; then
  echo '{"final":"ok","summary":"ok"}' > "$out"
fi

[[ -n "${KILROY_STAGE_STATUS_PATH:-}" ]] || { echo "missing KILROY_STAGE_STATUS_PATH" >&2; exit 21; }
[[ "${KILROY_STAGE_STATUS_PATH}" = /* ]] || { echo "status path must be absolute" >&2; exit 22; }

mkdir -p demo/rogue/rogue-wasm
cd demo/rogue/rogue-wasm
mkdir -p "$(dirname "$KILROY_STAGE_STATUS_PATH")"
cat > "$KILROY_STAGE_STATUS_PATH" <<'JSON'
{"status":"fail","failure_reason":"nested cd status path write"}
JSON

echo '{"type":"start"}'
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="status contract nested cd"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]

  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do the thing"]
  fix [shape=parallelogram, tool_command="echo fixed > fixed.txt"]

  start -> a
  a -> fix [condition="outcome=fail"]
  a -> exit [condition="outcome=success"]
  a -> fix
  fix -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{
		RunID:         "test-status-contract-path-nested-cd",
		LogsRoot:      logsRoot,
		AllowTestShim: true,
	})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(res.LogsRoot, "a", "status.json"))
	if err != nil {
		t.Fatalf("read a/status.json: %v", err)
	}
	out, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode a/status.json: %v", err)
	}
	if out.Status != runtime.StatusFail {
		t.Fatalf("a status: got %q want %q (out=%+v)", out.Status, runtime.StatusFail, out)
	}
	if !strings.Contains(out.FailureReason, "nested cd status path write") {
		t.Fatalf("a failure_reason: got %q, want nested cd marker", out.FailureReason)
	}
	if got := strings.TrimSpace(runCmdOut(t, repo, "git", "show", res.FinalCommitSHA+":fixed.txt")); got != "fixed" {
		t.Fatalf("fixed.txt: got %q want %q", got, "fixed")
	}
}

func TestRunWithConfig_CLIBackend_StatusContract_ClearsStaleWorktreeStatusBeforeStage(t *testing.T) {
	requireIntegration(t)
	cleanupStrayEngineArtifacts(t)
	t.Cleanup(func() { cleanupStrayEngineArtifacts(t) })

	repo := initTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "status.json"), []byte(`{"status":"success","notes":"stale-root-status"}`), 0o644); err != nil {
		t.Fatalf("seed stale status.json in repo: %v", err)
	}
	runCmd(t, repo, "git", "add", "status.json")
	runCmd(t, repo, "git", "commit", "-m", "seed stale status")
	logsRoot := t.TempDir()

	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail

out=""
schema=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      out="$2"
      shift 2
      ;;
    --output-schema)
      schema="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [[ -n "$schema" ]]; then
  [[ -f "$schema" ]] || { echo "missing schema: $schema" >&2; exit 2; }
fi
if [[ -n "$out" ]]; then
  echo '{"final":"ok","summary":"ok"}' > "$out"
fi

echo '{"type":"start"}'
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="stale status cleanup"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]

  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do the thing"]
  fix [shape=parallelogram, tool_command="echo fixed > fixed.txt"]

  start -> a
  a -> fix [condition="outcome=fail"]
  a -> exit [condition="outcome=success"]
  a -> fix
  fix -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{
		RunID:         "test-status-contract-stale-worktree-status",
		LogsRoot:      logsRoot,
		AllowTestShim: true,
	})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(res.LogsRoot, "a", "status.json"))
	if err != nil {
		t.Fatalf("read a/status.json: %v", err)
	}
	out, err := runtime.DecodeOutcomeJSON(b)
	if err != nil {
		t.Fatalf("decode a/status.json: %v", err)
	}
	if out.Status != runtime.StatusFail {
		t.Fatalf("a status: got %q want %q (out=%+v)", out.Status, runtime.StatusFail, out)
	}
	if !strings.Contains(out.FailureReason, "missing status.json") {
		t.Fatalf("a failure_reason: got %q want mention of missing status.json", out.FailureReason)
	}
	if got := strings.TrimSpace(runCmdOut(t, repo, "git", "show", res.FinalCommitSHA+":fixed.txt")); got != "fixed" {
		t.Fatalf("fixed.txt: got %q want %q", got, "fixed")
	}
}

func TestRunWithConfig_CLIBackend_StatusContractPromptPreambleWritten(t *testing.T) {
	requireIntegration(t)
	cleanupStrayEngineArtifacts(t)
	t.Cleanup(func() { cleanupStrayEngineArtifacts(t) })

	repo := initTestRepo(t)
	logsRoot := t.TempDir()

	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail

out=""
schema=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      out="$2"
      shift 2
      ;;
    --output-schema)
      schema="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [[ -n "$schema" ]]; then
  [[ -f "$schema" ]] || { echo "missing schema: $schema" >&2; exit 2; }
fi
if [[ -n "$out" ]]; then
  echo '{"final":"ok","summary":"ok"}' > "$out"
fi

cat > status.json <<'JSON'
{"status":"success","notes":"ok"}
JSON
echo '{"type":"start"}'
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"openai": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="status contract prompt preamble"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  a [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="do the thing"]
  start -> a
  a -> exit [condition="outcome=success"]
}
`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{
		RunID:         "test-status-contract-prompt-preamble",
		LogsRoot:      logsRoot,
		AllowTestShim: true,
	})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	promptPath := filepath.Join(res.LogsRoot, "a", "prompt.md")
	b, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatalf("read prompt.md: %v", err)
	}
	prompt := string(b)
	if !strings.Contains(prompt, "Execution status contract") {
		t.Fatalf("prompt.md missing status contract preamble: %s", promptPath)
	}
	if !strings.Contains(prompt, stageStatusPathEnvKey) {
		t.Fatalf("prompt.md missing env key %s", stageStatusPathEnvKey)
	}
	// Guardrail: input materialization preamble can be prepended, but status
	// contract language must remain present and authoritative.
	if !strings.Contains(prompt, inputsManifestEnvKey) {
		t.Fatalf("prompt.md missing input manifest env key %s", inputsManifestEnvKey)
	}
	wantPrimary := filepath.Join(res.WorktreeDir, "status.json")
	if !strings.Contains(prompt, wantPrimary) {
		t.Fatalf("prompt.md missing absolute worktree status path %q", wantPrimary)
	}
}
