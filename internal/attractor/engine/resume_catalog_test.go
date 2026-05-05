package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResume_WithRunConfig_LoadErrorIsNotSilentlyIgnored(t *testing.T) {
	requireIntegration(t)
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	runCmd(t, repo, "git", "config", "user.name", "tester")
	runCmd(t, repo, "git", "config", "user.email", "tester@example.com")
	_ = os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644)
	runCmd(t, repo, "git", "add", "-A")
	runCmd(t, repo, "git", "commit", "-m", "init")

	logsRoot := t.TempDir()
	pinned := filepath.Join(t.TempDir(), "pinned.json")
	_ = os.WriteFile(pinned, []byte(`{"data":[{"id":"openai/gpt-5","supported_parameters":["tools"]}]}`), 0o644)
	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "codex")
	_ = os.WriteFile(cli, []byte("#!/usr/bin/env bash\nset -euo pipefail\n\necho '{\"type\":\"done\",\"text\":\"ok\"}'\n"), 0o755)
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{"openai": {Backend: BackendCLI, Executable: cli}}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := []byte(`
digraph G {
  graph [goal="test"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  failed [shape=Msquare, terminal_status="fail"]
  a [shape=box, llm_provider=openai, llm_model=gpt-5, prompt="say hi"]
  start -> a
  a -> exit   [condition="outcome=success"]
  a -> failed [condition="outcome!=success"]
}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := RunWithConfig(ctx, dot, cfg, RunOptions{RunID: "resume-run-config-load-fail", LogsRoot: logsRoot, AllowTestShim: true}); err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	cfgPath := filepath.Join(logsRoot, "run_config.json")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read run_config.json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal run_config.json: %v", err)
	}
	gitMap, _ := doc["git"].(map[string]any)
	if gitMap == nil {
		gitMap = map[string]any{}
		doc["git"] = gitMap
	}
	gitMap["checkpoint_exclude_globs"] = []any{"**/legacy/**"}
	updated, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal run_config.json: %v", err)
	}
	if err := os.WriteFile(cfgPath, updated, 0o644); err != nil {
		t.Fatalf("write run_config.json: %v", err)
	}

	_, err = Resume(ctx, logsRoot)
	if err == nil {
		t.Fatal("expected resume to fail when run_config.json cannot be loaded")
	}
	if !strings.Contains(err.Error(), "resume: load run config") {
		t.Fatalf("expected resume load-config error, got: %v", err)
	}
}
