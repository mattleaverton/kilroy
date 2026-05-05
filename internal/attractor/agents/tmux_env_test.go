// Unit tests for the tmux agent session env-building helper.
package agents

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
)

func TestBuildTmuxAgentEnv_IncludesStageStatusContractAndRuntime(t *testing.T) {
	tmpl := &templates.Template{
		BuildEnv: func() map[string]string {
			return map[string]string{"TOOL_DEFAULT": "present"}
		},
	}
	workDir := t.TempDir()
	logsRoot := t.TempDir()
	execCtx := &engine.Execution{
		WorktreeDir: workDir,
		LogsRoot:    logsRoot,
		Engine: &engine.Engine{
			Options: engine.RunOptions{RunID: "run-001"},
		},
	}

	templateEnv := tmpl.BuildEnv()
	runtimeEnv := engine.BuildStageRuntimeEnv(execCtx, "node-alpha")
	statusContractEnv := engine.BuildStageStatusContract(execCtx.WorktreeDir, "run-001").EnvVars
	env := transport.BuildTmuxAgentEnv(templateEnv, runtimeEnv, statusContractEnv)

	cases := map[string]string{
		"TOOL_DEFAULT":        "present",
		"KILROY_RUN_ID":       "run-001",
		"KILROY_NODE_ID":      "node-alpha",
		"KILROY_WORKTREE_DIR": workDir,
		"KILROY_LOGS_ROOT":    logsRoot,
	}
	for k, want := range cases {
		if got := env[k]; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if env["KILROY_STAGE_STATUS_PATH"] == "" {
		t.Error("KILROY_STAGE_STATUS_PATH missing — agents cannot find where to write status.json")
	}
	if env["KILROY_STAGE_STATUS_FALLBACK_PATH"] == "" {
		t.Error("KILROY_STAGE_STATUS_FALLBACK_PATH missing")
	}
	if env["KILROY_STAGE_LOGS_DIR"] == "" {
		t.Error("KILROY_STAGE_LOGS_DIR missing")
	}
	if env["KILROY_DATA_DIR"] == "" {
		t.Error("KILROY_DATA_DIR missing")
	}
}

func TestBuildTmuxAgentEnv_NilTemplateStartsClean(t *testing.T) {
	workDir := t.TempDir()
	logsRoot := t.TempDir()
	execCtx := &engine.Execution{
		WorktreeDir: workDir,
		LogsRoot:    logsRoot,
		Engine:      &engine.Engine{Options: engine.RunOptions{RunID: "run-nil-tmpl"}},
	}
	runtimeEnv := engine.BuildStageRuntimeEnv(execCtx, "node")
	statusContractEnv := engine.BuildStageStatusContract(workDir, "run-nil-tmpl").EnvVars
	env := transport.BuildTmuxAgentEnv(nil, runtimeEnv, statusContractEnv)
	if env == nil {
		t.Fatal("env must not be nil even when template is nil")
	}
	if env["KILROY_RUN_ID"] != "run-nil-tmpl" {
		t.Errorf("KILROY_RUN_ID = %q, want run-nil-tmpl", env["KILROY_RUN_ID"])
	}
}
