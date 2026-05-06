package config

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/projectroot"
)

func TestLoadLayered_DefaultsWithoutConfigFiles(t *testing.T) {
	workDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv(projectroot.EnvVar, "")

	lc, err := LoadLayered(workDir, CLIFlags{})
	if err != nil {
		t.Fatalf("LoadLayered() error = %v", err)
	}
	if lc.ProjectRoot != "" {
		t.Fatalf("ProjectRoot = %q, want empty when no marker exists", lc.ProjectRoot)
	}
	if got := derefInt(lc.Config.Runtime.CodergenHeartbeatIntervalMS); got != 5000 {
		t.Fatalf("CodergenHeartbeatIntervalMS = %d, want 5000", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexIdleTimeoutMS); got != 300000 {
		t.Fatalf("CodexIdleTimeoutMS = %d, want 300000", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexTotalTimeoutMS); got != 3600000 {
		t.Fatalf("CodexTotalTimeoutMS = %d, want 3600000", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexKillGraceMS); got != 5000 {
		t.Fatalf("CodexKillGraceMS = %d, want 5000", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexTimeoutMaxRetries); got != 3 {
		t.Fatalf("CodexTimeoutMaxRetries = %d, want 3", got)
	}
}

func TestLoadLayered_PrecedenceDefaultsUserProjectEnvCLI(t *testing.T) {
	xdg := t.TempDir()
	project := t.TempDir()
	writeTestFile(t, filepath.Join(project, ".kilroy", "config.toml"), `
[runtime]
codex_idle_timeout_ms = 2000
codex_total_timeout_ms = 2100
`)
	writeTestFile(t, filepath.Join(xdg, "kilroy", "config.toml"), `
[runtime]
codex_idle_timeout_ms = 1000
codex_kill_grace_ms = 1100
`)

	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv(projectroot.EnvVar, "")
	t.Setenv("KILROY_CODEX_IDLE_TIMEOUT_MS", "3000")

	lc, err := LoadLayered(filepath.Join(project, "subdir"), CLIFlags{
		Runtime: RuntimeCLIFlags{
			CodexIdleTimeoutMS: intPtr(4000),
		},
	})
	if err != nil {
		t.Fatalf("LoadLayered() error = %v", err)
	}
	if lc.ProjectRoot != project {
		t.Fatalf("ProjectRoot = %q, want %q", lc.ProjectRoot, project)
	}
	if got := derefInt(lc.Config.Runtime.CodexIdleTimeoutMS); got != 4000 {
		t.Fatalf("CodexIdleTimeoutMS = %d, want CLI override 4000", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexTotalTimeoutMS); got != 2100 {
		t.Fatalf("CodexTotalTimeoutMS = %d, want project value 2100", got)
	}
	if got := derefInt(lc.Config.Runtime.CodexKillGraceMS); got != 1100 {
		t.Fatalf("CodexKillGraceMS = %d, want user value 1100", got)
	}
	if got := derefInt(lc.Config.Runtime.CodergenHeartbeatIntervalMS); got != 5000 {
		t.Fatalf("CodergenHeartbeatIntervalMS = %d, want default 5000", got)
	}
}

func TestLoadLayered_EmptyEnvDoesNotOverride(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv(projectroot.EnvVar, "")
	t.Setenv("KILROY_CODEX_IDLE_TIMEOUT_MS", "")
	writeTestFile(t, filepath.Join(xdg, "kilroy", "config.toml"), `
[runtime]
codex_idle_timeout_ms = 1234
`)

	lc, err := LoadLayered(t.TempDir(), CLIFlags{})
	if err != nil {
		t.Fatalf("LoadLayered() error = %v", err)
	}
	if got := derefInt(lc.Config.Runtime.CodexIdleTimeoutMS); got != 1234 {
		t.Fatalf("CodexIdleTimeoutMS = %d, want user value 1234", got)
	}
}

func TestLoadLayered_InvalidProjectRootEnvReturnsError(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv(projectroot.EnvVar, filepath.Join(t.TempDir(), "missing"))

	_, err := LoadLayered(t.TempDir(), CLIFlags{})
	if err == nil {
		t.Fatal("LoadLayered() error = nil, want invalid project root error")
	}
	if !errors.Is(err, projectroot.ErrEnvRootMissingMarker) {
		t.Fatalf("LoadLayered() error = %v, want ErrEnvRootMissingMarker", err)
	}
}
