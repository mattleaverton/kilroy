package engine

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestStageStatusContract_AbsolutePaths_FromRelativeWorktreeInput(t *testing.T) {
	rel := filepath.Join("tmp", "wt")
	c := BuildStageStatusContract(rel, "test-run")

	if !filepath.IsAbs(c.PrimaryPath) {
		t.Fatalf("primary path must be absolute, got %q", c.PrimaryPath)
	}
	if !filepath.IsAbs(c.FallbackPath) {
		t.Fatalf("fallback path must be absolute, got %q", c.FallbackPath)
	}
}

func TestStageStatusContract_DefaultPaths(t *testing.T) {
	wt := t.TempDir()
	c := BuildStageStatusContract(wt, "test-run")

	if got, want := c.PrimaryPath, filepath.Join(wt, "status.json"); got != want {
		t.Fatalf("primary path: got %q want %q", got, want)
	}
	if got, want := c.FallbackPath, filepath.Join(wt, ".ai", "runs", "test-run", "status.json"); got != want {
		t.Fatalf("fallback path: got %q want %q", got, want)
	}
	if got := c.EnvVars[stageStatusPathEnvKey]; strings.TrimSpace(got) == "" {
		t.Fatalf("missing %s in EnvVars", stageStatusPathEnvKey)
	}
	if got := c.EnvVars[stageStatusFallbackPathEnvKey]; strings.TrimSpace(got) == "" {
		t.Fatalf("missing %s in EnvVars", stageStatusFallbackPathEnvKey)
	}
	if !strings.Contains(c.PromptPreamble, stageStatusPathEnvKey) {
		t.Fatalf("prompt preamble missing env key %s", stageStatusPathEnvKey)
	}
	if !strings.Contains(c.PromptPreamble, stageStatusFallbackPathEnvKey) {
		t.Fatalf("prompt preamble missing env key %s", stageStatusFallbackPathEnvKey)
	}
}

// TestStageStatusContract_RunIDExplicit_BeatsEnvLeak verifies that when
// the caller passes a specific runID, that wins over a leaked
// KILROY_RUN_ID from the parent process env. This is the F11 fix:
// previously, a kilroy run launched from inside a kilroy tool node
// would inherit the parent's run_id in the fallback path because
// inferRunIDForStatusFallback consulted os.Getenv.
func TestStageStatusContract_RunIDExplicit_BeatsEnvLeak(t *testing.T) {
	wt := t.TempDir()
	t.Setenv(runIDEnvKey, "leaked-parent-run-id")

	c := BuildStageStatusContract(wt, "current-run")
	if got, want := c.FallbackPath, filepath.Join(wt, ".ai", "runs", "current-run", "status.json"); got != want {
		t.Fatalf("fallback should use explicit runID, not leaked env: got %q want %q", got, want)
	}
}
