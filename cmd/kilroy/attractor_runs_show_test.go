// Tests for `attractor runs show --json` provider_selections surfacing.
//
// F2 contract: when the run DB has rows in provider_selections for this run,
// `kilroy runs show <id> --json` MUST include them as a top-level array
// `provider_selections`. When the run has zero rows (e.g. prelaunch failed
// before any node ran), the field is omitted from the JSON entirely
// (omitempty), matching the rest of runShowDetail's optional-field style.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

func runKilroyShow(t *testing.T, bin, stateDir string, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateDir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("kilroy runs show timed out\n%s", out)
	}
	if err == nil {
		return 0, string(out)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("unexpected non-ExitError: %v\n%s", err, out)
	}
	return ee.ExitCode(), string(out)
}

// TestRunsShow_JSONIncludesProviderSelections inserts a run with two
// provider_selections rows and confirms the JSON output exposes them under
// the `provider_selections` key with the expected fields.
func TestRunsShow_JSONIncludesProviderSelections(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "show-selections-001"
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "f2-show-selections",
		Status:    "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.InsertProviderSelection(runID, "start", 1, "anthropic", "claude-opus-4-7", "cli"); err != nil {
		t.Fatalf("InsertProviderSelection start: %v", err)
	}
	if err := db.InsertProviderSelection(runID, "agent", 1, "anthropic", "claude-opus-4-7", "cli"); err != nil {
		t.Fatalf("InsertProviderSelection agent: %v", err)
	}
	if err := db.CompleteRun(runID, "success", "", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", runID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	var got struct {
		RunID              string `json:"run_id"`
		ProviderSelections []struct {
			NodeID     string `json:"node_id"`
			Attempt    int    `json:"attempt"`
			Provider   string `json:"provider"`
			Model      string `json:"model"`
			Backend    string `json:"backend"`
			SelectedAt string `json:"selected_at"`
		} `json:"provider_selections"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if got.RunID != runID {
		t.Errorf("run_id = %q, want %q", got.RunID, runID)
	}
	if len(got.ProviderSelections) != 2 {
		t.Fatalf("provider_selections len = %d, want 2; output:\n%s", len(got.ProviderSelections), out)
	}
	first := got.ProviderSelections[0]
	if first.NodeID != "start" || first.Provider != "anthropic" || first.Model != "claude-opus-4-7" || first.Backend != "cli" || first.Attempt != 1 {
		t.Errorf("first selection = %+v; want node_id=start provider=anthropic model=claude-opus-4-7 backend=cli attempt=1", first)
	}
	if first.SelectedAt == "" {
		t.Errorf("selected_at should be populated; got empty string")
	}
	if _, err := time.Parse(time.RFC3339Nano, first.SelectedAt); err != nil {
		// Fall back to RFC3339 (no fractional seconds) before failing.
		if _, err2 := time.Parse(time.RFC3339, first.SelectedAt); err2 != nil {
			t.Errorf("selected_at %q is not RFC3339[Nano]: %v / %v", first.SelectedAt, err, err2)
		}
	}
	second := got.ProviderSelections[1]
	if second.NodeID != "agent" {
		t.Errorf("second selection node_id = %q, want agent", second.NodeID)
	}
}

// TestRunsShow_JSONOmitsEmptyProviderSelections confirms that a run with
// zero rows in provider_selections (e.g. prelaunch-failure case) does not
// emit the field — matching the rest of runShowDetail's omitempty style.
func TestRunsShow_JSONOmitsEmptyProviderSelections(t *testing.T) {
	bin := buildKilroyBinary(t)
	stateDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateDir)

	const runID = "show-no-selections-001"
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		t.Fatalf("open rundb: %v", err)
	}
	if err := db.InsertRun(rundb.RunRecord{
		RunID:     runID,
		GraphName: "f2-no-selections",
		Status:    "running",
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertRun: %v", err)
	}
	if err := db.CompleteRun(runID, "fail", "prelaunch_validation_failed", "", nil); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}
	db.Close()

	code, out := runKilroyShow(t, bin, stateDir, "runs", "show", runID, "--json")
	if code != 0 {
		t.Fatalf("runs show exit %d; output:\n%s", code, out)
	}

	if strings.Contains(out, `"provider_selections"`) {
		t.Errorf("JSON should omit provider_selections when no rows exist; got:\n%s", out)
	}

	var generic map[string]any
	if err := json.Unmarshal([]byte(out), &generic); err != nil {
		t.Fatalf("parse JSON: %v\n%s", err, out)
	}
	if _, present := generic["provider_selections"]; present {
		t.Errorf("provider_selections key present in decoded map; want absent")
	}
}
