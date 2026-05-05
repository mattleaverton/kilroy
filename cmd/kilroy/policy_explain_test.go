// Tests for `kilroy policy explain <run-id>`. Builds a synthetic logs_root
// with two resolution.json artifacts and a corresponding rundb entry, then
// exercises both --json and human-text rendering through the real binary.
package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

// seedRunWithResolutions creates a temp XDG_STATE_HOME and writes:
//   - a rundb entry pointing at a synthetic logs_root
//   - a per-stage resolution.json under <logs_root>/<node_id>/resolution.json
//
// Returns the state dir (env XDG_STATE_HOME) and run id.
func seedRunWithResolutions(t *testing.T, runID string, resolutions map[string]string) string {
	t.Helper()
	stateHome := t.TempDir()
	logsRoot := filepath.Join(stateHome, "kilroy", "runs", runID)

	for node, body := range resolutions {
		stageDir := filepath.Join(logsRoot, node)
		if err := os.MkdirAll(stageDir, 0o755); err != nil {
			t.Fatalf("mkdir stage: %v", err)
		}
		if err := os.WriteFile(filepath.Join(stageDir, "resolution.json"), []byte(body), 0o644); err != nil {
			t.Fatalf("write resolution.json: %v", err)
		}
	}

	dbPath := filepath.Join(stateHome, "kilroy", "runs.db")
	db, err := rundb.Open(dbPath)
	if err != nil {
		t.Fatalf("rundb open: %v", err)
	}
	defer db.Close()

	if err := db.RecordRunStart(runID, "explain-test", "", "running", logsRoot, "", "", "", "", nil, map[string]string{"task": "explain-test"}, nil, nil, ""); err != nil {
		t.Fatalf("rundb RecordRunStart: %v", err)
	}
	if err := db.RecordRunComplete(runID, "success", "", "", nil); err != nil {
		t.Fatalf("rundb RecordRunComplete: %v", err)
	}

	return stateHome
}

const fixtureResolutionAgent = `{
  "schema_version": "1",
  "node_id": "agent",
  "workflow_id": "explain-test",
  "resolution": {
    "requested": {"type": "class", "value": "hard_coding"},
    "resolved": {
      "model_id": "claude-opus-4-7",
      "driver": "claude_cli",
      "transport": "cli_subprocess",
      "auth_method": "cli_session",
      "auth_source": "claude",
      "turn_codec": "claude-cli-jsonl"
    },
    "fallback_rank": 1,
    "skipped": [
      {"rank": 0, "model_id": "claude-opus-4-7", "driver": "anthropic_sdk", "reason": "env_var_missing:ANTHROPIC_API_KEY"}
    ],
    "policy_version": "2.0.0",
    "resolved_at": "2026-05-01T22:00:00Z"
  }
}`

const fixtureResolutionRefiner = `{
  "schema_version": "1",
  "node_id": "refiner",
  "workflow_id": "explain-test",
  "resolution": {
    "requested": {"type": "class", "value": "quick_easy"},
    "resolved": {
      "model_id": "claude-haiku-4-5",
      "driver": "claude_cli",
      "transport": "cli_subprocess",
      "auth_method": "cli_session",
      "auth_source": "claude",
      "turn_codec": "claude-cli-jsonl"
    },
    "fallback_rank": 0,
    "skipped": [],
    "policy_version": "2.0.0",
    "resolved_at": "2026-05-01T22:00:01Z"
  }
}`

func TestPolicyExplain_JSONOutput_RendersAllResolutions(t *testing.T) {
	bin := buildTestBinary(t)
	stateHome := seedRunWithResolutions(t, "01TEST_EXPLAIN_JSON", map[string]string{
		"agent":   fixtureResolutionAgent,
		"refiner": fixtureResolutionRefiner,
	})

	cmd := exec.Command(bin, "policy", "explain", "01TEST_EXPLAIN_JSON", "--json")
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateHome)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kilroy policy explain --json failed: %v\nstdout: %s", err, out)
	}

	var got policyExplainResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}

	if got.RunID != "01TEST_EXPLAIN_JSON" {
		t.Errorf("run_id = %q, want 01TEST_EXPLAIN_JSON", got.RunID)
	}
	if got.GraphName != "explain-test" {
		t.Errorf("graph_name = %q, want explain-test", got.GraphName)
	}
	if !strings.HasSuffix(got.LogsRoot, "01TEST_EXPLAIN_JSON") {
		t.Errorf("logs_root = %q, want suffix 01TEST_EXPLAIN_JSON", got.LogsRoot)
	}
	if len(got.Resolutions) != 2 {
		t.Fatalf("resolutions len = %d, want 2", len(got.Resolutions))
	}

	// Sorted by node_id alphabetically.
	if got.Resolutions[0]["node_id"] != "agent" {
		t.Errorf("resolutions[0].node_id = %v, want agent", got.Resolutions[0]["node_id"])
	}
	if got.Resolutions[1]["node_id"] != "refiner" {
		t.Errorf("resolutions[1].node_id = %v, want refiner", got.Resolutions[1]["node_id"])
	}
}

func TestPolicyExplain_HumanOutput_ShowsResolvedAndSkipped(t *testing.T) {
	bin := buildTestBinary(t)
	stateHome := seedRunWithResolutions(t, "01TEST_EXPLAIN_TEXT", map[string]string{
		"agent": fixtureResolutionAgent,
	})

	cmd := exec.Command(bin, "policy", "explain", "01TEST_EXPLAIN_TEXT")
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateHome)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kilroy policy explain failed: %v\nstdout: %s", err, out)
	}
	s := string(out)

	for _, want := range []string{
		"01TEST_EXPLAIN_TEXT",
		"explain-test",
		"node agent:",
		"requested: class=hard_coding",
		"claude-opus-4-7 via claude_cli",
		"cli_session: claude",
		"rank:      1",
		"policy:    2.0.0",
		"skipped:",
		"env_var_missing:ANTHROPIC_API_KEY",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, s)
		}
	}
}

func TestPolicyExplain_UnknownRun_Exit1(t *testing.T) {
	bin := buildTestBinary(t)
	stateHome := t.TempDir()
	// Build an empty rundb so the lookup at least gets a clean DB.
	db, err := rundb.Open(filepath.Join(stateHome, "kilroy", "runs.db"))
	if err != nil {
		t.Fatalf("rundb open: %v", err)
	}
	db.Close()

	cmd := exec.Command(bin, "policy", "explain", "definitely_not_a_run")
	cmd.Env = append(os.Environ(), "XDG_STATE_HOME="+stateHome)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1 for unknown run, got exit 0")
	}
	if !strings.Contains(stderr.String(), "no run found") {
		t.Errorf("stderr missing %q\nfull stderr:\n%s", "no run found", stderr.String())
	}
}
