// Verifies the v2 §4 output contract for the run handle: JSON to stdout
// by default, key=value text under --pretty. The shape stays a stable
// single-line JSON object so agents juggling N runs can parse it
// without hunting through stderr noise.
package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestRunHandle_JSON_IsDefault(t *testing.T) {
	res := &engine.Result{
		RunID:          "01TESTRUN",
		LogsRoot:       "/tmp/logs",
		WorktreeDir:    "/tmp/worktree",
		RunBranch:      "attractor/run/01TESTRUN",
		FinalCommitSHA: "abc123",
		FinalStatus:    runtime.FinalSuccess,
	}
	var buf bytes.Buffer
	emitRunHandle(&buf, runHandleFromResult(res), false)

	out := strings.TrimSpace(buf.String())
	if !strings.HasPrefix(out, "{") || !strings.HasSuffix(out, "}") {
		t.Fatalf("expected JSON object, got: %q", out)
	}

	var got runHandle
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON handle: %v", err)
	}
	if got.RunID != res.RunID {
		t.Errorf("run_id: got %q want %q", got.RunID, res.RunID)
	}
	if got.LogsRoot != res.LogsRoot {
		t.Errorf("logs_root: got %q want %q", got.LogsRoot, res.LogsRoot)
	}
	if got.FinalStatus != string(res.FinalStatus) {
		t.Errorf("final_status: got %q want %q", got.FinalStatus, string(res.FinalStatus))
	}
}

func TestRunHandle_Pretty_EmitsKeyValueText(t *testing.T) {
	res := &engine.Result{
		RunID:          "01TESTRUN",
		LogsRoot:       "/tmp/logs",
		WorktreeDir:    "/tmp/worktree",
		RunBranch:      "attractor/run/01TESTRUN",
		FinalCommitSHA: "abc123",
		FinalStatus:    runtime.FinalSuccess,
	}
	var buf bytes.Buffer
	emitRunHandle(&buf, runHandleFromResult(res), true)

	out := buf.String()
	for _, want := range []string{
		"run_id=01TESTRUN",
		"logs_root=/tmp/logs",
		"worktree=/tmp/worktree",
		"run_branch=attractor/run/01TESTRUN",
		"final_commit=abc123",
		"final_status=success",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("--pretty output missing %q:\n%s", want, out)
		}
	}
	// JSON braces should not appear in pretty mode.
	if strings.Contains(out, "{") || strings.Contains(out, "}") {
		t.Errorf("--pretty output should not contain JSON braces:\n%s", out)
	}
}

func TestRunHandle_Detached_PrintsDetachedKey(t *testing.T) {
	h := runHandle{
		Detached: true,
		RunID:    "01DETACH",
		LogsRoot: "/tmp/logs",
		PIDFile:  "/tmp/logs/run.pid",
	}
	var buf bytes.Buffer
	emitRunHandle(&buf, h, false)

	var got struct {
		Detached bool   `json:"detached"`
		RunID    string `json:"run_id"`
		LogsRoot string `json:"logs_root"`
		PIDFile  string `json:"pid_file"`
	}
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, line)
	}
	if !got.Detached {
		t.Errorf("detached should be true; got %+v", got)
	}
	if got.PIDFile == "" {
		t.Errorf("pid_file missing from detached handle")
	}
}

func TestRunHandle_OmitsEmptyFields(t *testing.T) {
	h := runHandle{
		RunID:    "01MINIMAL",
		LogsRoot: "/tmp/logs",
	}
	var buf bytes.Buffer
	emitRunHandle(&buf, h, false)
	out := strings.TrimSpace(buf.String())

	for _, mustNot := range []string{"worktree", "run_branch", "final_commit", "cxdb_ui", "pid_file", "final_status"} {
		if strings.Contains(out, mustNot) {
			t.Errorf("expected %q omitted in JSON: %s", mustNot, out)
		}
	}
}
