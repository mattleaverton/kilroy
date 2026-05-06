package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunAsyncHandleIncludesPreLaunchReport(t *testing.T) {
	bin := buildKilroyBinary(t)
	graph := filepath.Join(t.TempDir(), "graph.dot")
	if err := os.WriteFile(graph, []byte(`digraph ok {
  start [shape=Mdiamond]
  done [shape=Msquare]
  start -> done
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	logsRoot := filepath.Join(t.TempDir(), "logs")
	code, out := runKilroy(t, bin,
		"run", "--graph", graph,
		"--logs-root", logsRoot,
		"--no-cxdb",
		"--confirm-stale-build",
	)
	if code != 0 {
		t.Fatalf("async run exit %d, want 0\n%s", code, out)
	}
	var got runHandle
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("decode run handle: %v\n%s", err, out)
	}
	if !got.Detached || got.RunID == "" || got.PreLaunch == nil {
		t.Fatalf("handle missing detached/run_id/prelaunch: %+v\n%s", got, out)
	}
	if got.PreLaunch.Summary.Fail != 0 {
		t.Fatalf("prelaunch failures in async handle: %+v\n%s", got.PreLaunch.Summary, out)
	}
}

func TestRunAsyncFailsBeforeDetachWhenPreLaunchFails(t *testing.T) {
	bin := buildKilroyBinary(t)
	pkgRoot := t.TempDir()
	dir := filepath.Join(pkgRoot, "badflow")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(`
[workflow]
name = "badflow"
version = "1"
description = "bad"
default_class = "definitely_not_a_class"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(`digraph badflow {
  start [shape=Mdiamond]
  agent [shape=box, agent_class="definitely_not_a_class", prompt="do it"]
  done [shape=Msquare]
  start -> agent
  agent -> done [condition="outcome=success"]
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	logsRoot := filepath.Join(t.TempDir(), "logs")
	code, out := runKilroy(t, bin,
		"run", "badflow",
		"--logs-root", logsRoot,
		"--no-cxdb",
		"--confirm-stale-build",
	)
	if code == 0 {
		t.Fatalf("expected async launch to fail before detach, got exit 0\n%s", out)
	}
	for _, want := range []string{"prelaunch validation failed", "definitely_not_a_class"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(logsRoot, "run.pid")); err == nil {
		t.Fatalf("run.pid exists; launch detached after failed prelaunch")
	}
}
