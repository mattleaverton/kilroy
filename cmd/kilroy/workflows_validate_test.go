// CLI tests for `kilroy workflows validate`.
package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowsValidate_Clean_ExitsZero(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	dir := filepath.Join(pkgRoot, "good")
	if err := os.MkdirAll(filepath.Join(dir, "scripts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(`
[workflow]
name = "good"
version = "1"
description = "test"
default_class = "hard_coding"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "scripts", "stage.sh"), []byte("#!/bin/bash\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(`digraph good {
  start [shape=Mdiamond, label="Start"]
  stage [shape=parallelogram, label="stage", tool_command="bash .kilroy/package/scripts/stage.sh"]
  done  [shape=Msquare, label="Done"]
  start -> stage
  stage -> done [condition="outcome=success"]
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "workflows", "validate", "good")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("validate good: %v\nstdout: %s", err, out)
	}
	var got workflowsValidateResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, out)
	}
	if got.Status != "ok" {
		t.Errorf("status = %q, want ok\nfull: %+v", got.Status, got)
	}
}

func TestWorkflowsValidate_BadPackage_ExitsOne_PrintsErrors(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	dir := filepath.Join(pkgRoot, "bad")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Manifest missing version; graph references a missing script.
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(`
[workflow]
name = "bad"
description = "missing version"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(`digraph bad {
  stage [shape=parallelogram, tool_command="bash .kilroy/package/scripts/missing.sh"]
  done  [shape=Msquare, label="Done"]
  stage -> done
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "workflows", "validate", "bad", "--pretty")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1, got 0")
	}
	for _, want := range []string{
		"status:    fail",
		"version is required",
		"missing.sh",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout missing %q\nfull stdout:\n%s\nstderr:\n%s", want, stdout.String(), stderr.String())
		}
	}
}

func TestWorkflowsValidate_UnknownName_ExitsOne(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "workflows", "validate", "no-such-workflow")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+t.TempDir(),
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1")
	}
}
