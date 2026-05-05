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

func TestWorkflowsValidate_AdHocProviderWithoutSpec_ExitsOne(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	dir := filepath.Join(pkgRoot, "unknown-provider")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(`
[workflow]
name = "unknown-provider"
version = "1"
description = "test"
default_class = "hard_coding"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(`digraph unknown_provider {
  start [shape=Mdiamond, label="Start"]
  a [shape=box, label="agent", llm_provider="local-openai-compatible", llm_model="local-model"]
  done [shape=Msquare, label="Done"]
  start -> a
  a -> done [condition="outcome=success"]
}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "workflows", "validate", "unknown-provider")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err == nil {
		t.Fatalf("expected validate to fail for unresolved provider route\nstdout: %s", out)
	}
	var got workflowsValidateResult
	if decodeErr := json.Unmarshal(out, &got); decodeErr != nil {
		t.Fatalf("output not JSON: %v\n%s", decodeErr, out)
	}
	if got.Status != "fail" {
		t.Fatalf("status = %q, want fail\nfull: %+v", got.Status, got)
	}
	if got.PreLaunch == nil || len(got.PreLaunch.Nodes) != 1 || got.PreLaunch.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed prelaunch node, got %+v", got.PreLaunch)
	}
	joined := strings.Join(got.PreLaunch.Nodes[0].Errors, " ")
	for _, want := range []string{"local-openai-compatible", "no executable route"} {
		if !strings.Contains(joined, want) {
			t.Errorf("prelaunch errors missing %q: %v", want, got.PreLaunch.Nodes[0].Errors)
		}
	}
}

// TestWorkflowsValidate_RejectsRawModelStylesheet verifies that `workflows
// validate <name>` rejects raw model references in stylesheets. A workflow
// whose graph.dot declares llm_model/llm_provider in model_stylesheet must
// produce a stylesheet_raw_model_rejected ERROR in dot_issues and status fail.
func TestWorkflowsValidate_RejectsRawModelStylesheet(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	dir := filepath.Join(pkgRoot, "badmodel")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(`
[workflow]
name        = "badmodel"
version     = "1"
description = "Test workflow with raw model stylesheet"
graph       = "graph.dot"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	src := `digraph G {
  graph [model_stylesheet="* { llm_provider: anthropic; llm_model: claude-sonnet-4.6; }"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  work  [shape=box, agent_class="hard_coding", prompt="Do the work. Write $KILROY_STAGE_STATUS_PATH (fallback: $KILROY_STAGE_STATUS_FALLBACK_PATH) with outcome=success when done."]
  start -> work
  work -> exit [condition="outcome=success"]
}`
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "workflows", "validate", "badmodel")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, _ := cmd.Output()

	var got workflowsValidateResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, out)
	}
	if got.Status != "fail" {
		t.Errorf("status = %q, want fail\nfull: %+v", got.Status, got)
	}
	found := false
	for _, d := range got.DOTIssues {
		if d.Rule == "stylesheet_raw_model_rejected" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected stylesheet_raw_model_rejected in dot_issues, got: %+v", got.DOTIssues)
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
