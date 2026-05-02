// Tests for `kilroy workflows list / describe`.
package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const v2ReviewToml = `
[workflow]
name              = "review"
version           = "1"
description       = "Review a target diff and produce a verdict."
agent_description = "Review a PR/branch/patch."
default_class     = "hard_coding"
graph             = "graph.dot"

[inputs.target]
type        = "string"
required    = true
description = "PR URL, branch ref, or path to a .patch file."

[outputs.result]
type        = "path"
path        = "result.md"
description = "Human-readable review output."

[side_effects]
mutates_git    = false
writes_files   = true
network_egress = false
idempotent     = true

[nodes.agent]
class = "hard_coding"
`

const v2InvestigateToml = `
[workflow]
name              = "tiny-investigate"
version           = "1"
description       = "A small investigate workflow for tests."
default_class     = "deep_investigation"

[inputs.question]
type        = "string"
required    = true
description = "What to ask."
`

func writePackage(t *testing.T, root, name, manifest string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte("digraph "+name+" {}"), 0o644); err != nil {
		t.Fatalf("dot: %v", err)
	}
	return dir
}

func TestWorkflowsList_JSON_FindsAllPackages(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "review", v2ReviewToml)
	writePackage(t, pkgRoot, "tiny-investigate", v2InvestigateToml)

	cmd := exec.Command(bin, "workflows", "list", "--json")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("workflows list --json: %v\n%s", err, out)
	}

	var got struct {
		Workflows []struct {
			Name         string `json:"name"`
			Description  string `json:"description"`
			DefaultClass string `json:"default_class"`
			Schema       string `json:"schema"`
		} `json:"workflows"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if len(got.Workflows) != 2 {
		t.Fatalf("workflows len = %d, want 2: %+v", len(got.Workflows), got.Workflows)
	}
	// Sorted by name: review, tiny-investigate.
	if got.Workflows[0].Name != "review" || got.Workflows[1].Name != "tiny-investigate" {
		t.Errorf("names = %q, %q; want review, tiny-investigate",
			got.Workflows[0].Name, got.Workflows[1].Name)
	}
	if got.Workflows[0].Schema != "v2" {
		t.Errorf("schema = %q, want v2", got.Workflows[0].Schema)
	}
	if got.Workflows[0].DefaultClass != "hard_coding" {
		t.Errorf("default_class = %q, want hard_coding", got.Workflows[0].DefaultClass)
	}
}

func TestWorkflowsDescribe_HumanOutput_ShowsAllSections(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "review", v2ReviewToml)

	cmd := exec.Command(bin, "workflows", "describe", "review")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("describe: %v\n%s", err, out)
	}
	s := string(out)
	for _, want := range []string{
		"name:        review",
		"schema:      v2",
		"default class: hard_coding",
		"agent description:",
		"inputs:",
		"target (REQUIRED)",
		"outputs:",
		"result (path)",
		"side effects:",
		"node overrides:",
		"agent: class=hard_coding",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("describe output missing %q\nfull:\n%s", want, s)
		}
	}
}

func TestWorkflowsDescribe_JSON_HasJSONFieldNames(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "review", v2ReviewToml)

	cmd := exec.Command(bin, "workflows", "describe", "review", "--json")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("describe --json: %v\n%s", err, out)
	}
	s := string(out)
	// Verify json tags are honored — no Go-style PascalCase keys.
	for _, bad := range []string{`"Name"`, `"Type"`, `"Required"`} {
		if strings.Contains(s, bad) {
			t.Errorf("json output contains Go-style key %s; want snake_case\nfull:\n%s", bad, s)
		}
	}
	for _, want := range []string{`"name":`, `"type":`, `"required":`} {
		if !strings.Contains(s, want) {
			t.Errorf("json output missing %s\nfull:\n%s", want, s)
		}
	}
}

func TestWorkflowsDescribe_UnknownName_Exit1(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "workflows", "describe", "no-such-workflow")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+t.TempDir(),
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1")
	}
	if !strings.Contains(stderr.String(), `workflow "no-such-workflow" not found`) {
		t.Errorf("stderr missing not-found message:\n%s", stderr.String())
	}
}
