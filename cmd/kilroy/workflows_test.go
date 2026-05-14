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

// legacyToml uses the pre-v2 [[inputs]] shape (top-level name/description/version,
// inputs as an array of tables). Curated `kilroy workflows list` should hide
// these unless --all is passed.
const legacyToml = `
name        = "legacy-stub"
description = "Pre-v2 workflow with [[inputs]] shape."
version     = "1"
outputs = ["result.md"]
[[inputs]]
name        = "x"
description = "x"
required    = true
`

type workflowListTestEntry struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	DefaultClass string `json:"default_class"`
	Schema       string `json:"schema"`
}

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

	// JSON is the default per plan §2.2 — no flag needed.
	cmd := exec.Command(bin, "workflows", "list")
	cmd.Dir = pkgRoot
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("workflows list --json: %v\n%s", err, out)
	}

	var got struct {
		Workflows []workflowListTestEntry `json:"workflows"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	review := workflowListEntryByName(got.Workflows, "review")
	if review == nil {
		t.Fatalf("workflows missing review: %+v", got.Workflows)
	}
	tiny := workflowListEntryByName(got.Workflows, "tiny-investigate")
	if tiny == nil {
		t.Fatalf("workflows missing tiny-investigate: %+v", got.Workflows)
	}
	if review.Schema != "v2" {
		t.Errorf("review schema = %q, want v2", review.Schema)
	}
	if review.DefaultClass != "hard_coding" {
		t.Errorf("review default_class = %q, want hard_coding", review.DefaultClass)
	}
}

func TestTopLevelWorkflowAliases(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "review", v2ReviewToml)

	env := append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "list", args: []string{"list"}, want: `"workflows"`},
		{name: "describe", args: []string{"describe", "review"}, want: `"name": "review"`},
		{name: "check", args: []string{"check", "review"}, want: `"status": "ok"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(bin, tc.args...)
			cmd.Dir = pkgRoot
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("kilroy %s failed: %v\n%s", strings.Join(tc.args, " "), err, out)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Fatalf("output missing %q\n%s", tc.want, out)
			}
		})
	}
}

func TestWorkflowsDescribe_HumanOutput_ShowsAllSections(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "review", v2ReviewToml)

	// --pretty opts into the human view; without it, JSON is default.
	cmd := exec.Command(bin, "workflows", "describe", "review", "--pretty")
	cmd.Dir = pkgRoot
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

	// JSON is the default — no flag needed.
	cmd := exec.Command(bin, "workflows", "describe", "review")
	cmd.Dir = pkgRoot
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

func TestWorkflowsDescribe_SourceBuiltInOutsideRepo(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "workflows", "describe", "implement")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS=",
		"KILROY_PROJECT_ROOT=",
		"XDG_CONFIG_HOME="+t.TempDir(),
		"XDG_DATA_HOME="+t.TempDir(),
		"LOCALAPPDATA=",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("describe source built-in outside repo: %v\n%s", err, out)
	}
	s := string(out)
	for _, want := range []string{`"name": "implement"`, `"schema": "v2"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("output missing %s\nfull:\n%s", want, s)
		}
	}
}

func TestWorkflowsList_SourceBuiltInCuratedSurface(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "workflows", "list")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS=",
		"KILROY_PROJECT_ROOT=",
		"XDG_CONFIG_HOME="+t.TempDir(),
		"XDG_DATA_HOME="+t.TempDir(),
		"LOCALAPPDATA=",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("workflows list source built-ins: %v\n%s", err, out)
	}

	var got struct {
		Workflows []workflowListTestEntry `json:"workflows"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	var names []string
	for _, wf := range got.Workflows {
		names = append(names, wf.Name)
	}
	want := []string{"implement", "plan", "validate"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("curated workflow names = %v, want %v\nfull output:\n%s", names, want, out)
	}
}

func TestPublicWorkflowGraphInputsOnlyRequireSeed(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(filepath.Dir(wd))
	for rel, want := range map[string]string{
		"workflows/plan/graph.dot":      `inputs="goal"`,
		"workflows/implement/graph.dot": `inputs="task_packet"`,
		"workflows/validate/graph.dot":  `inputs="task_packet"`,
	} {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if !strings.Contains(string(raw), want) {
			t.Fatalf("%s must declare only required graph inputs %s; optional manifest inputs should not become launch-required", rel, want)
		}
	}
}

// TestWorkflowsList_DefaultsToV2_HidesLegacy confirms the curated default
// list excludes legacy `[[inputs]]` packages. The packages remain
// reachable via `kilroy run <name>` and via --all.
func TestWorkflowsList_DefaultsToV2_HidesLegacy(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "v2pkg", v2ReviewToml)
	writePackage(t, pkgRoot, "old-stub", legacyToml)

	// Default: only v2.
	cmd := exec.Command(bin, "workflows", "list")
	cmd.Dir = pkgRoot
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("default list: %v\n%s", err, out)
	}
	var got struct {
		Workflows []workflowListTestEntry `json:"workflows"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, out)
	}
	if workflowListEntryByName(got.Workflows, "v2pkg") == nil {
		t.Errorf("default list should show v2pkg, got %+v", got.Workflows)
	}
	if workflowListEntryByName(got.Workflows, "old-stub") != nil {
		t.Errorf("default list should hide legacy old-stub, got %+v", got.Workflows)
	}

	// --all: both.
	cmd = exec.Command(bin, "workflows", "list", "--all")
	cmd.Dir = pkgRoot
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err = cmd.Output()
	if err != nil {
		t.Fatalf("--all list: %v\n%s", err, out)
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, out)
	}
	if workflowListEntryByName(got.Workflows, "v2pkg") == nil || workflowListEntryByName(got.Workflows, "old-stub") == nil {
		t.Errorf("--all list should show v2pkg and old-stub, got %+v", got.Workflows)
	}
}

func workflowListEntryByName(entries []workflowListTestEntry, name string) *workflowListTestEntry {
	for i := range entries {
		if entries[i].Name == name {
			return &entries[i]
		}
	}
	return nil
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

func TestWorkflowsValidate_UsesProjectPolicyOverride(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeGlobalOpenAIAuth(t, tmpHome)

	projectRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(projectRoot, ".kilroy"), 0o755); err != nil {
		t.Fatalf("mkdir .kilroy: %v", err)
	}
	workflowRoot := filepath.Join(projectRoot, ".kilroy", "workflows")
	workflowDir := writePackage(t, workflowRoot, "policy-check", `
[workflow]
name = "policy-check"
version = "1"
description = "Validate project policy override."
default_class = "hard_coding"
graph = "graph.dot"
`)
	graph := `digraph policy_check {
  graph [model_stylesheet="* { agent_class: hard_coding; }"]
  start [shape=Mdiamond]
  agent [shape=box, agent_class="hard_coding", prompt="Check routing."]
  done [shape=Msquare]
  start -> agent
  agent -> done [condition="outcome=success"]
}`
	if err := os.WriteFile(filepath.Join(workflowDir, "graph.dot"), []byte(graph), 0o644); err != nil {
		t.Fatalf("write graph: %v", err)
	}

	prefer := exec.Command(bin, "policy", "prefer", "hard_coding", "gpt-5", "--scope", "project")
	prefer.Dir = projectRoot
	prefer.Env = policyOverrideEnv(tmpHome)
	if out, err := prefer.CombinedOutput(); err != nil {
		t.Fatalf("policy prefer: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, "workflows", "validate", "policy-check")
	cmd.Dir = projectRoot
	cmd.Env = policyOverrideEnv(tmpHome,
		"OPENAI_API_KEY_KILROY=present",
		"KILROY_WORKFLOW_PATHS="+workflowRoot,
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("workflows validate: %v\n%s", err, out)
	}
	var got workflowsValidateResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	if got.PreLaunch == nil || len(got.PreLaunch.Nodes) == 0 {
		t.Fatalf("missing prelaunch nodes: %+v", got.PreLaunch)
	}
	node := got.PreLaunch.Nodes[0]
	if node.ResolvedModel != "gpt-5" || node.ResolvedDriver != "openai_sdk" {
		t.Fatalf("resolved node = %+v, want gpt-5/openai_sdk", node)
	}
	if node.PolicySource != "project_override" || node.OverrideMode != "prefer" {
		t.Fatalf("policy provenance = %q/%q, want project_override/prefer",
			node.PolicySource, node.OverrideMode)
	}
}
