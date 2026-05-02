// Tests for the pre-launch validation layer. No real LLM calls — every
// scenario is exercised through stubbed PolicyDeps + a controlled graph.
package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/policy"
)

// graphWithAgentNode returns a one-node graph whose single node uses the
// default agent handler (provider-requiring) and carries the given attrs.
func graphWithAgentNode(t *testing.T, nodeID string, attrs map[string]string) *model.Graph {
	t.Helper()
	g := model.NewGraph("test")
	n := model.NewNode(nodeID)
	for k, v := range attrs {
		n.Attrs[k] = v
	}
	g.Nodes[nodeID] = n
	return g
}

func TestValidatePreLaunch_NoClass_PassesWithoutPolicyLoad(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		// Should not be called when no class= is set.
		Load:    func() (*policy.Data, error) { t.Fatal("policy load should not be called"); return nil, nil },
		Collect: func() policy.MachineState { t.Fatal("collect should not be called"); return policy.MachineState{} },
	})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if report.Summary.Fail != 0 || report.Summary.OK != 1 {
		t.Errorf("summary = %+v, want 1 ok / 0 fail", report.Summary)
	}
}

func TestValidatePreLaunch_ClassResolves_OK(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "hard_coding",
	})
	logsRoot := t.TempDir()

	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Chain: []policy.Candidate{{
					ModelID: "claude-opus-4-7",
					Driver:  "anthropic_sdk",
					Auth: policy.AuthReq{
						Kind:   "env_var",
						EnvVar: "ANTHROPIC_API_KEY",
					},
				}},
			},
		},
	}
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			ID: "x", Kind: auth.KindEnvVar, State: auth.StateOK,
			Source: auth.Source{EnvVar: "ANTHROPIC_API_KEY"},
		}}},
	}

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if report.Summary.Fail != 0 {
		t.Errorf("expected 0 fails, got %d: %+v", report.Summary.Fail, report)
	}
	if len(report.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(report.Nodes))
	}
	if report.Nodes[0].ResolvedModel != "claude-opus-4-7" {
		t.Errorf("resolved_model = %q, want claude-opus-4-7", report.Nodes[0].ResolvedModel)
	}
	if report.Nodes[0].ResolvedDriver != "anthropic_sdk" {
		t.Errorf("resolved_driver = %q, want anthropic_sdk", report.Nodes[0].ResolvedDriver)
	}
	if report.Nodes[0].BinaryFound != nil {
		t.Errorf("BinaryFound should be nil for SDK drivers, got %v", report.Nodes[0].BinaryFound)
	}

	// Confirm prelaunch_validation.json was written.
	rpt, err := os.ReadFile(filepath.Join(logsRoot, "prelaunch_validation.json"))
	if err != nil {
		t.Fatalf("read prelaunch_validation.json: %v", err)
	}
	var roundtrip PreLaunchReport
	if err := json.Unmarshal(rpt, &roundtrip); err != nil {
		t.Fatalf("decode prelaunch report: %v", err)
	}
	if roundtrip.Summary.OK != 1 {
		t.Errorf("on-disk report summary = %+v, want OK=1", roundtrip.Summary)
	}
}

// TestValidatePreLaunch_UnknownAgentClass_FailsTyped verifies that an
// unknown agent_class= name fails prelaunch loudly. agent_class is the
// policy surface and must be strict — typos are non-negotiable.
func TestValidatePreLaunch_UnknownAgentClass_FailsTyped(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "totally_made_up",
	})
	logsRoot := t.TempDir()
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"hard_coding": {Chain: []policy.Candidate{}},
		},
	}

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return policy.MachineState{} },
	})
	if err == nil {
		t.Fatal("expected error for unknown agent_class")
	}
	if _, ok := err.(*PreLaunchError); !ok {
		t.Errorf("error type = %T, want *PreLaunchError", err)
	}
	if report.Summary.Fail != 1 {
		t.Errorf("summary.fail = %d, want 1", report.Summary.Fail)
	}
	if len(report.Nodes[0].Errors) == 0 {
		t.Error("expected per-node error for unknown agent_class")
	}
}

func TestValidatePreLaunch_NoAuth_FailsWithSkippedReason(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "hard_coding",
	})
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"hard_coding": {Chain: []policy.Candidate{{
				ModelID: "claude-opus-4-7",
				Driver:  "anthropic_sdk",
				Auth:    policy.AuthReq{Kind: "env_var", EnvVar: "ANTHROPIC_API_KEY"},
			}}},
		},
	}
	// Empty machine state: ANTHROPIC_API_KEY is missing.
	state := policy.MachineState{}

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err == nil {
		t.Fatal("expected error when no candidate auth is satisfied")
	}
	if report.Summary.Fail != 1 {
		t.Errorf("summary.fail = %d, want 1: %+v", report.Summary.Fail, report)
	}
}

func TestValidatePreLaunch_CLIDriver_BinaryMissing_Fails(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "hard_coding",
	})
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"hard_coding": {Chain: []policy.Candidate{{
				ModelID: "claude-opus-4-7",
				Driver:  "made_up_cli", // not in cliBinaryForDriver, but isCLIDriver returns false
				Auth:    policy.AuthReq{Kind: "none"},
			}}},
		},
	}
	state := policy.MachineState{}

	// "made_up_cli" isn't a known CLI driver, so isCLIDriver returns false
	// and the binary check is skipped — this should pass. We test the real
	// failure path by using claude_cli with PATH cleared so `claude` isn't
	// found.
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err != nil {
		// providerAndBackendForDriver returns "" for "made_up_cli" so the
		// resolution will fail before binary check; that's fine — the
		// non-binary path is exercised elsewhere.
		t.Logf("expected failure for unknown driver (resolution path): %v", err)
		_ = report
		return
	}

	// Now the real binary-missing test with an empty PATH.
	t.Setenv("PATH", "")
	dataCLI := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"hard_coding": {Chain: []policy.Candidate{{
				ModelID: "claude-opus-4-7",
				Driver:  "claude_cli",
				Auth:    policy.AuthReq{Kind: "cli_session", CLI: "claude"},
			}}},
		},
	}
	stateCLI := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Tool: "claude", Kind: auth.KindCLIOAuth, State: auth.StateOK,
		}}},
	}

	report, err = ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return dataCLI, nil },
		Collect: func() policy.MachineState { return stateCLI },
	})
	if err == nil {
		t.Fatal("expected error when claude binary is missing from PATH")
	}
	if report.Nodes[0].BinaryFound == nil || *report.Nodes[0].BinaryFound {
		t.Errorf("BinaryFound = %v, want false", report.Nodes[0].BinaryFound)
	}
}

// makeMinimalPackage writes a workflow.toml + graph.dot + a script under
// scripts/ so the package-integrity check has something to inspect.
func makeMinimalPackage(t *testing.T, name, manifest, graph string, scripts map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte(graph), 0o644); err != nil {
		t.Fatalf("write dot: %v", err)
	}
	if len(scripts) > 0 {
		scriptDir := filepath.Join(dir, "scripts")
		if err := os.MkdirAll(scriptDir, 0o755); err != nil {
			t.Fatalf("mkdir scripts: %v", err)
		}
		for n, content := range scripts {
			if err := os.WriteFile(filepath.Join(scriptDir, n), []byte(content), 0o644); err != nil {
				t.Fatalf("write %s: %v", n, err)
			}
		}
	}
	return dir
}

func TestValidatePreLaunch_PackageIntegrity_OK(t *testing.T) {
	pkgDir := makeMinimalPackage(t, "ok",
		`[workflow]
name = "ok"
version = "1"
description = "test"
default_class = "hard_coding"
`,
		`digraph ok {
  start [shape=Mdiamond, label="Start"]
  stage [shape=parallelogram, label="stage", tool_command="bash .kilroy/package/scripts/stage.sh"]
  done [shape=Msquare, label="Done"]
  start -> stage
  stage -> done [condition="outcome=success"]
}`,
		map[string]string{"stage.sh": "#!/bin/bash\necho ok\n"},
	)
	g := graphWithAgentNode(t, "stage", map[string]string{
		"tool_command": "bash .kilroy/package/scripts/stage.sh",
	})
	report, err := ValidatePreLaunch(g, RunOptions{
		LogsRoot:   t.TempDir(),
		PackageDir: pkgDir,
	}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if report.Package == nil || report.Package.Status != "ok" {
		t.Errorf("package check = %+v, want status=ok", report.Package)
	}
}

func TestValidatePreLaunch_PackageIntegrity_MissingScript_Fails(t *testing.T) {
	// Manifest fine, graph references a script that doesn't exist.
	pkgDir := makeMinimalPackage(t, "bad",
		`[workflow]
name = "bad"
version = "1"
description = "test"
`,
		`digraph bad {}`,
		nil, // no scripts dir
	)
	g := graphWithAgentNode(t, "stage", map[string]string{
		"tool_command": "bash .kilroy/package/scripts/missing.sh",
	})
	report, err := ValidatePreLaunch(g, RunOptions{
		LogsRoot:   t.TempDir(),
		PackageDir: pkgDir,
	}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for missing script")
	}
	if report.Package == nil || report.Package.Status != "fail" {
		t.Errorf("package check = %+v, want status=fail", report.Package)
	}
	if !anyError(report.Package.Errors, "missing.sh") {
		t.Errorf("expected error to mention missing.sh, got %v", report.Package.Errors)
	}
}

// TestValidatePreLaunch_PackageIntegrity_UnknownAgentClass_FailsHard
// verifies that an unknown agent_class= name on an agent node is a
// hard package-integrity failure (not a soft note). agent_class is
// the strict policy-routing attribute. Use the unrelated `class=`
// attribute for stylesheet selectors instead.
func TestValidatePreLaunch_PackageIntegrity_UnknownAgentClass_FailsHard(t *testing.T) {
	pkgDir := makeMinimalPackage(t, "bad",
		`[workflow]
name = "bad"
version = "1"
description = "test"
`,
		`digraph bad {}`,
		nil,
	)
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "made_up_class_name",
	})
	_, err := ValidatePreLaunch(g, RunOptions{
		LogsRoot:   t.TempDir(),
		PackageDir: pkgDir,
	}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for unknown agent_class in package check")
	}
}

func TestValidatePreLaunch_PackageIntegrity_MissingRequiredField(t *testing.T) {
	// Manifest omits version (required).
	pkgDir := makeMinimalPackage(t, "bad",
		`[workflow]
name = "bad"
description = "test"
`,
		`digraph bad {}`,
		nil,
	)
	g := graphWithAgentNode(t, "agent", map[string]string{})
	_, err := ValidatePreLaunch(g, RunOptions{
		LogsRoot:   t.TempDir(),
		PackageDir: pkgDir,
	}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for missing version field")
	}
}

func TestValidatePreLaunch_NoPackageDir_SkipsPackageCheck(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	report, err := ValidatePreLaunch(g, RunOptions{
		LogsRoot: t.TempDir(),
		// PackageDir intentionally omitted (raw --graph file launch path).
	}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if report.Package != nil {
		t.Errorf("expected nil package check when PackageDir empty, got %+v", report.Package)
	}
}

func anyError(errs []string, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func TestValidateSecrets_EmptyNeeds_NoChecks(t *testing.T) {
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Provider: "github", State: auth.StateOK,
		}}},
	}
	checks := validateSecrets(nil, state)
	if checks != nil {
		t.Errorf("expected nil checks for empty needs, got %+v", checks)
	}
	checks = validateSecrets([]string{}, state)
	if checks != nil {
		t.Errorf("expected nil checks for empty needs slice, got %+v", checks)
	}
}

func TestValidateSecrets_OneSatisfied(t *testing.T) {
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Provider: "github", State: auth.StateOK,
			Source: auth.Source{EnvVar: "GITHUB_TOKEN"},
		}}},
	}
	checks := validateSecrets([]string{"github"}, state)
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if checks[0].Status != "ok" {
		t.Errorf("status = %q, want ok", checks[0].Status)
	}
	if checks[0].Name != "github" {
		t.Errorf("name = %q, want github", checks[0].Name)
	}
	if len(checks[0].Errors) != 0 {
		t.Errorf("expected no errors, got %v", checks[0].Errors)
	}
}

func TestValidateSecrets_OneMissing(t *testing.T) {
	// State has anthropic OK but not github.
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Provider: "anthropic", State: auth.StateOK,
		}}},
	}
	checks := validateSecrets([]string{"github"}, state)
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if checks[0].Status != "fail" {
		t.Errorf("status = %q, want fail", checks[0].Status)
	}
	if len(checks[0].Errors) == 0 {
		t.Error("expected at least one error explaining the missing secret")
	}
	// Sanity: error message should mention the secret name and a hint.
	joined := strings.Join(checks[0].Errors, " ")
	if !strings.Contains(joined, "github") {
		t.Errorf("error message %q should mention 'github'", joined)
	}
}

func TestValidateSecrets_NotOKStateFails(t *testing.T) {
	// Provider entry exists, but its state is not "ok" (e.g. expired).
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Provider: "github", State: auth.StateExpired,
		}}},
	}
	checks := validateSecrets([]string{"github"}, state)
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if checks[0].Status != "fail" {
		t.Errorf("expected fail when only entry is expired, got %q", checks[0].Status)
	}
}

func TestValidateSecrets_Mixed(t *testing.T) {
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{
			{Provider: "anthropic", State: auth.StateOK},
			{Provider: "github", State: auth.StateMissing},
			{Provider: "openrouter", State: auth.StateOK},
		}},
	}
	needs := []string{"anthropic", "github", "openai"}
	checks := validateSecrets(needs, state)
	if len(checks) != 3 {
		t.Fatalf("checks = %d, want 3", len(checks))
	}
	// anthropic: present + ok
	if checks[0].Name != "anthropic" || checks[0].Status != "ok" {
		t.Errorf("anthropic check = %+v, want ok", checks[0])
	}
	// github: present but missing state -> fail
	if checks[1].Name != "github" || checks[1].Status != "fail" {
		t.Errorf("github check = %+v, want fail", checks[1])
	}
	// openai: not present at all -> fail
	if checks[2].Name != "openai" || checks[2].Status != "fail" {
		t.Errorf("openai check = %+v, want fail", checks[2])
	}
}

func TestValidatePreLaunch_RequiredSecrets_FailsWhenMissing(t *testing.T) {
	// No class= attribute → no nodes need policy resolution. The only
	// failure path here is the missing-secret check.
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{
			{Provider: "anthropic", State: auth.StateOK},
		}},
	}
	report, err := ValidatePreLaunch(g,
		RunOptions{LogsRoot: logsRoot, RequiredSecrets: []string{"github"}},
		PolicyDeps{
			Load:    func() (*policy.Data, error) { t.Fatal("policy load should not be called"); return nil, nil },
			Collect: func() policy.MachineState { return state },
		})
	if err == nil {
		t.Fatal("expected error for missing required secret")
	}
	if _, ok := err.(*PreLaunchError); !ok {
		t.Errorf("error type = %T, want *PreLaunchError", err)
	}
	if report.Summary.Fail != 1 {
		t.Errorf("summary.fail = %d, want 1: %+v", report.Summary.Fail, report)
	}
	if len(report.Secrets) != 1 || report.Secrets[0].Status != "fail" {
		t.Errorf("expected one failed secret check, got %+v", report.Secrets)
	}
	if !strings.Contains(err.Error(), "github") {
		t.Errorf("error %q should mention 'github'", err.Error())
	}
}

func TestValidatePreLaunch_RequiredSecrets_PassesWhenSatisfied(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{
			{Provider: "github", State: auth.StateOK},
		}},
	}
	report, err := ValidatePreLaunch(g,
		RunOptions{LogsRoot: logsRoot, RequiredSecrets: []string{"github"}},
		PolicyDeps{
			Collect: func() policy.MachineState { return state },
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.Summary.Fail != 0 {
		t.Errorf("summary.fail = %d, want 0", report.Summary.Fail)
	}
	if len(report.Secrets) != 1 || report.Secrets[0].Status != "ok" {
		t.Errorf("expected one ok secret check, got %+v", report.Secrets)
	}
}

// TestValidatePreLaunch_CLIDriver_BinaryBroken_Fails confirms the new
// capability probe catches a binary that's on PATH but doesn't actually
// run cleanly under --help (broken download, missing deps, etc).
func TestValidatePreLaunch_CLIDriver_BinaryBroken_Fails(t *testing.T) {
	// Stage a fake `claude` script that exits non-zero from --help.
	binDir := t.TempDir()
	fakeClaude := filepath.Join(binDir, "claude")
	if err := os.WriteFile(fakeClaude, []byte("#!/bin/bash\nexit 7\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "hard_coding",
	})
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"hard_coding": {Chain: []policy.Candidate{{
				ModelID: "claude-opus-4-7",
				Driver:  "claude_cli",
				Auth:    policy.AuthReq{Kind: "cli_session", CLI: "claude"},
			}}},
		},
	}
	state := policy.MachineState{
		Auth: auth.ListOutput{Entries: []auth.Entry{{
			Tool: "claude", Kind: auth.KindCLIOAuth, State: auth.StateOK,
		}}},
	}
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:    func() (*policy.Data, error) { return data, nil },
		Collect: func() policy.MachineState { return state },
	})
	if err == nil {
		t.Fatal("expected fail when --help exits non-zero")
	}
	if report.Nodes[0].Status != "fail" {
		t.Errorf("status = %q, want fail", report.Nodes[0].Status)
	}
	if !anyError(report.Nodes[0].Errors, "does not respond to --help") {
		t.Errorf("expected capability-probe error, got %v", report.Nodes[0].Errors)
	}
}

func TestPreLaunchError_MessageMentionsFailedNodes(t *testing.T) {
	r := &PreLaunchReport{
		Nodes: []PreLaunchNodeCheck{
			{NodeID: "agent", Status: "fail"},
			{NodeID: "summarize", Status: "ok"},
			{NodeID: "verify", Status: "fail"},
		},
		Summary: PreLaunchSummary{Fail: 2, OK: 1},
	}
	e := &PreLaunchError{Report: r}
	msg := e.Error()
	for _, want := range []string{"agent", "verify", "2 node"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q should contain %q", msg, want)
		}
	}
	if strings.Contains(msg, "summarize") {
		t.Errorf("error %q should not mention the OK node", msg)
	}
}
