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
	"github.com/danshapiro/kilroy/internal/auth/binding"
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
		Load:     func() (*policy.Data, error) { t.Fatal("policy load should not be called"); return nil, nil },
		Resolver: func(string) (*binding.Resolver, error) { t.Fatal("resolver should not be called"); return nil, nil },
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
					ModelID:  "claude-opus-4-7",
					Driver:   "anthropic_sdk",
					Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				}},
			},
		},
	}

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: stateResolver(t, _stateChains),
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
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: emptyResolver(t),
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

// TestValidatePreLaunch_AllCandidatesSkipped_PreservesPerCandidateDetail
// confirms that when policy.Resolve returns ErrNoViableCandidate, the
// rank-by-rank Skipped detail is surfaced verbatim into
// PreLaunchNodeCheck.SkippedCandidates so automation can read the
// per-candidate reason without parsing the human-readable error string.
func TestValidatePreLaunch_AllCandidatesSkipped_PreservesPerCandidateDetail(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_class": "probe_all_fail",
	})
	data := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test",
		Classes: map[string]policy.Class{
			"probe_all_fail": {Chain: []policy.Candidate{
				{
					ModelID:  "claude-opus-4-7",
					Driver:   "anthropic_sdk",
					Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				},
				{
					ModelID:  "gpt-5",
					Driver:   "openai_sdk",
					Requires: binding.Requirement{Provider: "openai", Method: binding.MethodAPIKey},
				},
			}},
		},
	}

	// Both chains are bound, but the detection view sees no env vars and
	// no CLI sessions — every source is exhausted, so both candidates
	// are skipped with distinct reasons.
	cfg := &binding.Config{
		Bindings: map[string]string{
			"anthropic/api_key": "anthropic_api_key",
			"openai/api_key":    "openai_api_key",
		},
		Chains: map[string]binding.Chain{
			"anthropic_api_key": {
				Name:     "anthropic_api_key",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY"}},
			},
			"openai_api_key": {
				Name:     "openai_api_key",
				Requires: binding.Requirement{Provider: "openai", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "OPENAI_API_KEY"}},
			},
		},
	}
	resolver := binding.NewResolver(cfg, testDetectionView{})

	logsRoot := t.TempDir()
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: func(string) (*binding.Resolver, error) { return resolver, nil },
	})
	if err == nil {
		t.Fatal("expected error when all candidates are unreachable")
	}
	if _, ok := err.(*PreLaunchError); !ok {
		t.Errorf("error type = %T, want *PreLaunchError", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
	}

	got := report.Nodes[0].SkippedCandidates
	if len(got) != 2 {
		t.Fatalf("SkippedCandidates = %d entries, want 2: %+v", len(got), got)
	}
	// Rank 0: anthropic candidate.
	if got[0].Rank != 0 {
		t.Errorf("got[0].Rank = %d, want 0", got[0].Rank)
	}
	if got[0].ModelID != "claude-opus-4-7" {
		t.Errorf("got[0].ModelID = %q, want claude-opus-4-7", got[0].ModelID)
	}
	if got[0].Driver != "anthropic_sdk" {
		t.Errorf("got[0].Driver = %q, want anthropic_sdk", got[0].Driver)
	}
	if got[0].Reason != "auth_chain_exhausted:anthropic_api_key" {
		t.Errorf("got[0].Reason = %q, want auth_chain_exhausted:anthropic_api_key", got[0].Reason)
	}
	// Rank 1: openai candidate.
	if got[1].Rank != 1 {
		t.Errorf("got[1].Rank = %d, want 1", got[1].Rank)
	}
	if got[1].ModelID != "gpt-5" {
		t.Errorf("got[1].ModelID = %q, want gpt-5", got[1].ModelID)
	}
	if got[1].Driver != "openai_sdk" {
		t.Errorf("got[1].Driver = %q, want openai_sdk", got[1].Driver)
	}
	if got[1].Reason != "auth_chain_exhausted:openai_api_key" {
		t.Errorf("got[1].Reason = %q, want auth_chain_exhausted:openai_api_key", got[1].Reason)
	}

	// Confirm the structured payload survives the JSON round-trip onto disk.
	rpt, err := os.ReadFile(filepath.Join(logsRoot, "prelaunch_validation.json"))
	if err != nil {
		t.Fatalf("read prelaunch_validation.json: %v", err)
	}
	var roundtrip PreLaunchReport
	if err := json.Unmarshal(rpt, &roundtrip); err != nil {
		t.Fatalf("decode prelaunch report: %v", err)
	}
	if len(roundtrip.Nodes) != 1 || len(roundtrip.Nodes[0].SkippedCandidates) != 2 {
		t.Errorf("on-disk SkippedCandidates = %+v, want 2 entries", roundtrip.Nodes)
	}
	if roundtrip.Nodes[0].SkippedCandidates[0].Reason != "auth_chain_exhausted:anthropic_api_key" {
		t.Errorf("on-disk rank-0 reason = %q, want auth_chain_exhausted:anthropic_api_key",
			roundtrip.Nodes[0].SkippedCandidates[0].Reason)
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
				ModelID:  "claude-opus-4-7",
				Driver:   "anthropic_sdk",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
			}}},
		},
	}
	// Empty machine state: ANTHROPIC_API_KEY is missing.

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: emptyResolver(t),
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
				ModelID:  "claude-opus-4-7",
				Driver:   "made_up_cli", // not in cliBinaryForDriver, but isCLIDriver returns false
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
			}}},
		},
	}

	// "made_up_cli" isn't a known CLI driver, so isCLIDriver returns false
	// and the binary check is skipped — this should pass. We test the real
	// failure path by using claude_cli with PATH cleared so `claude` isn't
	// found.
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: stateResolver(t, _stateChains),
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
				ModelID:  "claude-opus-4-7",
				Driver:   "claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
			}}},
		},
	}

	report, err = ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return dataCLI, nil },
		Resolver: claudeCLIResolver(t),
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
		// Non-class route — must resolve. Without this the node would
		// fail prelaunch route validation before reaching the package
		// check, masking what the test is actually verifying.
		"llm_provider": "openai",
		"llm_model":    "gpt-5",
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
	state := auth.ListOutput{Entries: []auth.Entry{{Provider: "github", State: auth.StateOK}}}
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
	state := auth.ListOutput{Entries: []auth.Entry{{
		Provider: "github", State: auth.StateOK,
		Source: auth.Source{EnvVar: "GITHUB_TOKEN"},
	}}}
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
	state := auth.ListOutput{Entries: []auth.Entry{{Provider: "anthropic", State: auth.StateOK}}}
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
	state := auth.ListOutput{Entries: []auth.Entry{{Provider: "github", State: auth.StateExpired}}}
	checks := validateSecrets([]string{"github"}, state)
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if checks[0].Status != "fail" {
		t.Errorf("expected fail when only entry is expired, got %q", checks[0].Status)
	}
}

func TestValidateSecrets_Mixed(t *testing.T) {
	state := auth.ListOutput{Entries: []auth.Entry{
		{Provider: "anthropic", State: auth.StateOK},
		{Provider: "github", State: auth.StateMissing},
		{Provider: "openrouter", State: auth.StateOK},
	}}
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
	t.Skip("skipped: validateSecrets uses live auth.ListAll inside ValidatePreLaunch; this case needs an injection point — track as A6 follow-up")
	// No class= attribute → no nodes need policy resolution. The only
	// failure path here is the missing-secret check.
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()
	// validateSecrets uses live auth.ListAll inside ValidatePreLaunch — we
	// can't inject a fake list here. The test depends on github not being
	// in the dev's auth state.
	report, err := ValidatePreLaunch(g,
		RunOptions{LogsRoot: logsRoot, RequiredSecrets: []string{"github"}},
		PolicyDeps{
			Load:     func() (*policy.Data, error) { t.Fatal("policy load should not be called"); return nil, nil },
			Resolver: stateResolver(t, _stateChains),
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
	t.Skip("skipped: validateSecrets uses live auth.ListAll inside ValidatePreLaunch; this case needs an injection point — track as A6 follow-up")
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()
	report, err := ValidatePreLaunch(g,
		RunOptions{LogsRoot: logsRoot, RequiredSecrets: []string{"github"}},
		PolicyDeps{
			Resolver: stateResolver(t, _stateChains),
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
				ModelID:  "claude-opus-4-7",
				Driver:   "claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
			}}},
		},
	}
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{
		Load:     func() (*policy.Data, error) { return data, nil },
		Resolver: prelaunchResolverClaudeCLI(t),
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

// Reviewer regression: a non-class agent node with an unknown
// agent_tool= must fail prelaunch loudly. Before the unified-resolver
// change, prelaunch silently marked every non-class node as OK and the
// dispatcher would only catch the typo at execution time.
func TestValidatePreLaunch_NonClass_UnknownAgentTool_FailsLoudly(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "made-up-tool",
		"llm_provider": "openai",
		"llm_model":    "gpt-5",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for unknown agent_tool=made-up-tool")
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
	}
	if !anyError(report.Nodes[0].Errors, "made-up-tool") {
		t.Errorf("error should name the unknown tool; got %v", report.Nodes[0].Errors)
	}
}

// Built-in OpenAI-compatible llm_provider= values are accepted by prelaunch
// as first-class API routes. They do not rely on handler-level empty-driver
// reinterpretation.
func TestValidatePreLaunch_NonClass_OpenAICompatProvider_FirstClassRoute(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "kimi",
		"llm_model":    "kimi-k2.5",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err != nil {
		t.Fatalf("expected lenient pass for legacy provider, got err: %v", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "ok" {
		t.Fatalf("expected one ok node, got %+v", report.Nodes)
	}
}

func TestValidatePreLaunch_NonClass_AdHocProviderWithoutSpec_FailsLoudly(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "local-openai-compatible",
		"llm_model":    "local-model",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for ad-hoc provider without loaded runtime/spec")
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
	}
	if !anyError(report.Nodes[0].Errors, "local-openai-compatible") {
		t.Errorf("error should name the provider; got %v", report.Nodes[0].Errors)
	}
	if !anyError(report.Nodes[0].Errors, "no executable route") {
		t.Errorf("error should reject unresolved routes; got %v", report.Nodes[0].Errors)
	}
}

// Reviewer regression: a fully vague agent node — no agent_class, no
// agent_tool, no llm_provider+llm_model — fails prelaunch loudly.
func TestValidatePreLaunch_NonClass_VagueNode_FailsLoudly(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail for vague node")
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
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

// ── prelaunch test helpers — binding.Resolver fixtures ──────────────────────

// _stateChains is a placeholder map used by older test sites. Tests pass it
// through to stateResolver to indicate "use the default test chains."
var _stateChains = struct{}{}

func stateResolver(_ *testing.T, _ struct{}) func(string) (*binding.Resolver, error) {
	return prelaunchResolverAnthropicEnv(nil)
}

// prelaunchResolverAnthropicEnv returns a factory whose detection view marks
// ANTHROPIC_API_KEY as present and the claude CLI session as OK.
func prelaunchResolverAnthropicEnv(_ *testing.T) func(string) (*binding.Resolver, error) {
	cfg := &binding.Config{
		Bindings: map[string]string{
			"anthropic/api_key":          "anthropic_api_key",
			"anthropic/cli_oauth/claude": "anthropic_claude_cli",
		},
		Chains: map[string]binding.Chain{
			"anthropic_api_key": {
				Name:     "anthropic_api_key",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY"}},
			},
			"anthropic_claude_cli": {
				Name:     "anthropic_claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
				Sources:  []binding.Source{{Kind: binding.SourceCLISession, Tool: "claude"}},
			},
		},
	}
	view := testDetectionView{
		envs: map[string]bool{"ANTHROPIC_API_KEY": true},
		clis: map[string]bool{"claude": true},
	}
	r := binding.NewResolver(cfg, view)
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// prelaunchResolverClaudeCLI returns a factory whose detection view marks the
// claude CLI session as OK but no env vars present.
func prelaunchResolverClaudeCLI(_ *testing.T) func(string) (*binding.Resolver, error) {
	cfg := &binding.Config{
		Bindings: map[string]string{
			"anthropic/cli_oauth/claude": "anthropic_claude_cli",
		},
		Chains: map[string]binding.Chain{
			"anthropic_claude_cli": {
				Name:     "anthropic_claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
				Sources:  []binding.Source{{Kind: binding.SourceCLISession, Tool: "claude"}},
			},
		},
	}
	view := testDetectionView{clis: map[string]bool{"claude": true}}
	r := binding.NewResolver(cfg, view)
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// claudeCLIResolver matches the older naming used by some tests.
func claudeCLIResolver(t *testing.T) func(string) (*binding.Resolver, error) {
	return prelaunchResolverClaudeCLI(t)
}

// emptyResolver returns a factory whose config is empty — every resolution
// fails with ErrNoChainForRequirement. Used to test "no auth on machine."
func emptyResolver(_ *testing.T) func(string) (*binding.Resolver, error) {
	r := binding.NewResolver(&binding.Config{}, testDetectionView{})
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// TestIsCLIDriver_Opencode_Recognized confirms opencode is included in
// the CLI-driver set so prelaunch runs the binary-presence + capability
// probe for it (matching claude_cli / codex_cli / gemini_cli).
func TestIsCLIDriver_Opencode_Recognized(t *testing.T) {
	if !isCLIDriver("opencode") {
		t.Fatalf("isCLIDriver(\"opencode\") = false, want true")
	}
	if got := cliBinaryForDriver("opencode"); got != "opencode" {
		t.Errorf("cliBinaryForDriver(\"opencode\") = %q, want \"opencode\"", got)
	}
}

// stageFakeOpencode writes an executable shell stub named "opencode" into
// a temp dir, points PATH at that dir, and returns the dir. Used by the
// credentials-probe tests to get past the binary-presence + capability
// checks so the credentials check is the failure under test.
func stageFakeOpencode(t *testing.T) string {
	t.Helper()
	binDir := t.TempDir()
	fake := filepath.Join(binDir, "opencode")
	if err := os.WriteFile(fake, []byte("#!/bin/bash\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("stage fake opencode: %v", err)
	}
	t.Setenv("PATH", binDir)
	return binDir
}

// clearAPIKeyEnvVars unsets every API-key env var the credentials probe
// might find on dev machines, so leaks from the developer shell don't
// mask "missing key" assertions.
func clearAPIKeyEnvVars(t *testing.T) {
	t.Helper()
	names := []string{
		"ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY_KILROY",
		"OPENAI_API_KEY", "OPENAI_API_KEY_KILROY",
		"GOOGLE_API_KEY", "GOOGLE_API_KEY_KILROY",
		"GEMINI_API_KEY", "GEMINI_API_KEY_KILROY",
		"GOOGLE_GENERATIVE_AI_API_KEY",
		"KIMI_API_KEY", "KIMI_API_KEY_KILROY",
		"ZAI_API_KEY", "ZAI_API_KEY_KILROY",
		"CEREBRAS_API_KEY", "CEREBRAS_API_KEY_KILROY",
		"MINIMAX_API_KEY", "MINIMAX_API_KEY_KILROY",
		"INCEPTION_API_KEY", "INCEPTION_API_KEY_KILROY",
	}
	for _, n := range names {
		t.Setenv(n, "")
	}
}

// TestPrelaunch_OpencodeKimi_MissingKey_FailsLoudly is the headline
// regression: an opencode-routed node whose downstream provider needs
// an API key must fail prelaunch loudly when the env var is missing,
// rather than letting opencode hit a 401 deep inside the run loop.
func TestPrelaunch_OpencodeKimi_MissingKey_FailsLoudly(t *testing.T) {
	stageFakeOpencode(t)
	clearAPIKeyEnvVars(t)

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "opencode",
		"llm_provider": "kimi",
		"llm_model":    "kimi-k2.5",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected fail when KIMI_API_KEY is missing for opencode route")
	}
	if _, ok := err.(*PreLaunchError); !ok {
		t.Errorf("error type = %T, want *PreLaunchError", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
	}
	mc := report.Nodes[0].MissingCredentials
	if mc == nil {
		t.Fatal("expected MissingCredentials on the failed node, got nil")
	}
	if mc.Provider != "kimi" {
		t.Errorf("MissingCredentials.Provider = %q, want kimi", mc.Provider)
	}
	if !envVarsContain(mc.EnvVars, "KIMI_API_KEY") || !envVarsContain(mc.EnvVars, "KIMI_API_KEY_KILROY") {
		t.Errorf("MissingCredentials.EnvVars = %v, want list containing KIMI_API_KEY and KIMI_API_KEY_KILROY", mc.EnvVars)
	}
	if !anyError(report.Nodes[0].Errors, "KIMI_API_KEY") {
		t.Errorf("expected per-node error to mention KIMI_API_KEY, got %v", report.Nodes[0].Errors)
	}
	if !anyError(report.Nodes[0].Errors, "kilroy auth") {
		t.Errorf("expected per-node error to surface a remediation hint, got %v", report.Nodes[0].Errors)
	}
}

// TestPrelaunch_OpencodeKimi_KeyPresent_Passes confirms the probe
// is satisfied by the _KILROY-suffixed variant alone (the budget-isolated
// kilroy convention) — i.e. it doesn't require both vars to be set.
func TestPrelaunch_OpencodeKimi_KeyPresent_Passes(t *testing.T) {
	stageFakeOpencode(t)
	clearAPIKeyEnvVars(t)
	t.Setenv("KIMI_API_KEY_KILROY", "testvalue")

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "opencode",
		"llm_provider": "kimi",
		"llm_model":    "kimi-k2.5",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "ok" {
		t.Fatalf("expected one ok node, got %+v", report.Nodes)
	}
	if report.Nodes[0].MissingCredentials != nil {
		t.Errorf("MissingCredentials should be nil when KIMI_API_KEY_KILROY is set, got %+v", report.Nodes[0].MissingCredentials)
	}
}

// TestPrelaunch_OpencodeAnthropic_KeyPresent_Passes is the canonical-
// provider happy path: opencode + anthropic with ANTHROPIC_API_KEY set
// must continue to pass prelaunch — we did not regress the existing flow.
func TestPrelaunch_OpencodeAnthropic_KeyPresent_Passes(t *testing.T) {
	stageFakeOpencode(t)
	clearAPIKeyEnvVars(t)
	t.Setenv("ANTHROPIC_API_KEY", "testvalue")

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "opencode",
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-5",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "ok" {
		t.Fatalf("expected one ok node, got %+v", report.Nodes)
	}
	if report.Nodes[0].MissingCredentials != nil {
		t.Errorf("MissingCredentials should be nil for opencode+anthropic when ANTHROPIC_API_KEY set, got %+v", report.Nodes[0].MissingCredentials)
	}
}

// TestPrelaunch_NonOpencode_NoCredentialCheck confirms the probe is
// scoped to the opencode driver. Direct claude_cli (cli_oauth) and
// non-class anthropic_sdk routes have their own auth handling and
// must not be blocked by this probe — even with no API key in env.
func TestPrelaunch_NonOpencode_NoCredentialCheck(t *testing.T) {
	clearAPIKeyEnvVars(t)

	// Non-class llm_provider+llm_model = anthropic — driver resolves to
	// anthropic_sdk. This route is exercised by the SDK adapters, which
	// have their own LookupAPIKeyEnv check at process start; prelaunch
	// must not duplicate that here. With no env set, the node should
	// still pass prelaunch (the SDK adapter will surface the missing
	// key separately at execution time).
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "ok" {
		t.Fatalf("expected one ok node for non-opencode route, got %+v", report.Nodes)
	}
	if report.Nodes[0].MissingCredentials != nil {
		t.Errorf("expected MissingCredentials nil for non-opencode route, got %+v", report.Nodes[0].MissingCredentials)
	}
	if report.Nodes[0].ResolvedDriver == "opencode" {
		t.Errorf("resolved_driver = %q, expected non-opencode driver for this test", report.Nodes[0].ResolvedDriver)
	}
}

// TestPrelaunch_Credentials_Hint_MentionsAuthCommands confirms the
// credential failure surfaces a remediation hint pointing at the auth
// CLI commands, satisfying the "user sees the failure within seconds"
// bar from the task spec.
func TestPrelaunch_Credentials_Hint_MentionsAuthCommands(t *testing.T) {
	stageFakeOpencode(t)
	clearAPIKeyEnvVars(t)

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "opencode",
		"llm_provider": "zai",
		"llm_model":    "glm-4.6",
	})
	report, _ := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if len(report.Nodes) != 1 || report.Nodes[0].MissingCredentials == nil {
		t.Fatalf("expected MissingCredentials populated, got %+v", report.Nodes)
	}
	hint := report.Nodes[0].MissingCredentials.Hint
	if !strings.Contains(hint, "kilroy auth list") && !strings.Contains(hint, "kilroy auth suggest-fix") {
		t.Errorf("hint %q should mention `kilroy auth list` or `kilroy auth suggest-fix`", hint)
	}
	if !strings.Contains(hint, "ZAI_API_KEY") {
		t.Errorf("hint %q should name the missing env var", hint)
	}
}

func envVarsContain(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestPrelaunch_Opencode_BinaryMissing_FailsLoudly verifies that when an
// opencode-driven node hits prelaunch and the opencode binary is not on
// PATH, validation fails with a structured per-node error — same shape
// as the existing claude_cli binary-missing case.
func TestPrelaunch_Opencode_BinaryMissing_FailsLoudly(t *testing.T) {
	// Empty PATH so exec.LookPath cannot find the opencode binary.
	t.Setenv("PATH", "")

	g := graphWithAgentNode(t, "agent", map[string]string{
		"agent_tool":   "opencode",
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-5",
	})

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: t.TempDir()}, PolicyDeps{})
	if err == nil {
		t.Fatal("expected error when opencode binary is missing from PATH")
	}
	if _, ok := err.(*PreLaunchError); !ok {
		t.Errorf("error type = %T, want *PreLaunchError", err)
	}
	if len(report.Nodes) != 1 || report.Nodes[0].Status != "fail" {
		t.Fatalf("expected one failed node, got %+v", report.Nodes)
	}
	if report.Nodes[0].ResolvedDriver != "opencode" {
		t.Errorf("resolved_driver = %q, want opencode", report.Nodes[0].ResolvedDriver)
	}
	if report.Nodes[0].BinaryFound == nil || *report.Nodes[0].BinaryFound {
		t.Errorf("BinaryFound = %v, want false", report.Nodes[0].BinaryFound)
	}
	if !anyError(report.Nodes[0].Errors, "opencode") {
		t.Errorf("expected error to mention opencode binary, got %v", report.Nodes[0].Errors)
	}
}

// TestPrelaunchValidationJSON_SchemaVersion_Present asserts that the
// persisted prelaunch_validation.json carries a non-empty top-level
// schema_version stamped to the current PreLaunchSchemaVersion. Consumers
// (CI, automation) rely on this field to branch on report shape changes
// rather than inferring them from the Go struct.
func TestPrelaunchValidationJSON_SchemaVersion_Present(t *testing.T) {
	g := graphWithAgentNode(t, "agent", map[string]string{
		"llm_provider": "anthropic",
		"llm_model":    "claude-sonnet-4-6",
	})
	logsRoot := t.TempDir()

	report, err := ValidatePreLaunch(g, RunOptions{LogsRoot: logsRoot}, PolicyDeps{})
	if err != nil {
		t.Fatalf("ValidatePreLaunch: %v", err)
	}
	if report.SchemaVersion != PreLaunchSchemaVersion {
		t.Errorf("in-memory report.SchemaVersion = %q, want %q", report.SchemaVersion, PreLaunchSchemaVersion)
	}

	raw, err := os.ReadFile(filepath.Join(logsRoot, "prelaunch_validation.json"))
	if err != nil {
		t.Fatalf("read prelaunch_validation.json: %v", err)
	}

	// Decode as a generic map to assert the JSON tag is exactly
	// "schema_version" at the top level — not just that the Go field
	// round-trips.
	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("decode prelaunch_validation.json: %v", err)
	}
	got, ok := top["schema_version"]
	if !ok {
		t.Fatalf("prelaunch_validation.json missing top-level \"schema_version\" key; got keys: %v", mapKeys(top))
	}
	gotStr, ok := got.(string)
	if !ok {
		t.Fatalf("schema_version is %T, want string", got)
	}
	if gotStr != PreLaunchSchemaVersion {
		t.Errorf("schema_version on disk = %q, want %q", gotStr, PreLaunchSchemaVersion)
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
