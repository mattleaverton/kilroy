// Regression tests for class-driven routing through TmuxAgentHandler.
// Locks in Block 4 Step 4b: a node with class="hard_coding" must override
// the legacy stylesheet attributes via the policy resolver, and emit both
// policy_class_resolved and provider_selected progress events.
package agents

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/tmux"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// agentTestDetectionView is a binding.DetectionView for tmux handler tests.
type agentTestDetectionView struct {
	envs map[string]bool
	clis map[string]bool
}

func (v agentTestDetectionView) EnvVarPresent(name string) bool { return v.envs[name] }
func (v agentTestDetectionView) CLISessionOK(tool string) bool  { return v.clis[tool] }

// claudeCLIResolverFactory builds a binding.Resolver factory whose detection
// view marks the claude CLI as logged-in.
func claudeCLIResolverFactory() func(string) (*binding.Resolver, error) {
	cfg := &binding.Config{
		Bindings: map[string]string{"anthropic/cli_oauth/claude": "anthropic_claude_cli"},
		Chains: map[string]binding.Chain{
			"anthropic_claude_cli": {
				Name:     "anthropic_claude_cli",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
				Sources:  []binding.Source{{Kind: binding.SourceCLISession, Tool: "claude"}},
			},
		},
	}
	view := agentTestDetectionView{clis: map[string]bool{"claude": true}}
	r := binding.NewResolver(cfg, view)
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// anthropicSDKResolverFactory builds a binding.Resolver factory whose detection
// view marks ANTHROPIC_API_KEY as set in the env.
func anthropicSDKResolverFactory() func(string) (*binding.Resolver, error) {
	cfg := &binding.Config{
		Bindings: map[string]string{"anthropic/api_key": "anthropic_api_key"},
		Chains: map[string]binding.Chain{
			"anthropic_api_key": {
				Name:     "anthropic_api_key",
				Requires: binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
				Sources:  []binding.Source{{Kind: binding.SourceEnvVar, Name: "ANTHROPIC_API_KEY"}},
			},
		},
	}
	view := agentTestDetectionView{envs: map[string]bool{"ANTHROPIC_API_KEY": true}}
	r := binding.NewResolver(cfg, view)
	return func(string) (*binding.Resolver, error) { return r, nil }
}

// claudePolicy returns a stub policy whose hard_coding class resolves to a
// single claude_cli candidate gated on a CLI session for "claude".
func claudePolicy() *policy.Data {
	return &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Description: "test class",
				Chain: []policy.Candidate{
					{
						ModelID:     "claude-opus-4-7",
						Driver:      "claude_cli",
						Transport:   "cli_subprocess",
						HistorySink: "jsonl_local",
						Requires:    binding.Requirement{Provider: "anthropic", Method: binding.MethodCLIOAuth, Tool: "claude"},
					},
				},
			},
		},
	}
}

// readProgressEvents parses progress.ndjson into a slice of decoded events.
func readProgressEvents(t *testing.T, logsRoot string) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join(logsRoot, "progress.ndjson"))
	if err != nil {
		t.Fatalf("open progress.ndjson: %v", err)
	}
	defer f.Close()
	var out []map[string]any
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("unmarshal progress event: %v", err)
		}
		out = append(out, ev)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan progress.ndjson: %v", err)
	}
	return out
}

// TestTmuxAgentHandler_ClassAttribute_OverridesStylesheet is the tmux
// counterpart of agent_router_class_test.go's API-side test. A node with
// class="hard_coding" must drive routing through policy.Resolve regardless
// of the bogus llm_provider/llm_model/agent_tool stylesheet attrs that may
// be present.
func TestTmuxAgentHandler_ClassAttribute_OverridesStylesheet(t *testing.T) {
	scriptDir := t.TempDir()
	// Fake claude binary captures the model arg passed to it so we can
	// assert the policy-resolved value reaches the session, not the
	// stylesheet's bogus value.
	script := filepath.Join(scriptDir, "fake-claude")
	// Echo the model arg so we can read it back from response.md and prove
	// the policy-resolved value reached the session, not the bogus stylesheet.
	scriptContent := `#!/bin/bash
echo "MODEL_ARG=$2"
exit 0
`
	if err := os.WriteFile(script, []byte(scriptContent), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}

	// Register a "claude" template (the tool name produced by toolNameForDriver
	// for driver=claude_cli) backed by the fake script. BuildArgs passes the
	// resolved model as $2 so the script can echo it back, proving the
	// policy-resolved value reached the session.
	reg := templates.DefaultRegistry()
	reg.Register(templates.Template{
		Name:   "claude",
		Binary: script,
		BuildArgs: func(prompt, workDir, modelID, _ string) []string {
			return []string{prompt, modelID}
		},
		BuildEnv: func() map[string]string {
			return nil
		},
		ExitsOnComplete: true,
		StartupTimeout:  5 * time.Second,
	})

	mgr := tmux.NewManager(testSocket)
	defer exec.Command("tmux", "-u", "-L", testSocket, "kill-server").Run()

	handler := &TmuxAgentHandler{
		Tmux:      mgr,
		Templates: reg,
		Timeout:   30 * time.Second,
		PolicyDeps: engine.PolicyDeps{
			Load:     func() (*policy.Data, error) { return claudePolicy(), nil },
			Resolver: claudeCLIResolverFactory(),
		},
	}

	logsRoot := t.TempDir()
	workDir := t.TempDir()

	node := model.NewNode("agent")
	node.Attrs["agent_class"] = "hard_coding"
	// Bogus stylesheet values prove the override is real.
	node.Attrs["llm_provider"] = "bogus_provider"
	node.Attrs["llm_model"] = "bogus-model"
	node.Attrs["agent_tool"] = "bogus_tool"
	node.Attrs["prompt"] = "do work"

	execCtx := &engine.Execution{
		Graph:       model.NewGraph("test"),
		Context:     runtime.NewContext(),
		LogsRoot:    logsRoot,
		WorktreeDir: workDir,
		Engine: &engine.Engine{
			LogsRoot: logsRoot,
			Options:  engine.RunOptions{RunID: "test-class-001"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	outcome, err := handler.Execute(ctx, execCtx, node)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if outcome.Status != runtime.StatusSuccess {
		t.Fatalf("status = %q, want success (reason: %s)", outcome.Status, outcome.FailureReason)
	}

	events := readProgressEvents(t, logsRoot)
	resolvedEv := findEvent(events, "policy_class_resolved")
	if resolvedEv == nil {
		t.Fatalf("expected policy_class_resolved event, got events: %v", eventNames(events))
	}
	if got, want := resolvedEv["class"], "hard_coding"; got != want {
		t.Errorf("policy_class_resolved.class = %v, want %v", got, want)
	}
	if got, want := resolvedEv["model"], "claude-opus-4-7"; got != want {
		t.Errorf("policy_class_resolved.model = %v, want %v", got, want)
	}
	if got, want := resolvedEv["driver"], "claude_cli"; got != want {
		t.Errorf("policy_class_resolved.driver = %v, want %v", got, want)
	}

	providerEv := findEvent(events, "provider_selected")
	if providerEv == nil {
		t.Fatalf("expected provider_selected event, got events: %v", eventNames(events))
	}
	if got, want := providerEv["provider"], "anthropic"; got != want {
		t.Errorf("provider_selected.provider = %v, want %v (bogus stylesheet not overridden)", got, want)
	}
	if got, want := providerEv["model"], "claude-opus-4-7"; got != want {
		t.Errorf("provider_selected.model = %v, want %v (bogus stylesheet not overridden)", got, want)
	}
	if got, want := providerEv["source"], "policy_class:hard_coding"; got != want {
		t.Errorf("provider_selected.source = %v, want %v", got, want)
	}

	sessionEv := findEvent(events, "tmux_session_start")
	if sessionEv == nil {
		t.Fatalf("expected tmux_session_start event, got events: %v", eventNames(events))
	}
	if got, want := sessionEv["tool"], "claude"; got != want {
		t.Errorf("tmux_session_start.tool = %v, want %v (driver→tool override failed)", got, want)
	}

	// Sanity: the fake binary captured the resolved model in its output.
	respPath := filepath.Join(logsRoot, "agent", "response.md")
	resp, err := os.ReadFile(respPath)
	if err != nil {
		t.Fatalf("read response.md: %v", err)
	}
	if !strings.Contains(string(resp), "claude-opus-4-7") {
		t.Fatalf("response = %q, want to contain resolved model claude-opus-4-7", string(resp))
	}
}

// TestTmuxAgentHandler_ClassAttribute_NonCLIDriver_Errors verifies that when
// policy resolves to a non-CLI driver and the tmux handler is somehow
// invoked anyway (the dispatcher should never route that way; this is
// a belt-and-braces check), the handler refuses loudly rather than
// silently using whatever tool the legacy fallback would pick.
func TestTmuxAgentHandler_ClassAttribute_NonCLIDriver_Errors(t *testing.T) {
	apiOnlyPolicy := &policy.Data{
		SchemaVersion: "1",
		PolicyVersion: "test-1.0",
		Classes: map[string]policy.Class{
			"hard_coding": {
				Description: "test class — only api candidate",
				Chain: []policy.Candidate{
					{
						ModelID:   "claude-opus-4-7",
						Driver:    "anthropic_sdk",
						Transport: "http",
						Requires:  binding.Requirement{Provider: "anthropic", Method: binding.MethodAPIKey},
					},
				},
			},
		},
	}

	handler := &TmuxAgentHandler{
		Templates: templates.DefaultRegistry(),
		Timeout:   5 * time.Second,
		PolicyDeps: engine.PolicyDeps{
			Load:     func() (*policy.Data, error) { return apiOnlyPolicy, nil },
			Resolver: anthropicSDKResolverFactory(),
		},
	}

	logsRoot := t.TempDir()
	workDir := t.TempDir()

	node := model.NewNode("agent")
	node.Attrs["agent_class"] = "hard_coding"

	execCtx := &engine.Execution{
		Graph:       model.NewGraph("test"),
		Context:     runtime.NewContext(),
		LogsRoot:    logsRoot,
		WorktreeDir: workDir,
		Engine: &engine.Engine{
			LogsRoot: logsRoot,
			Options:  engine.RunOptions{RunID: "test-class-002"},
		},
	}

	outcome, err := handler.Execute(context.Background(), execCtx, node)
	if err != nil {
		t.Fatalf("Execute returned err: %v", err)
	}
	if outcome.Status != runtime.StatusFail {
		t.Fatalf("status = %q, want fail (non-CLI driver under --tmux)", outcome.Status)
	}
	if !strings.Contains(outcome.FailureReason, "anthropic_sdk") {
		t.Errorf("failure_reason = %q, want mention of resolved driver", outcome.FailureReason)
	}
	if !strings.Contains(outcome.FailureReason, "tmux") {
		t.Errorf("failure_reason = %q, want mention of tmux mode", outcome.FailureReason)
	}
}

// TestToolNameForDriver covers the driver→tmux-tool mapping for all known
// CLI drivers and a representative SDK driver that has no tmux tool.
func TestToolNameForDriver(t *testing.T) {
	tests := []struct {
		driver string
		want   string
	}{
		{"claude_cli", "claude"},
		{"codex_cli", "codex"},
		{"gemini_cli", "gemini"},
		{"anthropic_sdk", ""},
		{"openai_sdk", ""},
		{"google_sdk", ""},
		{"unknown_driver", ""},
	}
	for _, tc := range tests {
		t.Run(tc.driver, func(t *testing.T) {
			if got := toolNameForDriver(tc.driver); got != tc.want {
				t.Errorf("toolNameForDriver(%q) = %q, want %q", tc.driver, got, tc.want)
			}
		})
	}
}

func findEvent(events []map[string]any, eventName string) map[string]any {
	for _, ev := range events {
		if ev["event"] == eventName {
			return ev
		}
	}
	return nil
}

func eventNames(events []map[string]any) []string {
	names := make([]string, 0, len(events))
	for _, ev := range events {
		if name, ok := ev["event"].(string); ok {
			names = append(names, name)
		}
	}
	return names
}
