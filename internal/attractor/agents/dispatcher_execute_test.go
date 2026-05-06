// Acceptance tests for the Dispatcher.Execute path: verifies that
// the resolved driver routes to the right sub-handler. Tests 1, 2, 3
// from the v2 P0 architecture letter — class/DOT intent determines
// which path runs, never a CLI flag.
package agents

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// recordingHandler is a stub that captures whether it was invoked and
// returns a configurable outcome.
type recordingHandler struct {
	label         string
	called        bool
	sessionCalled bool
	route         engine.AgentRoute
	outcome       runtime.Outcome
}

func (r *recordingHandler) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
	r.called = true
	r.route = route
	return r.outcome, nil
}

func (r *recordingHandler) ExecuteAgentWithSession(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute, tmpl *templates.Template, toolName string, prompt string, modelID string, cfg transport.SessionConfig) (runtime.Outcome, error) {
	r.sessionCalled = true
	r.route = route
	return r.outcome, nil
}

// Test 1: an agent_tool=claude node (CLI driver) routes to the tmux
// path; the codergen path is not called.
func TestDispatcher_ExecuteRoutesAgentToolToTmux(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "tmux fired"}}
	apiRunner := &testEngineAgentBackend{response: "api should not fire", outcome: &runtime.Outcome{Status: runtime.StatusFail, Notes: "api fired"}}
	d := &Dispatcher{Tmux: tmux}

	node := &model.Node{
		ID:    "claude_write",
		Attrs: map[string]string{"agent_tool": "claude", "auto_status": "true"},
	}
	execCtx := dispatcherTestExecution(t, node.ID, apiRunner)
	out, err := d.Execute(context.Background(), execCtx, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tmux.sessionCalled {
		t.Fatalf("expected tmux session handler to be invoked")
	}
	if apiRunner.called {
		t.Fatalf("API runner must not be invoked for CLI driver")
	}
	if out.Notes != "tmux fired" {
		t.Fatalf("outcome from wrong path: %+v", out)
	}
}

// Test 2: an llm_provider=openai + llm_model node (SDK driver) routes
// to the codergen path; the tmux path is not called.
func TestDispatcher_ExecuteRoutesSDKToCodergen(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusFail, Notes: "tmux fired"}}
	apiRunner := &testEngineAgentBackend{
		response: "api response",
		outcome:  &runtime.Outcome{Status: runtime.StatusSuccess, Notes: "api runner fired"},
	}
	d := &Dispatcher{Tmux: tmux}

	node := &model.Node{
		ID: "implement",
		Attrs: map[string]string{
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
			"auto_status":  "true",
		},
	}
	execCtx := dispatcherTestExecution(t, node.ID, apiRunner)
	out, err := d.Execute(context.Background(), execCtx, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmux.called || tmux.sessionCalled {
		t.Fatalf("tmux handler must not be invoked for SDK driver")
	}
	if !apiRunner.called {
		t.Fatalf("expected API runner to be invoked")
	}
	if out.Notes != "api runner fired" {
		t.Fatalf("outcome from wrong path: %+v", out)
	}
	if apiRunner.route.Driver != "openai_sdk" {
		t.Fatalf("API runner route driver = %q, want openai_sdk", apiRunner.route.Driver)
	}
	if apiRunner.route.Backend != engine.BackendAPI {
		t.Fatalf("API runner route backend = %q, want api", apiRunner.route.Backend)
	}
}

// Test 3: a mixed-intent invocation — call dispatch twice with
// different drivers using the same Dispatcher. The first call routes
// to one path; the second to the other. State (the .called flag) is
// per-handler, so we can confirm both paths fired.
func TestDispatcher_ExecuteMixedRouting(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "tmux"}}
	apiRunner := &testEngineAgentBackend{response: "api", outcome: &runtime.Outcome{Status: runtime.StatusSuccess, Notes: "api"}}
	d := &Dispatcher{Tmux: tmux}

	cliNode := &model.Node{ID: "n1", Attrs: map[string]string{"agent_tool": "claude", "auto_status": "true"}}
	apiNode := &model.Node{ID: "n2", Attrs: map[string]string{"llm_provider": "anthropic", "llm_model": "claude-sonnet-4-6", "auto_status": "true"}}
	execCtx := dispatcherTestExecution(t, cliNode.ID, apiRunner)

	if _, err := d.Execute(context.Background(), execCtx, cliNode); err != nil {
		t.Fatalf("CLI dispatch err: %v", err)
	}
	ensureStageDir(t, execCtx.LogsRoot, apiNode.ID)
	if _, err := d.Execute(context.Background(), execCtx, apiNode); err != nil {
		t.Fatalf("API dispatch err: %v", err)
	}

	if !tmux.sessionCalled {
		t.Fatalf("CLI node should have invoked tmux handler")
	}
	if !apiRunner.called {
		t.Fatalf("API node should have invoked API runner")
	}
}

// Test 5 (reviewer regression): an llm_provider=<known OpenAI-compatible>
// node routes through an explicit API driver. Empty Driver="" is not a
// dispatch contract; known custom providers should arrive at codergen with a
// first-class route.
func TestDispatcher_ExecuteCustomProvider_DelegatesToCodergen(t *testing.T) {
	tmux := &recordingHandler{label: "tmux"}
	apiRunner := &testEngineAgentBackend{
		response: "custom provider response",
		outcome:  &runtime.Outcome{Status: runtime.StatusSuccess, Notes: "api handled custom"},
	}
	d := &Dispatcher{Tmux: tmux}

	node := &model.Node{
		ID: "minimax_call",
		Attrs: map[string]string{
			"llm_provider": "minimax",
			"llm_model":    "minimax-m2.5",
			"auto_status":  "true",
		},
	}
	execCtx := dispatcherTestExecution(t, node.ID, apiRunner)
	out, err := d.Execute(context.Background(), execCtx, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmux.called || tmux.sessionCalled {
		t.Fatalf("tmux handler must not be invoked for custom provider")
	}
	if !apiRunner.called {
		t.Fatalf("API runner must be invoked for custom provider (got out=%+v)", out)
	}
	if out.Status != runtime.StatusSuccess {
		t.Fatalf("expected delegated codergen success; got %+v", out)
	}
	if apiRunner.route.Driver != "openai_compat_api" {
		t.Fatalf("custom provider route driver = %q, want openai_compat_api", apiRunner.route.Driver)
	}
	if apiRunner.route.Provider != "minimax" {
		t.Fatalf("custom provider route provider = %q, want minimax", apiRunner.route.Provider)
	}
	if apiRunner.route.Backend != engine.BackendAPI {
		t.Fatalf("custom provider route backend = %q, want api", apiRunner.route.Backend)
	}
}

// Test 4: a node with no resolvable driver fails deterministically at
// dispatch time, with neither sub-handler invoked.
func TestDispatcher_ExecuteVagueNode_DeterministicFailure(t *testing.T) {
	tmux := &recordingHandler{label: "tmux"}
	d := &Dispatcher{Tmux: tmux}

	node := &model.Node{ID: "vague", Attrs: map[string]string{}}
	out, err := d.Execute(context.Background(), nil, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != runtime.StatusFail {
		t.Fatalf("vague node should fail; got status %q", out.Status)
	}
	if out.Meta["failure_class"] != "deterministic" {
		t.Fatalf("expected deterministic failure_class; got %v", out.Meta)
	}
	if tmux.called {
		t.Fatalf("tmux handler should not be invoked for vague node")
	}
}

func dispatcherTestExecution(t *testing.T, nodeID string, backend engine.AgentBackend) *engine.Execution {
	t.Helper()
	logsRoot := t.TempDir()
	worktree := t.TempDir()
	ensureStageDir(t, logsRoot, nodeID)
	eng := &engine.Engine{
		Options:      engine.RunOptions{RunID: "dispatcher-test"},
		LogsRoot:     logsRoot,
		WorktreeDir:  worktree,
		AgentBackend: backend,
	}
	return &engine.Execution{
		LogsRoot:    logsRoot,
		WorktreeDir: worktree,
		Engine:      eng,
		Context:     runtime.NewContext(),
	}
}

func ensureStageDir(t *testing.T, logsRoot string, nodeID string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(logsRoot, nodeID), 0o755); err != nil {
		t.Fatalf("mkdir stage dir: %v", err)
	}
}

func TestDispatcher_ExecuteAgentEmptyDriver_DeterministicFailure(t *testing.T) {
	tmux := &recordingHandler{label: "tmux"}
	d := &Dispatcher{Tmux: tmux}

	node := &model.Node{
		ID: "adhoc",
		Attrs: map[string]string{
			"llm_provider": "local-openai-compatible",
			"llm_model":    "local-model",
		},
	}
	out, err := d.ExecuteAgent(context.Background(), nil, node, engine.AgentRoute{
		NodeID:   node.ID,
		Source:   "test:unresolved",
		Provider: "local-openai-compatible",
		Model:    "local-model",
		Driver:   "",
		Backend:  "",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != runtime.StatusFail {
		t.Fatalf("empty-driver route should fail; got status %q", out.Status)
	}
	if out.Meta["failure_class"] != "deterministic" {
		t.Fatalf("expected deterministic failure_class; got %v", out.Meta)
	}
	if tmux.called {
		t.Fatalf("tmux handler should not be invoked for empty driver")
	}
}
