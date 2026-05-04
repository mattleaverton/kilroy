// Acceptance tests for the Dispatcher.Execute path: verifies that
// the resolved driver routes to the right sub-handler. Tests 1, 2, 3
// from the v2 P0 architecture letter — class/DOT intent determines
// which path runs, never a CLI flag.
package agents

import (
	"context"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// recordingHandler is a stub that captures whether it was invoked and
// returns a configurable outcome.
type recordingHandler struct {
	label   string
	called  bool
	outcome runtime.Outcome
}

func (r *recordingHandler) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	r.called = true
	return r.outcome, nil
}

// Test 1: an agent_tool=claude node (CLI driver) routes to the tmux
// path; the codergen path is not called.
func TestDispatcher_ExecuteRoutesAgentToolToTmux(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "tmux fired"}}
	cg := &recordingHandler{label: "codergen", outcome: runtime.Outcome{Status: runtime.StatusFail, Notes: "codergen fired"}}
	d := &Dispatcher{Tmux: tmux, Codergen: cg}

	node := &model.Node{
		ID:    "claude_write",
		Attrs: map[string]string{"agent_tool": "claude"},
	}
	out, err := d.Execute(context.Background(), nil, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tmux.called {
		t.Fatalf("expected tmux handler to be invoked")
	}
	if cg.called {
		t.Fatalf("codergen handler must not be invoked for CLI driver")
	}
	if out.Notes != "tmux fired" {
		t.Fatalf("outcome from wrong handler: %+v", out)
	}
}

// Test 2: an llm_provider=openai + llm_model node (SDK driver) routes
// to the codergen path; the tmux path is not called.
func TestDispatcher_ExecuteRoutesSDKToCodergen(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusFail, Notes: "tmux fired"}}
	cg := &recordingHandler{label: "codergen", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "codergen fired"}}
	d := &Dispatcher{Tmux: tmux, Codergen: cg}

	node := &model.Node{
		ID: "implement",
		Attrs: map[string]string{
			"llm_provider": "openai",
			"llm_model":    "gpt-5.4",
		},
	}
	out, err := d.Execute(context.Background(), nil, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmux.called {
		t.Fatalf("tmux handler must not be invoked for SDK driver")
	}
	if !cg.called {
		t.Fatalf("expected codergen handler to be invoked")
	}
	if out.Notes != "codergen fired" {
		t.Fatalf("outcome from wrong handler: %+v", out)
	}
}

// Test 3: a mixed-intent invocation — call dispatch twice with
// different drivers using the same Dispatcher. The first call routes
// to one path; the second to the other. State (the .called flag) is
// per-handler, so we can confirm both paths fired.
func TestDispatcher_ExecuteMixedRouting(t *testing.T) {
	tmux := &recordingHandler{label: "tmux", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "tmux"}}
	cg := &recordingHandler{label: "codergen", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "codergen"}}
	d := &Dispatcher{Tmux: tmux, Codergen: cg}

	cliNode := &model.Node{ID: "n1", Attrs: map[string]string{"agent_tool": "claude"}}
	apiNode := &model.Node{ID: "n2", Attrs: map[string]string{"llm_provider": "anthropic", "llm_model": "claude-sonnet-4-6"}}

	if _, err := d.Execute(context.Background(), nil, cliNode); err != nil {
		t.Fatalf("CLI dispatch err: %v", err)
	}
	if _, err := d.Execute(context.Background(), nil, apiNode); err != nil {
		t.Fatalf("API dispatch err: %v", err)
	}

	if !tmux.called {
		t.Fatalf("CLI node should have invoked tmux handler")
	}
	if !cg.called {
		t.Fatalf("API node should have invoked codergen handler")
	}
}

// Test 5 (reviewer regression): an llm_provider=<custom> node where the
// provider isn't one of the canonical SDK drivers (kimi/zai/minimax/etc.)
// resolves with Driver="" and Provider!="". The dispatcher must delegate
// to codergen — agent_router will then pick the backend from
// cfg.LLM.Providers at execution time. Without this delegation, runs with
// custom providers fail at dispatch with "no dispatch mapping" before
// run-config-aware routing has a chance to kick in.
func TestDispatcher_ExecuteCustomProvider_DelegatesToCodergen(t *testing.T) {
	tmux := &recordingHandler{label: "tmux"}
	cg := &recordingHandler{label: "codergen", outcome: runtime.Outcome{Status: runtime.StatusSuccess, Notes: "codergen handled custom"}}
	d := &Dispatcher{Tmux: tmux, Codergen: cg}

	node := &model.Node{
		ID: "minimax_call",
		Attrs: map[string]string{
			"llm_provider": "minimax",
			"llm_model":    "minimax-m2.5",
		},
	}
	out, err := d.Execute(context.Background(), nil, node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmux.called {
		t.Fatalf("tmux handler must not be invoked for custom provider")
	}
	if !cg.called {
		t.Fatalf("codergen handler must be invoked for custom provider with empty Driver (got out=%+v)", out)
	}
	if out.Status != runtime.StatusSuccess {
		t.Fatalf("expected delegated codergen success; got %+v", out)
	}
}

// Test 4: a node with no resolvable driver fails deterministically at
// dispatch time, with neither sub-handler invoked.
func TestDispatcher_ExecuteVagueNode_DeterministicFailure(t *testing.T) {
	tmux := &recordingHandler{label: "tmux"}
	cg := &recordingHandler{label: "codergen"}
	d := &Dispatcher{Tmux: tmux, Codergen: cg}

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
	if tmux.called || cg.called {
		t.Fatalf("neither handler should be invoked for vague node (tmux=%v cg=%v)", tmux.called, cg.called)
	}
}
