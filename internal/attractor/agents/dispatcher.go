// Unified agent dispatcher: routes execution to the CLI/tmux path or the
// API/SDK path based on the resolved driver. Replaces the old --tmux
// registry split — DOT/workflow intent + policy resolution determines
// which path runs, never a CLI flag.
package agents

import (
	"context"
	"fmt"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// agentHandlerImpl is the minimal interface the Dispatcher needs from
// each path (tmux or codergen). Both handlers naturally satisfy it.
// Exported as agentHandlerImpl rather than reusing engine.Handler so
// tests can inject mocks without dragging in the full engine surface.
type agentHandlerImpl interface {
	Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error)
}

// Dispatcher is the single agent handler registered for shape=box nodes.
// It resolves the agent route (engine.ResolveAgentRoute is the canonical
// resolver) and delegates to either the tmux handler (CLI drivers) or
// the codergen handler (SDK drivers).
type Dispatcher struct {
	Tmux      agentHandlerImpl
	Codergen  agentHandlerImpl
	PolicyDep engine.PolicyDeps
}

// NewDispatcher returns a Dispatcher with default tmux + codergen
// handlers wired up. Production code uses this; tests can construct a
// Dispatcher directly with custom sub-handlers (any type satisfying
// agentHandlerImpl).
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		Tmux:     NewTmuxAgentHandler(),
		Codergen: &AgentHandler{},
	}
}

// UsesFidelity is true if either sub-handler uses fidelity (both do today).
func (d *Dispatcher) UsesFidelity() bool { return true }

// RequiresProvider is true: every agent node needs a resolved provider.
func (d *Dispatcher) RequiresProvider() bool { return true }

// Execute routes the node to the appropriate handler based on the
// resolved driver. The full AgentRoute (provider/model/driver/backend
// plus auth snapshot when class-resolved) is computed once here via
// engine.ResolveAgentRoute and emitted on progress.ndjson; downstream
// handlers re-resolve from the frozen prelaunch snapshot, which means
// they always agree with the dispatcher's decision.
//
// A node that resolves to no usable driver — or to one without a
// dispatch mapping — is a deterministic failure. Prelaunch should have
// caught it; this is a belt-and-braces second line.
func (d *Dispatcher) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	route, err := engine.ResolveAgentRoute(node, exec, d.PolicyDep)
	if err != nil {
		return runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("dispatcher: %v", err),
			Meta:          map[string]any{"failure_class": "deterministic"},
			ContextUpdates: map[string]any{
				"failure_class": "deterministic",
			},
		}, nil
	}

	if exec != nil && exec.Engine != nil {
		exec.Engine.AppendProgress(map[string]any{
			"event":    "agent_dispatch",
			"node_id":  node.ID,
			"driver":   route.Driver,
			"source":   route.Source,
			"backend":  string(route.Backend),
			"provider": route.Provider,
			"model":    route.Model,
		})
	}

	switch dispatchPathForDriver(route.Driver) {
	case dispatchCLI:
		return d.tmux().Execute(ctx, exec, node)
	case dispatchAPI:
		return d.codergen().Execute(ctx, exec, node)
	default:
		// Empty driver with a non-empty Provider is the deferred-to-
		// runtime case: custom or non-canonical providers (kimi, zai,
		// minimax, custom OpenAI-compat endpoints) registered via
		// cfg.LLM.Providers in run.yaml. Delegate to codergen
		// (AgentRouter) which consults cfg to pick the backend at
		// execution time. agent_router fails loudly there if cfg has
		// no entry, surfacing a clear "no backend configured" error
		// rather than the cryptic "no dispatch mapping".
		if route.Driver == "" && strings.TrimSpace(route.Provider) != "" {
			return d.codergen().Execute(ctx, exec, node)
		}
		return runtime.Outcome{
			Status: runtime.StatusFail,
			FailureReason: fmt.Sprintf(
				"dispatcher: driver %q has no dispatch mapping; expected one of "+
					"claude_cli|codex_cli|gemini_cli|opencode|anthropic_sdk|openai_sdk|google_sdk",
				route.Driver),
			Meta:           map[string]any{"failure_class": "deterministic"},
			ContextUpdates: map[string]any{"failure_class": "deterministic"},
		}, nil
	}
}

func (d *Dispatcher) tmux() agentHandlerImpl {
	if d.Tmux != nil {
		return d.Tmux
	}
	return NewTmuxAgentHandler()
}

func (d *Dispatcher) codergen() agentHandlerImpl {
	if d.Codergen != nil {
		return d.Codergen
	}
	return &AgentHandler{}
}

// dispatchPath enumerates the two execution paths.
type dispatchPath int

const (
	dispatchUnknown dispatchPath = iota
	dispatchCLI
	dispatchAPI
)

// dispatchPathForDriver maps a resolved driver name to its execution path.
// Drivers not in this map are unsupported — surfaced as a deterministic
// failure at execution.
func dispatchPathForDriver(driver string) dispatchPath {
	switch strings.TrimSpace(driver) {
	case "claude_cli", "codex_cli", "gemini_cli", "opencode":
		return dispatchCLI
	case "anthropic_sdk", "openai_sdk", "google_sdk":
		return dispatchAPI
	default:
		return dispatchUnknown
	}
}
