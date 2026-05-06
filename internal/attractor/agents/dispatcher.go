// Unified agent dispatcher: routes execution to the CLI/tmux path or the
// API/SDK path based on the resolved driver. Replaces the old --tmux
// registry split — DOT/workflow intent + policy resolution determines
// which path runs, never a CLI flag.
package agents

import (
	"context"
	"fmt"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/agentbackend"
	agauth "github.com/danshapiro/kilroy/internal/attractor/agents/auth"
	"github.com/danshapiro/kilroy/internal/attractor/agents/loop"
	"github.com/danshapiro/kilroy/internal/attractor/agents/templates"
	"github.com/danshapiro/kilroy/internal/attractor/agents/transport"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
	"github.com/danshapiro/kilroy/internal/auth/binding"
)

// agentHandlerImpl is the minimal interface the Dispatcher needs from
// each path (tmux or codergen). Both handlers naturally satisfy it.
// Exported as agentHandlerImpl rather than reusing engine.Handler so
// tests can inject mocks without dragging in the full engine surface.
type agentHandlerImpl interface {
	ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error)
}

// tmuxHandler is the interface for the tmux/CLI path. It extends
// agentHandlerImpl with the session-based execution method.
// *TmuxAgentHandler satisfies this interface.
type tmuxHandler interface {
	agentHandlerImpl
	ExecuteAgentWithSession(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute, tmpl *templates.Template, toolName string, prompt string, modelID string, cfg transport.SessionConfig) (runtime.Outcome, error)
}

// Dispatcher is the single agent handler registered for shape=box nodes.
// It resolves the agent route (engine.ResolveAgentRoute is the canonical
// resolver) and delegates to either the tmux handler (CLI drivers) or
// the codergen handler (API/SDK drivers).
type Dispatcher struct {
	Tmux      tmuxHandler
	PolicyDep engine.PolicyDeps
}

// NewDispatcher returns a Dispatcher with default tmux + codergen
// handlers wired up. Production code uses this; tests can construct a
// Dispatcher directly with custom sub-handlers (any type satisfying
// agentHandlerImpl).
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		Tmux: NewTmuxAgentHandler(),
	}
}

// UsesFidelity is true if either sub-handler uses fidelity (both do today).
func (d *Dispatcher) UsesFidelity() bool { return true }

// RequiresProvider is true: every agent node needs a resolved provider.
func (d *Dispatcher) RequiresProvider() bool { return true }

// AgentRoutePolicyDeps exposes dispatcher test deps to the engine when the
// engine resolves a route before invoking ExecuteAgent.
func (d *Dispatcher) AgentRoutePolicyDeps() engine.PolicyDeps {
	if d == nil {
		return engine.PolicyDeps{}
	}
	return d.PolicyDep
}

// Execute routes the node to the appropriate handler. It exists for tests and
// legacy direct calls that bypass the engine's route-aware execution path.
// Normal execution should call ExecuteAgent with an already-resolved route.
//
// A node that resolves to no usable driver — or to one without a
// dispatch mapping — is a deterministic failure. Prelaunch should have
// caught it; this is a belt-and-braces second line.
func (d *Dispatcher) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	route, err := engine.ResolveAgentRoute(node, exec, d.PolicyDep)
	if err != nil {
		return engine.AgentRouteFailureOutcome(err), nil
	}
	return d.ExecuteAgent(ctx, exec, node, route)
}

// ExecuteAgent routes an explicitly resolved AgentRoute to the correct
// execution handler. Sub-handlers execute the supplied route and do not
// re-resolve provider/model/backend/auth from node attributes.
func (d *Dispatcher) ExecuteAgent(ctx context.Context, exec *engine.Execution, node *model.Node, route engine.AgentRoute) (runtime.Outcome, error) {
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
	case dispatchCLI, dispatchAPI:
		if exec == nil || exec.Engine == nil {
			return runtime.Outcome{
				Status:        runtime.StatusFail,
				FailureReason: "dispatcher: execution context with engine is required for agent backend dispatch",
				Meta:          map[string]any{"failure_class": "deterministic"},
			}, nil
		}
		prior := exec.Engine.AgentBackend
		exec.Engine.AgentBackend = &runTurnEngineBackend{
			apiRunner:    prior,
			tmux:         d.tmuxSessionHandler(),
			authResolver: agauth.NewBindingAuthResolver(engine.BindSnapshot, authBindWrapper),
		}
		defer func() {
			exec.Engine.AgentBackend = prior
		}()
		return (&engine.CodergenHandler{}).ExecuteAgent(ctx, exec, node, route)
	default:
		return runtime.Outcome{
			Status: runtime.StatusFail,
			FailureReason: fmt.Sprintf(
				"dispatcher: driver %q has no dispatch mapping; expected one of "+
					"claude_cli|codex_cli|gemini_cli|opencode|anthropic_sdk|openai_sdk|google_sdk|openai_compat_api|codex_app_server_api",
				route.Driver),
			Meta:           map[string]any{"failure_class": "deterministic"},
			ContextUpdates: map[string]any{"failure_class": "deterministic"},
		}, nil
	}
}

func (d *Dispatcher) tmuxSessionHandler() tmuxSessionHandler {
	if d.Tmux != nil {
		return d.Tmux
	}
	return NewTmuxAgentHandler()
}

type runTurnEngineBackend struct {
	apiRunner    engine.AgentBackend
	tmux         tmuxSessionHandler
	authResolver agauth.AuthResolver
}

func (b *runTurnEngineBackend) Run(ctx context.Context, exec *engine.Execution, node *model.Node, prompt string, route engine.AgentRoute) (string, *runtime.Outcome, error) {
	apiRunner := b.apiRunner
	if apiRunner == nil {
		apiRunner = &engine.SimulatedAgentBackend{}
	}

	var backend agentbackend.AgentBackend
	switch dispatchPathForDriver(route.Driver) {
	case dispatchAPI:
		backend = NewSDKBackend(apiRunner, WithSDKAuthResolver(b.authResolver))
	case dispatchCLI:
		backend = NewTmuxBackend(b.tmux, WithTmuxAuthResolver(b.authResolver))
	default:
		return "", &runtime.Outcome{
			Status:        runtime.StatusFail,
			FailureReason: fmt.Sprintf("dispatcher: driver %q has no dispatch mapping", route.Driver),
			Meta:          map[string]any{"failure_class": "deterministic"},
		}, nil
	}
	defer backend.Close()

	result, err := loop.RunTurn(
		ctx,
		backend,
		agentbackend.UserMessage{Text: prompt},
		agentbackend.TurnOptions{
			Model: route.Model,
			Extra: map[string]any{
				"provider": route.Provider,
				"driver":   route.Driver,
				"exec":     exec,
				"node":     node,
				"route":    route,
			},
		},
		nil,
		loop.TurnConfig{EventSink: runTurnProgressSink(exec, node)},
	)
	if err != nil {
		return result.Text, nil, err
	}

	if carrier, ok := backend.(interface{ LastOutcome() *runtime.Outcome }); ok {
		if out := carrier.LastOutcome(); out != nil {
			return result.Text, out, nil
		}
	}
	return result.Text, &runtime.Outcome{
		Status: runtime.StatusSuccess,
		Notes:  "agent turn completed",
		ContextUpdates: map[string]any{
			"last_stage":    node.ID,
			"last_response": engine.Truncate(result.Text, 200),
		},
	}, nil
}

func runTurnProgressSink(exec *engine.Execution, node *model.Node) func(loop.LoopEvent) {
	if exec == nil || exec.Engine == nil || node == nil {
		return nil
	}
	return func(ev loop.LoopEvent) {
		payload := map[string]any{
			"event":   "agent_turn_" + string(ev.Type),
			"node_id": node.ID,
		}
		if ev.ToolCall != nil {
			payload["tool_name"] = ev.ToolCall.Name
			payload["tool_call_id"] = ev.ToolCall.ID
		}
		if ev.ToolResult != nil {
			payload["tool_use_id"] = ev.ToolResult.ToolUseID
			payload["is_error"] = ev.ToolResult.IsError
		}
		if ev.Error != nil {
			payload["error"] = ev.Error.Error()
		}
		exec.Engine.AppendProgress(payload)
	}
}

func authBindWrapper(driver string, snap binding.Snapshot, cred binding.Credential, stageDir string) (agauth.BindResult, error) {
	result, err := engine.Bind(driver, snap, cred, stageDir)
	if err != nil {
		return agauth.BindResult{}, err
	}
	return agauth.BindResult{
		EnvSet:       result.EnvSet,
		EnvScrub:     result.EnvScrub,
		FilesToWrite: result.FilesToWrite,
		SDKArg:       result.SDKArg,
		SourceName:   result.SourceName,
		SourceKind:   result.SourceKind,
	}, nil
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
	case "anthropic_sdk", "openai_sdk", "google_sdk", "openai_compat_api", "codex_app_server_api":
		return dispatchAPI
	default:
		return dispatchUnknown
	}
}
