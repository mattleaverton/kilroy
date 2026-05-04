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

// Dispatcher is the single agent handler registered for shape=box nodes.
// It resolves the agent route (class via policy snapshot, or explicit
// DOT attrs) and delegates to either the tmux handler (CLI drivers) or
// the codergen handler (SDK drivers).
type Dispatcher struct {
	Tmux      *TmuxAgentHandler
	Codergen  *AgentHandler
	PolicyDep engine.PolicyDeps
}

// NewDispatcher returns a Dispatcher with default tmux + codergen
// handlers wired up. Production code uses this; tests can construct a
// Dispatcher directly with custom sub-handlers.
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
// resolved driver. Class-routed nodes use the prelaunch snapshot;
// explicit-DOT nodes derive their driver from agent_tool=/llm_provider=
// hints. A node that resolves to no usable driver (or to one without a
// dispatch mapping) is a deterministic failure.
func (d *Dispatcher) Execute(ctx context.Context, exec *engine.Execution, node *model.Node) (runtime.Outcome, error) {
	driver, source, err := resolveDriverForDispatch(node, exec, d.PolicyDep)
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
			"event":   "agent_dispatch",
			"node_id": node.ID,
			"driver":  driver,
			"source":  source,
		})
	}

	switch dispatchPathForDriver(driver) {
	case dispatchCLI:
		return d.tmux().Execute(ctx, exec, node)
	case dispatchAPI:
		return d.codergen().Execute(ctx, exec, node)
	default:
		return runtime.Outcome{
			Status: runtime.StatusFail,
			FailureReason: fmt.Sprintf(
				"dispatcher: driver %q has no dispatch mapping; expected one of "+
					"claude_cli|codex_cli|gemini_cli|opencode|anthropic_sdk|openai_sdk|google_sdk",
				driver),
			Meta:           map[string]any{"failure_class": "deterministic"},
			ContextUpdates: map[string]any{"failure_class": "deterministic"},
		}, nil
	}
}

func (d *Dispatcher) tmux() *TmuxAgentHandler {
	if d.Tmux != nil {
		return d.Tmux
	}
	return NewTmuxAgentHandler()
}

func (d *Dispatcher) codergen() *AgentHandler {
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

// resolveDriverForDispatch picks the driver to dispatch on, in
// precedence order:
//
//  1. class-routed: read the engine's class resolver (which itself
//     reads the prelaunch snapshot when available). The driver is
//     authoritative; no execution-time re-resolution.
//  2. explicit agent_tool=: tool-name → driver mapping
//     (claude → claude_cli, codex → codex_cli, gemini → gemini_cli,
//     opencode → opencode).
//  3. explicit llm_provider= without a CLI tool indicator:
//     provider → SDK driver mapping (anthropic → anthropic_sdk, etc.).
//
// A node that supplies neither agent_class= nor enough explicit DOT
// attributes for a complete route is a deterministic failure here. The
// prelaunch validator should have caught it earlier; this is a
// belt-and-braces second line.
func resolveDriverForDispatch(node *model.Node, exec *engine.Execution, deps engine.PolicyDeps) (driver string, source string, err error) {
	if node == nil {
		return "", "", fmt.Errorf("nil node")
	}
	cls, hasClass, classErr := engine.ResolveAgentClass(node, exec, deps)
	if classErr != nil {
		return "", "", fmt.Errorf("policy class resolve: %w", classErr)
	}
	if hasClass {
		return cls.Driver, "policy_class:" + cls.Class, nil
	}

	// Explicit agent_tool= takes the CLI path. Same convention used by
	// the legacy stylesheet route.
	tool := strings.TrimSpace(node.Attr("agent_tool", ""))
	if tool != "" {
		if d := driverForAgentTool(tool); d != "" {
			return d, "agent_tool=" + tool, nil
		}
		return "", "", fmt.Errorf("agent_tool=%q has no driver mapping (expected claude|codex|gemini|opencode)", tool)
	}

	// Explicit llm_provider= without agent_tool= → SDK.
	provider := strings.TrimSpace(node.Attr("llm_provider", ""))
	model := strings.TrimSpace(node.Attr("llm_model", ""))
	if provider != "" && model != "" {
		if d := driverForSDKProvider(provider); d != "" {
			return d, "llm_provider=" + provider, nil
		}
		return "", "", fmt.Errorf("llm_provider=%q has no SDK driver mapping (expected anthropic|openai|google)", provider)
	}

	return "", "", fmt.Errorf("agent node %q has no agent_class=, agent_tool=, or llm_provider+llm_model — cannot resolve driver", node.ID)
}

// driverForAgentTool maps the legacy agent_tool= attribute value to a
// driver. Returns "" for unknown tools.
func driverForAgentTool(tool string) string {
	switch strings.ToLower(strings.TrimSpace(tool)) {
	case "claude":
		return "claude_cli"
	case "codex":
		return "codex_cli"
	case "gemini":
		return "gemini_cli"
	case "opencode":
		return "opencode"
	default:
		return ""
	}
}

// driverForSDKProvider maps an explicit llm_provider= to its SDK driver.
// Returns "" for unknown providers.
func driverForSDKProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "anthropic":
		return "anthropic_sdk"
	case "openai":
		return "openai_sdk"
	case "google", "gemini":
		return "google_sdk"
	default:
		return ""
	}
}
