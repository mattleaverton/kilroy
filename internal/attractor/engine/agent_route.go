// Unified agent-route resolver. Single source of truth for the routing
// decision an agent node represents — driver, provider, model, backend,
// and (when class-resolved) full auth snapshot. Consumed by the
// dispatcher (to pick CLI vs API), prelaunch (to fail vague nodes
// loudly), and downstream handlers that need the resolved tuple.
package engine

import (
	"fmt"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/policy"
)

// AgentRoute is the complete routing decision for an agent node. Resolved
// once per node from one of three sources, in precedence order:
//
//  1. agent_class= → policy resolver (with prelaunch snapshot freeze)
//  2. agent_tool= → CLI driver
//  3. llm_provider= + llm_model= → SDK driver
//
// A node that supplies none of these — or whose attributes don't form a
// complete route — is a deterministic failure surfaced at prelaunch.
type AgentRoute struct {
	NodeID string

	// Source is a human-readable origin string suitable for progress
	// events and resolution.json: "policy_class:<name>", "agent_tool=<t>",
	// or "llm_provider=<p>".
	Source string

	// Class is set only when Source begins with "policy_class:".
	Class string

	Provider string      // canonical provider key: anthropic | openai | google
	Model    string      // model id (no normalization here)
	Driver   string      // claude_cli | codex_cli | gemini_cli | opencode | anthropic_sdk | openai_sdk | google_sdk
	Backend  BackendKind // BackendCLI | BackendAPI

	// ClassResult carries the full policy.ResolveResult (including auth
	// snapshot) for class-resolved routes. nil for non-class routes —
	// they don't go through the auth chain at resolution time, so there
	// is no snapshot to carry.
	ClassResult *policy.ResolveResult
}

// AuthMethod returns the auth method ("api_key" | "cli_oauth" | "") from
// the underlying ClassResult, or "" for non-class routes.
func (r AgentRoute) AuthMethod() string {
	if r.ClassResult == nil {
		return ""
	}
	return r.ClassResult.AuthMethod()
}

// AuthSource returns the source identifier (env var or CLI tool name)
// from the underlying ClassResult, or "" for non-class routes.
func (r AgentRoute) AuthSource() string {
	if r.ClassResult == nil {
		return ""
	}
	return r.ClassResult.AuthSource()
}

// IsCLI reports whether the route runs through a local CLI subprocess.
func (r AgentRoute) IsCLI() bool { return r.Backend == BackendCLI }

// IsAPI reports whether the route runs through the HTTP/SDK path.
func (r AgentRoute) IsAPI() bool { return r.Backend == BackendAPI }

// ResolveAgentRoute is the canonical entry point for turning an agent
// node into a routing decision. Used by:
//
//   - the dispatcher (to choose tmux vs codergen)
//   - prelaunch (to validate every agent node, not just class-bearing)
//   - any caller that needs the resolved tuple before execution
//
// When exec is non-nil and a prelaunch snapshot exists, class resolution
// reads from the frozen snapshot rather than re-running policy.Resolve —
// keeping the freeze authoritative.
func ResolveAgentRoute(node *model.Node, exec *Execution, deps PolicyDeps) (AgentRoute, error) {
	if node == nil {
		return AgentRoute{}, fmt.Errorf("nil node")
	}

	cls, hasClass, err := ResolveAgentClass(node, exec, deps)
	if err != nil {
		return AgentRoute{}, err
	}
	if hasClass {
		result := cls.Result
		return AgentRoute{
			NodeID:      node.ID,
			Source:      "policy_class:" + cls.Class,
			Class:       cls.Class,
			Provider:    cls.Provider,
			Model:       cls.Model,
			Driver:      cls.Driver,
			Backend:     cls.Backend,
			ClassResult: &result,
		}, nil
	}

	if tool := strings.TrimSpace(node.Attr("agent_tool", "")); tool != "" {
		driver := DriverForAgentTool(tool)
		if driver == "" {
			return AgentRoute{}, fmt.Errorf("agent_tool=%q has no driver mapping (expected claude|codex|gemini|opencode)", tool)
		}
		provider, backend := providerAndBackendForDriver(driver)
		if provider == "" {
			return AgentRoute{}, fmt.Errorf("agent_tool=%q maps to driver %q which has no provider mapping", tool, driver)
		}
		modelID := strings.TrimSpace(node.Attr("llm_model", ""))
		return AgentRoute{
			NodeID:   node.ID,
			Source:   "agent_tool=" + tool,
			Provider: provider,
			Model:    modelID,
			Driver:   driver,
			Backend:  backend,
		}, nil
	}

	provider := strings.TrimSpace(node.Attr("llm_provider", ""))
	modelID := strings.TrimSpace(node.Attr("llm_model", ""))
	if provider != "" && modelID != "" {
		driver := DriverForSDKProvider(provider)
		canonProv := provider
		backend := BackendKind("")
		if driver != "" {
			canonProv, backend = providerAndBackendForDriver(driver)
		}
		// Unknown providers (kimi, zai, minimax, custom OpenAI-compat
		// endpoints, etc.) are routed via run-config rather than a
		// canonical driver. The Dispatcher rejects empty-Driver routes;
		// the legacy CodergenHandler resolves them through
		// cfg.LLM.Providers. Prelaunch stays lenient — typos surface at
		// dispatch time, real config-driven providers continue to work.
		return AgentRoute{
			NodeID:   node.ID,
			Source:   "llm_provider=" + provider,
			Provider: canonProv,
			Model:    modelID,
			Driver:   driver,
			Backend:  backend,
		}, nil
	}

	return AgentRoute{}, fmt.Errorf("agent node %q has no agent_class=, agent_tool=, or llm_provider+llm_model — cannot resolve route", node.ID)
}

// DriverForAgentTool maps the agent_tool= attribute value to a CLI driver.
// Returns "" for unknown tools.
func DriverForAgentTool(tool string) string {
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

// DriverForSDKProvider maps llm_provider= to its SDK driver. Returns ""
// for unknown providers.
func DriverForSDKProvider(provider string) string {
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
