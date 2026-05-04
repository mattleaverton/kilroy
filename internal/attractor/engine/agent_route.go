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
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
	"github.com/danshapiro/kilroy/internal/policy"
	"github.com/danshapiro/kilroy/internal/providerspec"
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

func validateExecutableAgentRoute(route AgentRoute) error {
	if strings.TrimSpace(route.Driver) == "" || route.Backend == "" {
		provider := strings.TrimSpace(route.Provider)
		if provider == "" {
			provider = "<empty>"
		}
		return fmt.Errorf(
			"llm_provider=%q has no executable route: provider is not built in and no provider runtime/spec with a supported backend/protocol is loaded",
			provider,
		)
	}
	switch route.Backend {
	case BackendCLI, BackendAPI:
		return nil
	default:
		return fmt.Errorf("route for provider %q uses unsupported backend %q", route.Provider, route.Backend)
	}
}

func AgentRouteFailureOutcome(err error) runtime.Outcome {
	reason := "agent route: unresolved"
	if err != nil {
		reason = fmt.Sprintf("agent route: %v", err)
	}
	return runtime.Outcome{
		Status:        runtime.StatusFail,
		FailureReason: reason,
		Meta:          map[string]any{"failure_class": "deterministic"},
		ContextUpdates: map[string]any{
			"failure_class": "deterministic",
		},
	}
}

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

	if route, ok, err := LoadPreLaunchAgentRouteForExec(exec, node.ID); err != nil {
		return AgentRoute{}, err
	} else if ok {
		if err := validateExecutableAgentRoute(route); err != nil {
			return AgentRoute{}, fmt.Errorf("prelaunch snapshot route for node %q is invalid: %w", node.ID, err)
		}
		if route.ClassResult != nil {
			emitResolutionEvents(exec, node.ID, route.Class, *route.ClassResult)
			persistResolution(exec, node.ID, route.Class, *route.ClassResult)
		}
		return route, nil
	}

	cls, hasClass, err := ResolveAgentClass(node, exec, deps)
	if err != nil {
		return AgentRoute{}, err
	}
	if hasClass {
		result := cls.Result
		route := AgentRoute{
			NodeID:      node.ID,
			Source:      "policy_class:" + cls.Class,
			Class:       cls.Class,
			Provider:    cls.Provider,
			Model:       cls.Model,
			Driver:      cls.Driver,
			Backend:     cls.Backend,
			ClassResult: &result,
		}
		if err := validateExecutableAgentRoute(route); err != nil {
			return AgentRoute{}, err
		}
		return route, nil
	}

	if tool := strings.TrimSpace(node.Attr("agent_tool", "")); tool != "" {
		driver := DriverForAgentTool(tool)
		if driver == "" {
			return AgentRoute{}, fmt.Errorf("agent_tool=%q has no driver mapping (expected claude|codex|gemini|opencode)", tool)
		}
		modelID := strings.TrimSpace(node.Attr("llm_model", ""))
		explicitProvider := strings.TrimSpace(node.Attr("llm_provider", ""))

		// opencode is multi-provider — the user picks anthropic/kimi/zai/etc.
		// via opencode's --model arg and provider config. Provider must come
		// from llm_provider= on the node; the node says which provider
		// opencode should use.
		if driver == "opencode" {
			if explicitProvider == "" {
				return AgentRoute{}, fmt.Errorf(
					"agent_tool=\"opencode\" requires explicit llm_provider= " +
						"(opencode is multi-provider; the node must say which one)")
			}
			route := AgentRoute{
				NodeID:   node.ID,
				Source:   "agent_tool=opencode",
				Provider: normalizeProviderKey(explicitProvider),
				Model:    modelID,
				Driver:   "opencode",
				Backend:  BackendCLI,
			}
			if err := validateExecutableAgentRoute(route); err != nil {
				return AgentRoute{}, err
			}
			return route, nil
		}

		// Fixed-provider tools (claude/codex/gemini): driver determines
		// provider. If the node also sets llm_provider=, it must match —
		// otherwise the metadata says one thing and the binary does
		// another, which is exactly the silent-wrong-mapping class of
		// bug we're trying to eliminate.
		provider, backend := providerAndBackendForDriver(driver)
		if provider == "" {
			return AgentRoute{}, fmt.Errorf("agent_tool=%q maps to driver %q which has no provider mapping", tool, driver)
		}
		if explicitProvider != "" && normalizeProviderKey(explicitProvider) != provider {
			return AgentRoute{}, fmt.Errorf(
				"agent_tool=%q implies llm_provider=%q; node also sets llm_provider=%q which conflicts",
				tool, provider, explicitProvider)
		}
		route := AgentRoute{
			NodeID:   node.ID,
			Source:   "agent_tool=" + tool,
			Provider: provider,
			Model:    modelID,
			Driver:   driver,
			Backend:  backend,
		}
		if err := validateExecutableAgentRoute(route); err != nil {
			return AgentRoute{}, err
		}
		return route, nil
	}

	provider := strings.TrimSpace(node.Attr("llm_provider", ""))
	modelID := strings.TrimSpace(node.Attr("llm_model", ""))
	if provider != "" && modelID != "" {
		canonProv, driver, backend := routeForProvider(provider, deps.ProviderRuntimes)
		route := AgentRoute{
			NodeID:   node.ID,
			Source:   "llm_provider=" + provider,
			Provider: canonProv,
			Model:    modelID,
			Driver:   driver,
			Backend:  backend,
		}
		if err := validateExecutableAgentRoute(route); err != nil {
			return AgentRoute{}, err
		}
		return route, nil
	}

	return AgentRoute{}, fmt.Errorf("agent node %q has no agent_class=, agent_tool=, or llm_provider+llm_model — cannot resolve route", node.ID)
}

// routeForProvider turns an explicit llm_provider into a provider/driver/backend
// tuple. Run-config provider runtimes are resolver input here, not a handler
// fallback later.
func routeForProvider(provider string, runtimes map[string]ProviderRuntime) (string, string, BackendKind) {
	canonProv := providerspec.CanonicalProviderKey(provider)
	if canonProv == "" {
		canonProv = strings.TrimSpace(provider)
	}
	if rt, ok := providerRuntimeForRoute(canonProv, runtimes); ok {
		switch rt.Backend {
		case BackendCLI:
			if driver := cliDriverForProvider(canonProv); driver != "" {
				return canonProv, driver, BackendCLI
			}
			return canonProv, "", BackendCLI
		case BackendAPI:
			if driver := DriverForSDKProvider(canonProv); driver != "" {
				return canonProv, driver, BackendAPI
			}
			if driver := driverForAPIProtocol(rt.API.Protocol); driver != "" {
				return canonProv, driver, BackendAPI
			}
			return canonProv, "", BackendAPI
		}
	}
	if driver := DriverForSDKProvider(canonProv); driver != "" {
		prov, backend := providerAndBackendForDriver(driver)
		return prov, driver, backend
	}
	if spec, ok := providerspec.Builtin(canonProv); ok && spec.API != nil {
		return canonProv, driverForAPIProtocol(spec.API.Protocol), BackendAPI
	}
	return canonProv, "", ""
}

func providerRuntimeForRoute(provider string, runtimes map[string]ProviderRuntime) (ProviderRuntime, bool) {
	key := providerspec.CanonicalProviderKey(provider)
	if key == "" {
		return ProviderRuntime{}, false
	}
	if rt, ok := runtimes[key]; ok {
		return rt, true
	}
	for raw, rt := range runtimes {
		if providerspec.CanonicalProviderKey(raw) == key {
			return rt, true
		}
	}
	return ProviderRuntime{}, false
}

func cliDriverForProvider(provider string) string {
	switch providerspec.CanonicalProviderKey(provider) {
	case "anthropic":
		return "claude_cli"
	case "openai":
		return "codex_cli"
	case "google":
		return "gemini_cli"
	default:
		return ""
	}
}

func driverForAPIProtocol(protocol providerspec.APIProtocol) string {
	switch protocol {
	case providerspec.ProtocolOpenAIChatCompletions:
		return "openai_compat_api"
	case providerspec.ProtocolOpenAIResponses:
		return "openai_sdk"
	case providerspec.ProtocolAnthropicMessages:
		return "anthropic_sdk"
	case providerspec.ProtocolGoogleGenerateContent:
		return "google_sdk"
	case providerspec.ProtocolCodexAppServer:
		return "codex_app_server_api"
	default:
		return ""
	}
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
