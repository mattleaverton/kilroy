// Package transport provides standalone transport components for agent backends.
//
// The HTTP transport handles SDK/API-based agent calls, managing credential
// binding, adapter registration, and failover provider setup.
package transport

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/llm"
	"github.com/danshapiro/kilroy/internal/llm/providers/anthropic"
	"github.com/danshapiro/kilroy/internal/llm/providers/codexappserver"
	"github.com/danshapiro/kilroy/internal/llm/providers/google"
	"github.com/danshapiro/kilroy/internal/llm/providers/openai"
	"github.com/danshapiro/kilroy/internal/llm/providers/openaicompat"
	"github.com/danshapiro/kilroy/internal/providerspec"
)

// BackendKind identifies the transport backend type.
type BackendKind string

const (
	BackendAPI BackendKind = "api"
	BackendCLI BackendKind = "cli"
)

// ProviderRuntime captures the runtime configuration for a provider.
// This is a minimal copy of the type from engine package to avoid import cycles.
type ProviderRuntime struct {
	Key              string
	Backend          BackendKind
	API              providerspec.APISpec
	APIHeadersMap    map[string]string
	Failover         []string
	FailoverExplicit bool
}

// APIHeaders returns a copy of the API headers map.
func (r ProviderRuntime) APIHeaders() map[string]string {
	if r.APIHeadersMap == nil {
		return nil
	}
	out := make(map[string]string, len(r.APIHeadersMap))
	for k, v := range r.APIHeadersMap {
		out[k] = v
	}
	return out
}

// FailoverDecision records what happened when we tried to resolve a
// credential for a failover provider. Either: a bound value was found
// (use it), no chain was configured (canonical-env fallback is fine),
// or the chain failed to resolve (canonical-env fallback may wrong-bill).
type FailoverDecision struct {
	Bound      bool
	Value      string
	ChainName  string
	SourceKind string
	SourceName string
	SkipReason string // populated when Bound=false; empty when no chain configured
}

// HTTPClientConfig holds the configuration for building an HTTP transport client.
type HTTPClientConfig struct {
	Provider         string
	ClassResult      ClassResult
	ProviderRuntimes map[string]ProviderRuntime
	WorktreeDir      string
}

// ClassResult captures the resolved policy class information.
type ClassResult struct {
	AuthSnapshot binding.Snapshot
}

// BindSnapshotFunc materializes a frozen prelaunch credential.
// This is typically engine.BindSnapshot but can be injected for testing.
type BindSnapshotFunc func(snap binding.Snapshot) (binding.Credential, error)

// ResolverFactory creates a binding resolver for the given project root.
type ResolverFactory func(projectRoot string) (*binding.Resolver, error)

// NewHTTPClient builds an llm.Client for HTTP/SDK-backed agent calls.
//
// When classResult has a bound credential (the class-routed auth path), the
// credential is materialized from the FROZEN prelaunch snapshot. The selected
// provider's adapter is installed FIRST, before any code path that could read
// canonical env.
//
// Failover providers are best-effort: the resolver is consulted to walk auth
// chains, and providers without a chain keep canonical-env adapters from the
// runtime factory. Any failure during failover registration NEVER blocks the
// primary call — the class-routed credential is the source of truth.
//
// When classResult is nil (no agent_class on the node — legacy stylesheet
// routing), returns nil client and no error; the caller should fall back to
// the cached canonical-env client.
func NewHTTPClient(cfg HTTPClientConfig, bindFn BindSnapshotFunc, resolverFactory ResolverFactory) (*llm.Client, error) {
	if cfg.ClassResult.AuthSnapshot.Source.Kind == "" {
		// No class result - caller should use cached canonical-env client
		return nil, nil
	}

	// 1. Materialize the SELECTED credential from the frozen snapshot.
	//    bindFn deliberately does NOT reload auth.toml — the Snapshot's
	//    source identity is the contract. A vanished source (env var unset,
	//    CLI session expired) is still caught here via the live DetectionView.
	cred, err := bindFn(cfg.ClassResult.AuthSnapshot)
	if err != nil {
		return nil, err
	}
	if cred.Value == "" {
		return nil, fmt.Errorf("api path requires env_var credential, got %q", cfg.ClassResult.AuthSnapshot.Source.Kind)
	}

	// 2. Build a fresh client and install the bound-credential adapter
	//    for the SELECTED provider FIRST. CRITICAL ORDERING: the class-routed
	//    credential MUST be in place before any other adapter for this
	//    provider is registered.
	c := llm.NewClient()
	if !OverrideProviderAdapter(c, cfg.ProviderRuntimes, cfg.Provider, cred.Value) {
		return nil, fmt.Errorf(
			"class-routed provider %q has no supported API adapter (provider not in run-config runtimes or unsupported protocol)",
			cfg.Provider)
	}

	// 3. Failover providers are best-effort. We need a chain-aware
	//    resolver to walk auth chains for OTHER providers, but failure
	//    here MUST NOT block the primary call.
	var failoverResolver *binding.Resolver
	if resolverFactory != nil && cfg.WorktreeDir != "" {
		if r, rerr := resolverFactory(strings.TrimSpace(cfg.WorktreeDir)); rerr == nil {
			failoverResolver = r
		}
	}
	for otherProvider, rt := range cfg.ProviderRuntimes {
		if otherProvider == cfg.Provider {
			continue
		}
		if rt.Backend != BackendAPI {
			continue
		}
		var decision FailoverDecision
		if failoverResolver != nil {
			decision = ResolveFailoverDecision(failoverResolver, otherProvider)
		}
		if decision.Bound {
			OverrideProviderAdapter(c, cfg.ProviderRuntimes, otherProvider, decision.Value)
			continue
		}
		// No chain (or chain failed): fall back to a canonical-env adapter
		// from runtime config so failover to this provider still works
		// when canonical env IS set.
		RegisterCanonicalEnvAdapter(c, otherProvider, rt)
	}
	return c, nil
}

// OverrideProviderAdapter registers a credential-aware adapter for the
// given provider on c, replacing whatever was there. Dispatches on the
// runtime's API protocol. Returns true when an adapter was registered.
// Providers without runtime config, non-API backends, or unsupported
// protocols return false.
func OverrideProviderAdapter(c *llm.Client, runtimes map[string]ProviderRuntime, provider, value string) bool {
	rt, hasRT := runtimes[provider]
	if !hasRT || rt.Backend != BackendAPI {
		return false
	}
	baseURL := resolveBuiltInBaseURLOverride(provider, rt.API.DefaultBaseURL)
	switch rt.API.Protocol {
	case providerspec.ProtocolAnthropicMessages:
		c.Register(anthropic.NewWithProvider(provider, value, baseURL))
		return true
	case providerspec.ProtocolOpenAIResponses:
		c.Register(openai.NewWithProvider(provider, value, baseURL))
		return true
	case providerspec.ProtocolGoogleGenerateContent:
		c.Register(google.NewWithProvider(provider, value, baseURL))
		return true
	case providerspec.ProtocolOpenAIChatCompletions:
		c.Register(openaicompat.NewAdapter(openaicompat.Config{
			Provider:     provider,
			APIKey:       value,
			BaseURL:      baseURL,
			Path:         rt.API.DefaultPath,
			OptionsKey:   rt.API.ProviderOptionsKey,
			ExtraHeaders: rt.APIHeaders(),
		}))
		return true
	}
	// ProtocolCodexAppServer takes no api key (uses session); skip override.
	return false
}

// RegisterCanonicalEnvAdapter registers the canonical-env adapter for one
// failover provider on c. Silent no-op when the canonical env var is unset
// for this provider.
func RegisterCanonicalEnvAdapter(c *llm.Client, provider string, rt ProviderRuntime) {
	if rt.Backend != BackendAPI {
		return
	}
	if rt.API.Protocol == providerspec.ProtocolCodexAppServer {
		c.Register(codexappserver.NewAdapter(codexappserver.AdapterOptions{Provider: provider}))
		return
	}
	apiKeyEnv := strings.TrimSpace(rt.API.DefaultAPIKeyEnv)
	if apiKeyEnv == "" {
		return
	}
	apiKey := strings.TrimSpace(os.Getenv(apiKeyEnv))
	if apiKey == "" {
		return
	}
	OverrideProviderAdapter(c, map[string]ProviderRuntime{provider: rt}, provider, apiKey)
}

// CloneLLMClient produces a shallow copy of an llm.Client suitable for
// per-call adapter overrides. The underlying provider adapters are shared
// (they're stateless per request); only the registry map is duplicated so
// Register on the clone doesn't mutate the cached client.
func CloneLLMClient(src *llm.Client) *llm.Client {
	c := llm.NewClient()
	for _, name := range src.ProviderNames() {
		if a, ok := src.Provider(name); ok {
			c.Register(a)
		}
	}
	return c
}

// ResolveFailoverDecision walks the (provider, api_key) chain and
// returns a structured decision distinguishing "no chain" from "chain
// exhausted / ambiguous / unknown".
func ResolveFailoverDecision(resolver *binding.Resolver, provider string) FailoverDecision {
	req := binding.Requirement{Provider: provider, Method: binding.MethodAPIKey}
	snap, err := resolver.Resolve(req)
	if err != nil {
		// Distinguish "no chain configured" from real chain failures
		var noChain *binding.ErrNoChainForRequirement
		if errors.As(err, &noChain) {
			return FailoverDecision{}
		}
		return FailoverDecision{SkipReason: err.Error()}
	}
	cred, err := resolver.Bind(snap)
	if err != nil {
		return FailoverDecision{SkipReason: err.Error()}
	}
	return FailoverDecision{
		Bound:      true,
		Value:      cred.Value,
		ChainName:  snap.ChainName,
		SourceKind: string(snap.Source.Kind),
		SourceName: snap.Source.Name,
	}
}

// resolveBuiltInBaseURLOverride checks for environment variable overrides
// for built-in provider base URLs.
func resolveBuiltInBaseURLOverride(providerKey, defaultBaseURL string) string {
	normalized := strings.TrimSpace(defaultBaseURL)
	switch providerspec.CanonicalProviderKey(providerKey) {
	case "openai":
		if env := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")); env != "" {
			if normalized == "" || normalized == "https://api.openai.com" {
				return env
			}
		}
	case "anthropic":
		if env := strings.TrimSpace(os.Getenv("ANTHROPIC_BASE_URL")); env != "" {
			if normalized == "" || normalized == "https://api.anthropic.com" {
				return env
			}
		}
	case "google":
		if env := strings.TrimSpace(os.Getenv("GEMINI_BASE_URL")); env != "" {
			if normalized == "" || normalized == "https://generativelanguage.googleapis.com" {
				return env
			}
		}
	case "minimax":
		if env := strings.TrimSpace(os.Getenv("MINIMAX_BASE_URL")); env != "" {
			if normalized == "" || normalized == "https://api.minimax.io" {
				return env
			}
		}
	}
	return normalized
}
