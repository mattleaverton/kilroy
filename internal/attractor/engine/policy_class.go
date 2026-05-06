// Shared policy class-resolution helper consumed by both AgentRouter (API path)
// and TmuxAgentHandler (tmux path). Routes a node's class= attribute through
// internal/policy, emits the policy_class_resolved progress event, and
// persists the full ResolveResult to <logs_root>/<node_id>/resolution.json
// per plan §6.4.
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// PolicyDeps carries injectable policy data and auth-resolver construction.
// Zero values mean "use production defaults" (policy.Load + auth detection
// against the user/project auth.toml). Tests inject stubs.
type PolicyDeps struct {
	Load             func() (*policy.Data, error)
	Resolver         func(projectRoot string) (*binding.Resolver, error)
	ProviderRuntimes map[string]ProviderRuntime
}

// ClassResolution is the outcome of resolving a node's class= attribute.
type ClassResolution struct {
	Class    string
	Provider string
	Model    string
	Driver   string
	Backend  BackendKind
	Result   policy.ResolveResult
}

// PolicyClassAttr is the DOT node attribute that drives Step 4b's policy
// resolver. It is **distinct** from the legacy `class=` attribute, which
// is a CSS-style stylesheet selector matched by `model_stylesheet`
// rules. Splitting them lets workflow authors:
//
//   - use `class="…"` purely as a stylesheet selector (e.g. coding-loop's
//     `.implementer { llm_model: claude-sonnet-4.6 }` rule), and
//   - use `agent_class="hard_coding"` to drive deterministic policy
//     routing.
//
// Crucially, unknown values of `agent_class=` are a **hard error** at
// validation time — typo detection is non-negotiable for the policy
// surface. (An earlier "tolerant" version that fell through to
// stylesheet attrs was rolled back; see plan §13.7.)
const PolicyClassAttr = "agent_class"

// ResolveAgentClass resolves the agent_class attribute on a node via
// the policy resolver. If the node has no agent_class attribute, returns
// ok=false with no error. On a successful resolution this is the single
// source of truth that both AgentRouter (API path) and TmuxAgentHandler
// (tmux path) consume, and a policy_class_resolved progress event is
// emitted.
//
// Unknown agent_class names are an error — they're never silently
// treated as stylesheet selectors (use the unrelated `class=` attribute
// for that). This keeps the policy surface explicit and typo-safe.
func ResolveAgentClass(node *model.Node, exec *Execution, deps PolicyDeps) (ClassResolution, bool, error) {
	className := strings.TrimSpace(node.Attr(PolicyClassAttr, ""))
	if className == "" {
		return ClassResolution{}, false, nil
	}

	// Plan §5: when prelaunch ran for this run, snapshots are
	// AUTHORITATIVE — execution reads from the frozen snapshot and
	// must not re-resolve. Drift between prelaunch and execution
	// (env, config, filesystem) cannot silently change the route.
	//
	// Detection: the existence of <logs_root>/prelaunch_snapshots.json
	// signals "prelaunch ran". When that file exists:
	//   - this node's snapshot present  → use it
	//   - this node's snapshot absent   → hard error (snapshot integrity broken)
	// When the file is absent, fall back to live resolution — that's
	// the legitimate test / ad-hoc path that didn't run prelaunch.
	logsRoot := ""
	if exec != nil {
		logsRoot = strings.TrimSpace(exec.LogsRoot)
		if logsRoot == "" && exec.Engine != nil {
			logsRoot = strings.TrimSpace(exec.Engine.LogsRoot)
		}
	}
	if logsRoot != "" {
		fileExists, err := preLaunchSnapshotsFileExists(logsRoot)
		if err != nil {
			return ClassResolution{}, false, fmt.Errorf("stat prelaunch snapshots: %w", err)
		}
		if fileExists {
			frozen, ok, snapErr := LoadPreLaunchSnapshot(logsRoot, node.ID)
			if snapErr != nil {
				return ClassResolution{}, false, fmt.Errorf("read prelaunch snapshot: %w", snapErr)
			}
			if !ok {
				return ClassResolution{}, false, fmt.Errorf(
					"prelaunch_snapshots.json exists but has no entry for node %q — "+
						"snapshot integrity broken; do not silently re-resolve",
					node.ID,
				)
			}
			prov, be := providerAndBackendForDriver(frozen.Driver)
			if prov == "" {
				return ClassResolution{}, false, fmt.Errorf("prelaunch snapshot has unknown driver %q", frozen.Driver)
			}
			emitResolutionEvents(exec, node.ID, className, *frozen)
			persistResolution(exec, node.ID, className, *frozen)
			return ClassResolution{
				Class:    className,
				Provider: prov,
				Model:    frozen.ModelID,
				Driver:   frozen.Driver,
				Backend:  be,
				Result:   *frozen,
			}, true, nil
		}
	}

	load := deps.Load
	if load == nil {
		projectRoot := projectRootForExec(exec)
		load = func() (*policy.Data, error) {
			return policy.LoadEffective(projectRoot)
		}
	}
	data, err := load()
	if err != nil {
		return ClassResolution{}, false, fmt.Errorf("policy load: %w", err)
	}

	resolverFactory := deps.Resolver
	if resolverFactory == nil {
		resolverFactory = DefaultBindingResolver
	}
	authResolver, err := resolverFactory(projectRootForExec(exec))
	if err != nil {
		return ClassResolution{}, false, fmt.Errorf("auth resolver: %w", err)
	}

	res, err := policy.Resolve(policy.ResolveRequest{
		ClassID:    className,
		NodeID:     node.ID,
		WorkflowID: graphNameForExec(exec),
	}, data, authResolver)
	if err != nil {
		return ClassResolution{}, false, fmt.Errorf("policy resolve %q: %w", className, err)
	}

	prov, be := providerAndBackendForDriver(res.Driver)
	if prov == "" {
		return ClassResolution{}, false, fmt.Errorf("policy resolve %q: unknown driver %q", className, res.Driver)
	}

	emitResolutionEvents(exec, node.ID, className, res)
	persistResolution(exec, node.ID, className, res)

	return ClassResolution{
		Class:    className,
		Provider: prov,
		Model:    res.ModelID,
		Driver:   res.Driver,
		Backend:  be,
		Result:   res,
	}, true, nil
}

// EffectiveRouteForNode returns the (provider, model) the runtime would
// actually use for n at launch — class-resolved if agent_class= is set,
// else the legacy stylesheet attributes (llm_provider/llm_model).
// Suppresses event emission and resolution.json persistence by passing
// nil exec, so it's safe to call from preflight, validate, and other
// pre-run paths.
//
// Returns ("", "", err) when agent_class= is set but resolution fails —
// that is a real preflight-worthy failure (typo, no auth on this machine
// for any candidate in the chain) and callers should surface it as such
// rather than silently falling back to the stylesheet.
func EffectiveRouteForNode(n *model.Node) (provider, modelID string, err error) {
	if n == nil {
		return "", "", nil
	}
	cls, ok, err := ResolveAgentClass(n, nil, PolicyDeps{})
	if err != nil {
		return "", "", err
	}
	if ok {
		return cls.Provider, cls.Model, nil
	}
	provider = strings.TrimSpace(n.Attr("llm_provider", ""))
	modelID = strings.TrimSpace(n.Attr("llm_model", ""))
	if modelID == "" {
		modelID = strings.TrimSpace(n.Attr("model", ""))
	}
	return provider, modelID, nil
}

// resolutionRecord is the on-disk schema for <stage_dir>/resolution.json,
// matching plan §6.4. Best-effort: missing/nil exec or missing logs_root
// silently skip the write — the in-memory ResolveResult is still returned
// to the caller and the progress event still fires.
type resolutionRecord struct {
	SchemaVersion string            `json:"schema_version"`
	NodeID        string            `json:"node_id"`
	WorkflowID    string            `json:"workflow_id,omitempty"`
	Resolution    resolutionDetails `json:"resolution"`
}

type resolutionDetails struct {
	Requested     resolutionRequested `json:"requested"`
	Resolved      resolutionResolved  `json:"resolved"`
	FallbackRank  int                 `json:"fallback_rank"`
	Skipped       []resolutionSkipped `json:"skipped"`
	PolicyVersion string              `json:"policy_version"`
	PolicySource  string              `json:"policy_source,omitempty"`
	OverrideMode  string              `json:"override_mode,omitempty"`
	ResolvedAt    string              `json:"resolved_at"`
}

type resolutionRequested struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type resolutionResolved struct {
	ModelID    string         `json:"model_id"`
	Driver     string         `json:"driver"`
	Transport  string         `json:"transport"`
	AuthMethod string         `json:"auth_method"`
	AuthSource string         `json:"auth_source,omitempty"`
	Auth       resolutionAuth `json:"auth"`
	TurnCodec  string         `json:"turn_codec"`
}

// resolutionAuth carries the full binding.Snapshot identity for forensic
// observability. The flat AuthMethod/AuthSource fields are kept alongside
// for compatibility with consumers that haven't migrated.
type resolutionAuth struct {
	ChainName    string                  `json:"chain_name"`
	Method       string                  `json:"method"`
	Provider     string                  `json:"provider"`
	Source       resolutionAuthSource    `json:"source"`
	FallbackRank int                     `json:"fallback_rank"`
	Skipped      []resolutionAuthSkipped `json:"skipped,omitempty"`
}

type resolutionAuthSource struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
	Tool string `json:"tool,omitempty"`
}

type resolutionAuthSkipped struct {
	Source resolutionAuthSource `json:"source"`
	Reason string               `json:"reason"`
}

type resolutionSkipped struct {
	Rank    int    `json:"rank"`
	ModelID string `json:"model_id"`
	Driver  string `json:"driver"`
	Reason  string `json:"reason"`
}

func persistResolution(exec *Execution, nodeID, className string, res policy.ResolveResult) {
	if exec == nil {
		return
	}
	logsRoot := strings.TrimSpace(exec.LogsRoot)
	if logsRoot == "" && exec.Engine != nil {
		logsRoot = strings.TrimSpace(exec.Engine.LogsRoot)
	}
	if logsRoot == "" || nodeID == "" {
		return
	}

	requestType := res.RequestType
	if requestType == "" {
		requestType = "class"
	}
	requestValue := res.RequestValue
	if requestValue == "" {
		requestValue = className
	}

	resolvedAt := res.ResolvedAt
	if resolvedAt.IsZero() {
		resolvedAt = time.Now().UTC()
	}

	skipped := make([]resolutionSkipped, 0, len(res.Skipped))
	for _, s := range res.Skipped {
		skipped = append(skipped, resolutionSkipped{
			Rank:    s.Rank,
			ModelID: s.ModelID,
			Driver:  s.Driver,
			Reason:  s.Reason,
		})
	}

	authSkipped := make([]resolutionAuthSkipped, 0, len(res.AuthSnapshot.Skipped))
	for _, s := range res.AuthSnapshot.Skipped {
		authSkipped = append(authSkipped, resolutionAuthSkipped{
			Source: resolutionAuthSource{
				Kind: string(s.Source.Kind),
				Name: s.Source.Name,
				Tool: s.Source.Tool,
			},
			Reason: s.Reason,
		})
	}

	rec := resolutionRecord{
		SchemaVersion: "1",
		NodeID:        nodeID,
		WorkflowID:    graphNameForExec(exec),
		Resolution: resolutionDetails{
			Requested: resolutionRequested{
				Type:  requestType,
				Value: requestValue,
			},
			Resolved: resolutionResolved{
				ModelID:    res.ModelID,
				Driver:     res.Driver,
				Transport:  res.Transport,
				AuthMethod: res.AuthMethod(),
				AuthSource: res.AuthSource(),
				Auth: resolutionAuth{
					ChainName: res.AuthSnapshot.ChainName,
					Method:    string(res.AuthSnapshot.Method),
					Provider:  res.AuthSnapshot.Provider,
					Source: resolutionAuthSource{
						Kind: string(res.AuthSnapshot.Source.Kind),
						Name: res.AuthSnapshot.Source.Name,
						Tool: res.AuthSnapshot.Source.Tool,
					},
					FallbackRank: res.AuthSnapshot.FallbackRank,
					Skipped:      authSkipped,
				},
				TurnCodec: res.HistorySink,
			},
			FallbackRank:  res.FallbackRank,
			Skipped:       skipped,
			PolicyVersion: res.PolicyVersion,
			PolicySource:  res.PolicySource,
			OverrideMode:  res.OverrideMode,
			ResolvedAt:    resolvedAt.Format(time.RFC3339Nano),
		},
	}

	stageDir := filepath.Join(logsRoot, nodeID)
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(stageDir, "resolution.json"), append(b, '\n'), 0o644)
}

// DefaultBindingResolver constructs the production binding.Resolver from
// the user/project auth.toml + live detection. Returns ErrNoConfig when no
// config exists — callers (prelaunch, agent dispatch) surface this with
// the `kilroy auth init` remediation.
//
// projectRoot may be empty (no project override). Tests inject their own
// resolver via PolicyDeps.Resolver instead of calling this.
//
// IMPORTANT: at execution time, do NOT call this against the run worktree
// to materialize the selected credential. The prelaunch snapshot is the
// authoritative route; reloading auth.toml from the worktree breaks the
// freeze (a worktree branch may not have the project auth.toml the
// snapshot was resolved against, causing silent drift or hard fail).
// Use BindSnapshot for that path.
func DefaultBindingResolver(projectRoot string) (*binding.Resolver, error) {
	cfg, err := binding.LoadConfig(projectRoot)
	if err != nil {
		return nil, err
	}
	view := binding.AuthListView{List: auth.ListAll("", auth.DefaultDetectors())}
	return binding.NewResolver(&cfg, view), nil
}

// BindSnapshot materializes a frozen prelaunch credential without reloading
// auth.toml. The Snapshot itself carries the chain identity + selected
// source; binding.Resolver.Bind only needs a live DetectionView to verify
// the source is still usable (env var still set, CLI session still ok).
//
// This is the load-bearing piece of the "prelaunch snapshot is
// authoritative" invariant: at execution time, no path that materializes
// the selected credential is allowed to consult the worktree's
// `.kilroy/auth.toml`. Drift between prelaunch and execution (different
// project layer, missing project layer, mutated chain) cannot silently
// re-route the request.
func BindSnapshot(snap binding.Snapshot) (binding.Credential, error) {
	view := binding.AuthListView{List: auth.ListAll("", auth.DefaultDetectors())}
	resolver := binding.NewResolver(&binding.Config{}, view)
	return resolver.Bind(snap)
}

// projectRootForExec returns the worktree directory if available — used as
// the project root for auth config discovery. Empty string when exec is nil
// (e.g. preflight standalone calls); the binding loader treats empty as
// "no project layer."
func projectRootForExec(exec *Execution) string {
	if exec == nil {
		return ""
	}
	if root := strings.TrimSpace(exec.WorktreeDir); root != "" {
		return root
	}
	return ""
}

// authSourceIdentifier returns the env var name or CLI tool name for a
// binding.Source — used in progress events / artifacts where the kind is
// already separately recorded.
func authSourceIdentifier(s binding.Source) string {
	switch s.Kind {
	case binding.SourceEnvVar:
		return s.Name
	case binding.SourceCLISession:
		return s.Tool
	}
	return ""
}

// emitResolutionEvents fires policy_class_resolved + auth_credential_selected
// on progress.ndjson. Used by both the fresh-resolve path and the
// frozen-snapshot path so observers see identical events regardless of
// whether the route was just resolved or read from prelaunch.
func emitResolutionEvents(exec *Execution, nodeID, className string, res policy.ResolveResult) {
	if exec == nil || exec.Engine == nil {
		return
	}
	exec.Engine.appendProgress(map[string]any{
		"event":         "policy_class_resolved",
		"node_id":       nodeID,
		"class":         className,
		"model":         res.ModelID,
		"driver":        res.Driver,
		"fallback_rank": res.FallbackRank,
		"policy_source": res.PolicySource,
		"override_mode": res.OverrideMode,
	})
	exec.Engine.appendProgress(map[string]any{
		"event":       "auth_credential_selected",
		"node_id":     nodeID,
		"provider":    res.AuthSnapshot.Provider,
		"method":      string(res.AuthSnapshot.Method),
		"chain_name":  res.AuthSnapshot.ChainName,
		"source_kind": string(res.AuthSnapshot.Source.Kind),
		"source_name": authSourceIdentifier(res.AuthSnapshot.Source),
	})
}
