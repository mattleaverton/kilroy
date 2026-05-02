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
	"github.com/danshapiro/kilroy/internal/policy"
)

// PolicyDeps carries injectable policy data and machine-state collection.
// Zero values mean "use production defaults" (policy.Load /
// policy.CollectMachineState). Tests inject stubs.
type PolicyDeps struct {
	Load    func() (*policy.Data, error)
	Collect func() policy.MachineState
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

// ResolveAgentClass resolves the class attribute on a node via the policy
// resolver. If the node has no class attribute, returns ok=false with no
// error. On a successful class resolution this is the single source of truth
// that both AgentRouter (API path) and TmuxAgentHandler (tmux path) consume,
// and a policy_class_resolved progress event is emitted.
func ResolveAgentClass(node *model.Node, exec *Execution, deps PolicyDeps) (ClassResolution, bool, error) {
	className := strings.TrimSpace(node.Attr("class", ""))
	if className == "" {
		return ClassResolution{}, false, nil
	}

	load := deps.Load
	if load == nil {
		load = policy.Load
	}
	data, err := load()
	if err != nil {
		return ClassResolution{}, false, fmt.Errorf("policy load: %w", err)
	}

	collect := deps.Collect
	if collect == nil {
		collect = policy.CollectMachineState
	}
	state := collect()

	res, err := policy.Resolve(policy.ResolveRequest{
		ClassID:    className,
		NodeID:     node.ID,
		WorkflowID: graphNameForExec(exec),
	}, data, state)
	if err != nil {
		return ClassResolution{}, false, fmt.Errorf("policy resolve %q: %w", className, err)
	}

	prov, be := providerAndBackendForDriver(res.Driver)
	if prov == "" {
		return ClassResolution{}, false, fmt.Errorf("policy resolve %q: unknown driver %q", className, res.Driver)
	}

	if exec != nil && exec.Engine != nil {
		exec.Engine.appendProgress(map[string]any{
			"event":         "policy_class_resolved",
			"node_id":       node.ID,
			"class":         className,
			"model":         res.ModelID,
			"driver":        res.Driver,
			"fallback_rank": res.FallbackRank,
		})
	}

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
// actually use for n at launch — class-resolved if class= is set, else
// the legacy stylesheet attributes (llm_provider/llm_model). Suppresses
// event emission and resolution.json persistence by passing nil exec, so
// it's safe to call from preflight, validate, and other pre-run paths.
//
// Returns ("", "", err) when class= is set but resolution fails — that is
// a real preflight-worthy failure (e.g., no auth on this machine for any
// candidate in the chain) and callers should surface it as such rather
// than silently falling back to the stylesheet.
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
	SchemaVersion string             `json:"schema_version"`
	NodeID        string             `json:"node_id"`
	WorkflowID    string             `json:"workflow_id,omitempty"`
	Resolution    resolutionDetails  `json:"resolution"`
}

type resolutionDetails struct {
	Requested     resolutionRequested `json:"requested"`
	Resolved      resolutionResolved  `json:"resolved"`
	FallbackRank  int                 `json:"fallback_rank"`
	Skipped       []resolutionSkipped `json:"skipped"`
	PolicyVersion string              `json:"policy_version"`
	ResolvedAt    string              `json:"resolved_at"`
}

type resolutionRequested struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type resolutionResolved struct {
	ModelID    string `json:"model_id"`
	Driver     string `json:"driver"`
	Transport  string `json:"transport"`
	AuthMethod string `json:"auth_method"`
	AuthSource string `json:"auth_source,omitempty"`
	TurnCodec  string `json:"turn_codec"`
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
				AuthMethod: res.AuthMethod,
				AuthSource: res.AuthSource,
				TurnCodec:  res.HistorySink,
			},
			FallbackRank:  res.FallbackRank,
			Skipped:       skipped,
			PolicyVersion: res.PolicyVersion,
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
