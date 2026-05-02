// Shared policy class-resolution helper consumed by both AgentRouter (API path)
// and TmuxAgentHandler (tmux path). Routes a node's class= attribute through
// internal/policy and emits the policy_class_resolved progress event.
package engine

import (
	"fmt"
	"strings"

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

	return ClassResolution{
		Class:    className,
		Provider: prov,
		Model:    res.ModelID,
		Driver:   res.Driver,
		Backend:  be,
		Result:   res,
	}, true, nil
}
