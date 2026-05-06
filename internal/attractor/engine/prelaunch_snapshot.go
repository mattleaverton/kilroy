// Per-node prelaunch snapshots. Plan §5: prelaunch freezes the resolved
// route + auth identity per agentic node; execution reads from the
// snapshot rather than re-running policy.Resolve. This is what makes
// prelaunch authoritative: env or config drift between prelaunch and
// node execution cannot change the route silently.
//
// Snapshots live at <logs_root>/prelaunch_snapshots.json keyed by node_id.
// One file per run; written atomically by ValidatePreLaunch after every
// agentic node has been resolved. Loaded by ResolveAgentClass before it
// considers running the resolver fresh.
//
// The serialized form carries everything needed to re-construct a
// policy.ResolveResult without re-resolving — model_id, driver,
// transport, history_sink, plus the binding.Snapshot for auth.

package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/policy"
)

// preLaunchSnapshotsFile is the filename used inside logs_root.
const preLaunchSnapshotsFile = "prelaunch_snapshots.json"

// preLaunchSnapshotStore is the on-disk shape: schema_version + a map
// keyed by node_id.
type preLaunchSnapshotStore struct {
	SchemaVersion string                       `json:"schema_version"`
	Snapshots     map[string]preLaunchNodeSnap `json:"snapshots"`
}

// preLaunchNodeSnap captures everything ResolveAgentClass needs from a
// frozen snapshot: the policy.ResolveResult fields plus the auth Snapshot
// (for binder Bind at execution time).
type preLaunchNodeSnap struct {
	Source   string      `json:"source,omitempty"`
	Provider string      `json:"provider,omitempty"`
	Backend  BackendKind `json:"backend,omitempty"`

	ClassName     string `json:"class_name"`
	ModelID       string `json:"model_id"`
	Driver        string `json:"driver"`
	Transport     string `json:"transport"`
	HistorySink   string `json:"history_sink"`
	FallbackRank  int    `json:"fallback_rank"`
	PolicyVersion string `json:"policy_version"`
	PolicySource  string `json:"policy_source,omitempty"`
	OverrideMode  string `json:"override_mode,omitempty"`
	ResolvedAt    string `json:"resolved_at,omitempty"`

	// Auth carries the binding.Snapshot that the resolver picked.
	Auth snapAuth `json:"auth"`

	// Skipped records the policy candidates that were rejected (their
	// reasons), preserved so resolution.json at execution time matches
	// what prelaunch saw.
	Skipped []policy.SkipRecord `json:"skipped,omitempty"`
}

type snapAuth struct {
	ChainName    string                  `json:"chain_name"`
	Method       string                  `json:"method"`
	Provider     string                  `json:"provider"`
	Source       snapAuthSource          `json:"source"`
	FallbackRank int                     `json:"fallback_rank"`
	Skipped      []snapAuthSkippedSource `json:"skipped,omitempty"`
}

type snapAuthSource struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
	Tool string `json:"tool,omitempty"`
}

type snapAuthSkippedSource struct {
	Source snapAuthSource `json:"source"`
	Reason string         `json:"reason"`
}

// writePreLaunchSnapshots writes the per-node frozen snapshots to
// <logs_root>/prelaunch_snapshots.json. Best-effort; errors are returned
// but callers may downgrade to warnings since prelaunch.json is the
// human-facing report and snapshots is for execution.
func writePreLaunchSnapshots(logsRoot string, snaps map[string]preLaunchNodeSnap) error {
	if logsRoot == "" || len(snaps) == 0 {
		return nil
	}
	if err := os.MkdirAll(logsRoot, 0o755); err != nil {
		return err
	}
	store := preLaunchSnapshotStore{
		SchemaVersion: "1",
		Snapshots:     snaps,
	}
	b, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(logsRoot, preLaunchSnapshotsFile), append(b, '\n'), 0o644)
}

// preLaunchSnapshotsFileExists reports whether the snapshot file is
// present at the expected path inside logsRoot. Used by execution-side
// callers to decide between "prelaunch ran (snapshot is authoritative)"
// and "no prelaunch (legacy/test path; live resolve is fine)".
//
// Returns (false, nil) when logsRoot is empty.
func preLaunchSnapshotsFileExists(logsRoot string) (bool, error) {
	logsRoot = strings.TrimSpace(logsRoot)
	if logsRoot == "" {
		return false, nil
	}
	_, err := os.Stat(filepath.Join(logsRoot, preLaunchSnapshotsFile))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// LoadPreLaunchSnapshot returns the frozen snapshot for one node, or
// (nil, false, nil) when the snapshot file or this node's entry is
// absent. Errors are reserved for genuine parse failures.
//
// Callers (ResolveAgentClass) prefer the snapshot when present; if
// absent, they fall back to live resolution. That keeps tests, ad-hoc
// invocations, and pre-snapshot code paths working unchanged.
func LoadPreLaunchSnapshot(logsRoot, nodeID string) (*policy.ResolveResult, bool, error) {
	logsRoot = strings.TrimSpace(logsRoot)
	nodeID = strings.TrimSpace(nodeID)
	if logsRoot == "" || nodeID == "" {
		return nil, false, nil
	}
	path := filepath.Join(logsRoot, preLaunchSnapshotsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	var store preLaunchSnapshotStore
	if err := json.Unmarshal(data, &store); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", path, err)
	}
	snap, ok := store.Snapshots[nodeID]
	if !ok {
		return nil, false, nil
	}
	return snapToResolveResult(snap), true, nil
}

func LoadPreLaunchAgentRouteForExec(exec *Execution, nodeID string) (AgentRoute, bool, error) {
	logsRoot := ""
	if exec != nil {
		logsRoot = strings.TrimSpace(exec.LogsRoot)
		if logsRoot == "" && exec.Engine != nil {
			logsRoot = strings.TrimSpace(exec.Engine.LogsRoot)
		}
	}
	return LoadPreLaunchAgentRoute(logsRoot, nodeID)
}

func LoadPreLaunchAgentRoute(logsRoot, nodeID string) (AgentRoute, bool, error) {
	logsRoot = strings.TrimSpace(logsRoot)
	nodeID = strings.TrimSpace(nodeID)
	if logsRoot == "" || nodeID == "" {
		return AgentRoute{}, false, nil
	}
	exists, err := preLaunchSnapshotsFileExists(logsRoot)
	if err != nil {
		return AgentRoute{}, false, fmt.Errorf("stat prelaunch snapshots: %w", err)
	}
	if !exists {
		return AgentRoute{}, false, nil
	}
	path := filepath.Join(logsRoot, preLaunchSnapshotsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return AgentRoute{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	var store preLaunchSnapshotStore
	if err := json.Unmarshal(data, &store); err != nil {
		return AgentRoute{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	snap, ok := store.Snapshots[nodeID]
	if !ok {
		return AgentRoute{}, false, fmt.Errorf(
			"prelaunch_snapshots.json exists but has no entry for node %q — snapshot integrity broken; do not silently re-resolve",
			nodeID,
		)
	}
	route, err := snapToAgentRoute(nodeID, snap)
	if err != nil {
		return AgentRoute{}, false, err
	}
	return route, true, nil
}

func agentRouteToSnap(route AgentRoute) preLaunchNodeSnap {
	if route.ClassResult != nil {
		snap := resolveResultToSnap(route.Class, *route.ClassResult)
		snap.Source = route.Source
		snap.Provider = route.Provider
		snap.Backend = route.Backend
		return snap
	}
	return preLaunchNodeSnap{
		Source:   route.Source,
		Provider: route.Provider,
		ModelID:  route.Model,
		Driver:   route.Driver,
		Backend:  route.Backend,
	}
}

func snapToAgentRoute(nodeID string, snap preLaunchNodeSnap) (AgentRoute, error) {
	backend := snap.Backend
	provider := strings.TrimSpace(snap.Provider)
	if backend == "" && snap.Driver != "" {
		if p, be := providerAndBackendForDriver(snap.Driver); be != "" {
			backend = be
			if provider == "" {
				provider = p
			}
		}
	}
	if provider == "" {
		provider, _ = providerAndBackendForDriver(snap.Driver)
	}
	if snap.ClassName != "" {
		res := snapToResolveResult(snap)
		if provider == "" {
			return AgentRoute{}, fmt.Errorf("prelaunch snapshot has unknown driver %q", snap.Driver)
		}
		source := strings.TrimSpace(snap.Source)
		if source == "" {
			source = "policy_class:" + snap.ClassName
		}
		return AgentRoute{
			NodeID:      nodeID,
			Source:      source,
			Class:       snap.ClassName,
			Provider:    provider,
			Model:       snap.ModelID,
			Driver:      snap.Driver,
			Backend:     backend,
			ClassResult: res,
		}, nil
	}
	source := strings.TrimSpace(snap.Source)
	if source == "" && provider != "" {
		source = "llm_provider=" + provider
	}
	return AgentRoute{
		NodeID:   nodeID,
		Source:   source,
		Provider: provider,
		Model:    snap.ModelID,
		Driver:   snap.Driver,
		Backend:  backend,
	}, nil
}

// resolveResultToSnap converts a policy.ResolveResult into the on-disk
// frozen-snapshot shape.
func resolveResultToSnap(className string, res policy.ResolveResult) preLaunchNodeSnap {
	skipped := make([]snapAuthSkippedSource, 0, len(res.AuthSnapshot.Skipped))
	for _, s := range res.AuthSnapshot.Skipped {
		skipped = append(skipped, snapAuthSkippedSource{
			Source: snapAuthSource{
				Kind: string(s.Source.Kind),
				Name: s.Source.Name,
				Tool: s.Source.Tool,
			},
			Reason: s.Reason,
		})
	}
	resolvedAt := ""
	if !res.ResolvedAt.IsZero() {
		resolvedAt = res.ResolvedAt.UTC().Format(time.RFC3339Nano)
	}
	return preLaunchNodeSnap{
		Source:        "policy_class:" + className,
		ClassName:     className,
		ModelID:       res.ModelID,
		Driver:        res.Driver,
		Transport:     res.Transport,
		HistorySink:   res.HistorySink,
		FallbackRank:  res.FallbackRank,
		PolicyVersion: res.PolicyVersion,
		PolicySource:  res.PolicySource,
		OverrideMode:  res.OverrideMode,
		ResolvedAt:    resolvedAt,
		Auth: snapAuth{
			ChainName: res.AuthSnapshot.ChainName,
			Method:    string(res.AuthSnapshot.Method),
			Provider:  res.AuthSnapshot.Provider,
			Source: snapAuthSource{
				Kind: string(res.AuthSnapshot.Source.Kind),
				Name: res.AuthSnapshot.Source.Name,
				Tool: res.AuthSnapshot.Source.Tool,
			},
			FallbackRank: res.AuthSnapshot.FallbackRank,
			Skipped:      skipped,
		},
		Skipped: res.Skipped,
	}
}

// snapToResolveResult is the inverse — reconstructs a
// policy.ResolveResult from a frozen snapshot. Used by execution-side
// callers to skip live resolution.
func snapToResolveResult(snap preLaunchNodeSnap) *policy.ResolveResult {
	resolvedAt := time.Time{}
	if snap.ResolvedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, snap.ResolvedAt); err == nil {
			resolvedAt = t
		}
	}
	skipped := make([]binding.SkippedSource, 0, len(snap.Auth.Skipped))
	for _, s := range snap.Auth.Skipped {
		skipped = append(skipped, binding.SkippedSource{
			Source: binding.Source{
				Kind: binding.SourceKind(s.Source.Kind),
				Name: s.Source.Name,
				Tool: s.Source.Tool,
			},
			Reason: s.Reason,
		})
	}
	return &policy.ResolveResult{
		ModelID:     snap.ModelID,
		Driver:      snap.Driver,
		Transport:   snap.Transport,
		HistorySink: snap.HistorySink,
		AuthSnapshot: binding.Snapshot{
			ChainName: snap.Auth.ChainName,
			Method:    binding.Method(snap.Auth.Method),
			Provider:  snap.Auth.Provider,
			Source: binding.Source{
				Kind: binding.SourceKind(snap.Auth.Source.Kind),
				Name: snap.Auth.Source.Name,
				Tool: snap.Auth.Source.Tool,
			},
			FallbackRank: snap.Auth.FallbackRank,
			Skipped:      skipped,
		},
		RequestType:   "class",
		RequestValue:  snap.ClassName,
		FallbackRank:  snap.FallbackRank,
		Skipped:       snap.Skipped,
		PolicyVersion: snap.PolicyVersion,
		PolicySource:  snap.PolicySource,
		OverrideMode:  snap.OverrideMode,
		ResolvedAt:    resolvedAt,
	}
}
