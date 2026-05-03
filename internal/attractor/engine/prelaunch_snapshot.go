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
	ClassName     string `json:"class_name"`
	ModelID       string `json:"model_id"`
	Driver        string `json:"driver"`
	Transport     string `json:"transport"`
	HistorySink   string `json:"history_sink"`
	FallbackRank  int    `json:"fallback_rank"`
	PolicyVersion string `json:"policy_version"`

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
	return preLaunchNodeSnap{
		ClassName:     className,
		ModelID:       res.ModelID,
		Driver:        res.Driver,
		Transport:     res.Transport,
		HistorySink:   res.HistorySink,
		FallbackRank:  res.FallbackRank,
		PolicyVersion: res.PolicyVersion,
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
	}
}
