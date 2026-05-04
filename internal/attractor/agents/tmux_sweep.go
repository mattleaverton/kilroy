// Best-effort sweep of stale tmux sessions on the kilroy socket.
// Sessions whose run_id resolves to a completed run record are killed.
// Errors are silently swallowed — sweeping is opportunistic; a tmux
// server that's missing or a rundb that's unavailable is normal at
// launch time and must not block the new run.
package agents

import (
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/agents/tmux"
)

// RunStatusLookup reports whether the given run_id is in a terminal
// state (success, fail, canceled, error). Implementations should
// return (false, nil) for unknown run_ids — the caller treats those
// as "do not kill, leave for an in-flight or external run."
type RunStatusLookup func(runID string) (terminal bool, err error)

// SweepStaleTmuxSessions lists kilroy-* tmux sessions and kills the
// ones that map to terminal run_ids. Returns the count killed and any
// "unrecoverable" error (none today — all errors are best-effort).
//
// Session-name shape: "kilroy-<RUN_ID>-<NODE_ID>". Sessions that don't
// match this shape are ignored. Sessions whose run_id is unknown to
// the lookup are also ignored — they may belong to an in-flight run
// the registry doesn't yet know about.
func SweepStaleTmuxSessions(mgr *tmux.Manager, lookup RunStatusLookup) (killed int, err error) {
	if mgr == nil || lookup == nil {
		return 0, nil
	}
	sessions, listErr := mgr.ListSessions()
	if listErr != nil {
		return 0, nil
	}
	for _, name := range sessions {
		runID := runIDFromSessionName(name)
		if runID == "" {
			continue
		}
		terminal, lookupErr := lookup(runID)
		if lookupErr != nil || !terminal {
			continue
		}
		if killErr := mgr.DestroySession(name); killErr == nil {
			killed++
		}
	}
	return killed, nil
}

// runIDFromSessionName parses "kilroy-<RUN_ID>-<NODE_ID>" and returns
// the run id, or "" if the session name doesn't match. Run IDs are
// ULIDs (26 chars, [0-9A-Z]) so they have no embedded dashes; the
// first dash after "kilroy-" terminates the run id.
func runIDFromSessionName(name string) string {
	const prefix = "kilroy-"
	if !strings.HasPrefix(name, prefix) {
		return ""
	}
	rest := name[len(prefix):]
	dash := strings.IndexByte(rest, '-')
	if dash <= 0 {
		return ""
	}
	return rest[:dash]
}
