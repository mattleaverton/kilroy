// Snapshot, watch (polling), and --latest helpers for `kilroy status`.
// One-shot snapshot reads runstate.json; --watch reprints it on an
// interval; --latest finds the most recently modified run via the run
// DB then falls back to the XDG state path. v2 §2 (polling-not-
// streaming) — the legacy tail-mode --follow / -f / --raw / --cxdb
// flags were removed.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
	"github.com/danshapiro/kilroy/internal/attractor/runstate"
)

func evStr(ev map[string]any, key string) string {
	v, ok := ev[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func isTerminal(finalPath string) bool {
	_, err := os.Stat(finalPath)
	return err == nil
}

func readPID(pidPath string) int {
	b, err := os.ReadFile(pidPath)
	if err != nil {
		return 0
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return 0
	}
	var pid int
	if _, err := fmt.Sscanf(raw, "%d", &pid); err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func printFinalSummary(finalPath string, w io.Writer) {
	b, err := os.ReadFile(finalPath)
	if err != nil {
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return
	}
	status := evStr(doc, "status")
	fmt.Fprintf(w, "\nrun completed: %s\n", status)
	if reason := evStr(doc, "failure_reason"); reason != "" {
		fmt.Fprintf(w, "failure_reason: %s\n", reason)
	}
}

// latestRunLogsRoot finds the most recently modified run directory under the
// default XDG state path.
func latestRunFromDB() string {
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		return ""
	}
	defer db.Close()
	run, err := db.LatestRun()
	if err != nil || run == nil {
		return ""
	}
	return strings.TrimSpace(run.LogsRoot)
}

func latestRunLogsRoot() (string, error) {
	// Try RunDB first for instant lookup.
	if logsRoot := latestRunFromDB(); logsRoot != "" {
		return logsRoot, nil
	}

	// Fall back to filesystem scan.
	stateHome := strings.TrimSpace(os.Getenv("XDG_STATE_HOME"))
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	runsDir := filepath.Join(stateHome, "kilroy", "attractor", "runs")

	entries, err := os.ReadDir(runsDir)
	if err != nil {
		return "", fmt.Errorf("no runs found in %s: %w", runsDir, err)
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("no runs found in %s", runsDir)
	}

	type dirEntry struct {
		name    string
		modTime time.Time
	}
	var dirs []dirEntry
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		dirs = append(dirs, dirEntry{name: e.Name(), modTime: info.ModTime()})
	}
	if len(dirs) == 0 {
		return "", fmt.Errorf("no run directories found in %s", runsDir)
	}

	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].modTime.After(dirs[j].modTime)
	})

	return filepath.Join(runsDir, dirs[0].name), nil
}

// runWatchStatus polls the snapshot every interval and reprints it with
// screen clearing. Exits when the run reaches a terminal state.
func runWatchStatus(logsRoot string, stdout io.Writer, stderr io.Writer, asJSON bool, verbose bool, intervalSec int) int {
	if intervalSec <= 0 {
		intervalSec = 2
	}
	interval := time.Duration(intervalSec) * time.Second

	for {
		// Clear screen (ANSI escape).
		fmt.Fprint(stdout, "\033[2J\033[H")

		code := printSnapshot(logsRoot, stdout, stderr, asJSON, verbose)
		if code != 0 {
			return code
		}

		fmt.Fprintf(stdout, "\nrefreshing every %ds (ctrl-c to stop)\n", intervalSec)

		// Check if terminal.
		finalPath := filepath.Join(logsRoot, "final.json")
		if isTerminal(finalPath) {
			return 0
		}

		time.Sleep(interval)
	}
}

// printSnapshot loads and prints the current snapshot. Same as the one-shot
// path in runAttractorStatus but extracted for reuse.
func printSnapshot(logsRoot string, stdout io.Writer, stderr io.Writer, asJSON bool, verbose bool) int {
	snapshot, err := loadSnapshot(logsRoot)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	if verbose {
		if err := runstate.ApplyVerbose(snapshot); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(snapshot); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(stdout, "state=%s\n", snapshot.State)
	fmt.Fprintf(stdout, "run_id=%s\n", snapshot.RunID)
	fmt.Fprintf(stdout, "node=%s\n", snapshot.CurrentNodeID)
	fmt.Fprintf(stdout, "event=%s\n", snapshot.LastEvent)
	if snapshot.CurrentAttempt > 0 {
		fmt.Fprintf(stdout, "attempt=%d/%d\n", snapshot.CurrentAttempt, snapshot.MaxAttempts)
	}
	fmt.Fprintf(stdout, "pid=%d\n", snapshot.PID)
	fmt.Fprintf(stdout, "pid_alive=%t\n", snapshot.PIDAlive)
	if !snapshot.LastEventAt.IsZero() {
		fmt.Fprintf(stdout, "last_event_at=%s\n", snapshot.LastEventAt.UTC().Format(time.RFC3339Nano))
	}
	if snapshot.FailureReason != "" {
		fmt.Fprintf(stdout, "failure_reason=%s\n", snapshot.FailureReason)
	}

	if verbose {
		printVerboseSnapshot(stdout, snapshot)
	}
	return 0
}

func printVerboseSnapshot(w io.Writer, s *runstate.Snapshot) {
	if len(s.CompletedNodes) > 0 {
		fmt.Fprintf(w, "completed_nodes=%s\n", strings.Join(s.CompletedNodes, ","))
	}
	if len(s.RetryCounts) > 0 {
		parts := make([]string, 0, len(s.RetryCounts))
		for node, count := range s.RetryCounts {
			parts = append(parts, fmt.Sprintf("%s:%d", node, count))
		}
		sort.Strings(parts)
		fmt.Fprintf(w, "retry_counts=%s\n", strings.Join(parts, ","))
	}
	if s.FinalCommitSHA != "" {
		fmt.Fprintf(w, "final_commit_sha=%s\n", s.FinalCommitSHA)
	}
	if s.CXDBContextID != "" {
		fmt.Fprintf(w, "cxdb_context_id=%s\n", s.CXDBContextID)
	}

	if len(s.StageTrace) > 0 || len(s.EdgeTrace) > 0 {
		fmt.Fprintln(w, "\n--- stage trace ---")
		si, ei := 0, 0
		for si < len(s.StageTrace) || ei < len(s.EdgeTrace) {
			if si < len(s.StageTrace) {
				sa := s.StageTrace[si]
				line := fmt.Sprintf("  %-24s %-8s attempt %d/%d", sa.NodeID, sa.Status, sa.Attempt, sa.MaxAttempts)
				if sa.FailureReason != "" {
					line += "  " + sa.FailureReason
				}
				fmt.Fprintln(w, line)
				si++
				// Print any edges that follow this stage
				for ei < len(s.EdgeTrace) && s.EdgeTrace[ei].From == sa.NodeID {
					et := s.EdgeTrace[ei]
					cond := ""
					if et.Condition != "" {
						cond = " (" + et.Condition + ")"
					}
					fmt.Fprintf(w, "    → %-20s%s\n", et.To, cond)
					ei++
				}
			} else {
				// Remaining edges
				et := s.EdgeTrace[ei]
				cond := ""
				if et.Condition != "" {
					cond = " (" + et.Condition + ")"
				}
				fmt.Fprintf(w, "    → %-20s%s\n", et.To, cond)
				ei++
			}
		}
	}

	if s.PostmortemText != "" {
		fmt.Fprintf(w, "\n--- postmortem (%s) ---\n%s\n", runScopedVerboseArtifactLabel(s.RunID, "postmortem_latest.md"), s.PostmortemText)
	}
	if s.ReviewText != "" {
		fmt.Fprintf(w, "\n--- review (%s) ---\n%s\n", runScopedVerboseArtifactLabel(s.RunID, "review_final.md"), s.ReviewText)
	}
}

func runScopedVerboseArtifactLabel(runID string, fileName string) string {
	id := strings.TrimSpace(runID)
	if id == "" {
		id = "<run_id>"
	}
	return filepath.ToSlash(filepath.Join("worktree", ".ai", "runs", id, strings.TrimSpace(fileName)))
}
