package runcontrol

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/procutil"
	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

// ZombieRun is a running DB row whose PID no longer appears to be a live
// Kilroy process.
type ZombieRun struct {
	Run    rundb.RunSummary `json:"-"`
	RunID  string           `json:"run_id"`
	PID    int              `json:"pid"`
	Reason string           `json:"reason"`
}

// ZombieMutation records the outcome of a requested zombie-run mutation.
type ZombieMutation struct {
	RunID   string `json:"run_id"`
	PID     int    `json:"pid"`
	Reason  string `json:"reason"`
	Mutated bool   `json:"mutated"`
	DryRun  bool   `json:"dry_run"`
	Error   string `json:"error,omitempty"`
}

// DetectZombies returns status=running rows whose run.pid is dead or appears
// to have been recycled by a non-Kilroy process. Ambiguous rows are skipped.
func DetectZombies(db *rundb.DB) ([]ZombieRun, error) {
	runs, err := db.ListRuns(rundb.ListFilter{Status: "running"})
	if err != nil {
		return nil, err
	}
	found := make([]ZombieRun, 0)
	for _, r := range runs {
		pidPath := filepath.Join(r.LogsRoot, "run.pid")
		pid := ReadPID(pidPath)
		orphan, reason := IsZombieRun(pid, pidPath)
		if !orphan {
			continue
		}
		found = append(found, ZombieRun{Run: r, RunID: r.RunID, PID: pid, Reason: reason})
	}
	return found, nil
}

// DryRunZombieMutations converts detections to JSON-ready mutation records
// without changing the DB or run artifacts.
func DryRunZombieMutations(zombies []ZombieRun) []ZombieMutation {
	out := make([]ZombieMutation, 0, len(zombies))
	for _, z := range zombies {
		out = append(out, ZombieMutation{
			RunID:   z.RunID,
			PID:     z.PID,
			Reason:  z.Reason,
			Mutated: false,
			DryRun:  true,
		})
	}
	return out
}

// ApplyZombieMutations marks detected zombie runs as failed in the DB and
// writes terminal artifacts that match CLI-run expectations.
func ApplyZombieMutations(db *rundb.DB, zombies []ZombieRun) []ZombieMutation {
	results := make([]ZombieMutation, 0, len(zombies))
	for _, z := range zombies {
		err := db.CompleteRun(z.RunID, "fail", "orphan_detected", "", nil)
		mutated := err == nil
		if mutated {
			writeZombieFinalJSON(z.Run.LogsRoot, z.RunID)
			appendZombieProgressEvent(z.Run.LogsRoot, z.RunID)
		}
		result := ZombieMutation{
			RunID:   z.RunID,
			PID:     z.PID,
			Reason:  z.Reason,
			Mutated: mutated,
			DryRun:  false,
		}
		if err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
	}
	return results
}

// IsZombieRun returns true when the pid/pidPath pair is safe to treat as an
// orphan. False positives are worse than missed cleanup, so unknown states are
// left alone.
func IsZombieRun(pid int, pidPath string) (bool, string) {
	if pid <= 0 {
		return false, ""
	}
	if !procutil.PIDAlive(pid) {
		return true, fmt.Sprintf("pid %d not alive", pid)
	}
	if pidCmdlineLooksLikeKilroy(pid) {
		return false, ""
	}
	info, err := os.Stat(pidPath)
	if err != nil {
		return false, ""
	}
	const staleThreshold = 10 * time.Minute
	if time.Since(info.ModTime()) > staleThreshold {
		return true, fmt.Sprintf("pid %d alive but not kilroy (recycled; pidfile >10m old)", pid)
	}
	return false, ""
}

// ReadPID reads a run.pid file. It returns 0 when the file is absent or invalid.
func ReadPID(pidPath string) int {
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

func pidCmdlineLooksLikeKilroy(pid int) bool {
	args, err := ReadPIDCmdline(pid)
	if err != nil {
		return true
	}
	if len(args) == 0 {
		return true
	}
	exe := strings.ToLower(filepath.Base(args[0]))
	return strings.Contains(exe, "kilroy")
}

func writeZombieFinalJSON(logsRoot, runID string) {
	now := time.Now().UTC()
	final := map[string]any{
		"timestamp":            now.Format(time.RFC3339Nano),
		"status":               "fail",
		"run_id":               runID,
		"final_git_commit_sha": "",
		"failure_reason":       "orphan_detected",
		"cxdb_context_id":      "",
		"cxdb_head_turn_id":    "",
	}
	data, err := json.MarshalIndent(final, "", "  ")
	if err != nil {
		return
	}
	path := filepath.Join(logsRoot, "final.json")
	_ = os.WriteFile(path, append(data, '\n'), 0o644)
}

func appendZombieProgressEvent(logsRoot, runID string) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	eventID := fmt.Sprintf("%x", b)

	ev := map[string]any{
		"event":  "run_failed",
		"status": "fail",
		"reason": "orphan_detected",
		"run_id": runID,
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"id":     eventID,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	path := filepath.Join(logsRoot, "progress.ndjson")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.Write(append(data, '\n'))
}

// ReadPIDCmdline reads process argv for PID identity checks.
func ReadPIDCmdline(pid int) ([]string, error) {
	if !procutil.ProcFSAvailable() {
		return readPIDCmdlineFromPS(pid)
	}
	path := filepath.Join("/proc", strconv.Itoa(pid), "cmdline")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseCmdlineParts(string(b), "\x00"), nil
}

func readPIDCmdlineFromPS(pid int) ([]string, error) {
	out, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return nil, err
	}
	cmdline := strings.TrimSpace(string(out))
	if cmdline == "" {
		return nil, fmt.Errorf("empty command line")
	}
	return parseCmdlineParts(cmdline, " "), nil
}

func parseCmdlineParts(raw string, sep string) []string {
	parts := strings.Split(raw, sep)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}
