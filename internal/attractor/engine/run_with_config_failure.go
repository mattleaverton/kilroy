package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// persistBootstrapFailure writes terminal artifacts for a run that failed before
// the engine started executing nodes (e.g. graph validation error, preflight
// rejection, config defaulting failure). Without this, detached runs whose
// bootstrap fails leave a status=running DB row and no final.json — the zombie
// pathology described in plan §13.1.
//
// Best-effort: all helper errors are silently discarded so they cannot mask the
// caller's original bootstrap error. Caller still returns that error.
//
// Skipped entirely if logsRoot is empty (synchronous in-process Run() call with
// no on-disk audit destination).
func persistBootstrapFailure(logsRoot, runID string, runDB RunDBWriter, err error) {
	logsRoot = strings.TrimSpace(logsRoot)
	if logsRoot == "" || err == nil {
		return
	}
	runID = strings.TrimSpace(runID)

	reason := classifyBootstrapFailureReason(err)

	final := runtime.FinalOutcome{
		Timestamp:     time.Now().UTC(),
		Status:        runtime.FinalFail,
		RunID:         runID,
		FailureReason: reason,
	}
	finalPath := filepath.Join(logsRoot, "final.json")
	_ = final.Save(finalPath)

	appendBootstrapFailureProgressEvent(logsRoot, reason)

	if runDB != nil && runID != "" {
		_ = runDB.RecordRunComplete(runID, "fail", reason, "", nil)
	}
}

// classifyBootstrapFailureReason maps a bootstrap error string to a stable
// failure_reason code. Codes are terse and machine-parseable so external
// tooling can route on them.
func classifyBootstrapFailureReason(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "validation failed"):
		return "validation_failed"
	case strings.Contains(msg, "preflight"):
		return "preflight_failed"
	default:
		return "launch_failed"
	}
}

// appendBootstrapFailureProgressEvent appends a run_failed terminal event to
// progress.ndjson, mirroring the shape Engine.emitTerminalProgressEvent emits
// for engine-driven failures (engine.go:2073-2081). Best-effort.
func appendBootstrapFailureProgressEvent(logsRoot, reason string) {
	ev := map[string]any{
		"event":  "run_failed",
		"status": "fail",
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if reason != "" {
		ev["reason"] = reason
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return
	}
	path := filepath.Join(logsRoot, "progress.ndjson")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}
