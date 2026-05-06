package runcontrol

import (
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
)

// StaleRun is an age-based running row eligible for manual interruption.
type StaleRun struct {
	RunID      string    `json:"run_id"`
	GraphName  string    `json:"graph_name,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	AgeSeconds int64     `json:"age_seconds"`
}

// DetectStaleRuns returns running rows started before maxAge. This is an
// age-only DB reconciliation aid; use DetectZombies for PID-aware cleanup.
func DetectStaleRuns(db *rundb.DB, maxAge time.Duration) ([]StaleRun, error) {
	runs, err := db.ListRuns(rundb.ListFilter{Status: "running"})
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()
	out := make([]StaleRun, 0)
	for _, r := range runs {
		if r.StartedAt.IsZero() || !r.StartedAt.Before(cutoff) {
			continue
		}
		out = append(out, StaleRun{
			RunID:      r.RunID,
			GraphName:  r.GraphName,
			StartedAt:  r.StartedAt,
			AgeSeconds: int64(now.Sub(r.StartedAt).Seconds()),
		})
	}
	return out, nil
}

// ApplyStaleRuns marks age-matched running DB rows as interrupted.
func ApplyStaleRuns(db *rundb.DB, maxAge time.Duration) (int, error) {
	return db.ReconcileStaleRuns(maxAge)
}
