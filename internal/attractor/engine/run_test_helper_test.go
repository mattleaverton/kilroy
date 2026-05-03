package engine

import (
	"context"
	"os"
	"testing"
)

// runForTest wraps Run() with two test-friendly behaviours:
//
//  1. It stamps Labels{"source":"test","test_name":t.Name()} onto the run's
//     manifest.json so the run can be identified and pruned by
//     `kilroy runs prune --label source=test`.
//
//  2. It registers a t.Cleanup that removes LogsRoot only when the test
//     *passes*. On test failure the artifacts are left in place for inspection.
//
// The cleanup is registered unconditionally by pre-allocating the RunID and
// LogsRoot before calling Run(), so it fires even when Run() returns a nil
// Result (e.g., when a test deliberately exercises a fatal-error code path).
func runForTest(t *testing.T, ctx context.Context, dot []byte, opts RunOptions) (*Result, error) {
	t.Helper()
	if opts.Labels == nil {
		opts.Labels = map[string]string{}
	}
	opts.Labels["source"] = "test"
	opts.Labels["test_name"] = t.Name()

	// Pre-allocate RunID/LogsRoot so we know the artifact path regardless of
	// whether Run() returns a nil Result.
	if opts.RunID == "" {
		if id, err := NewRunID(); err == nil {
			opts.RunID = id
		}
	}
	if opts.LogsRoot == "" && opts.RunID != "" {
		opts.LogsRoot = defaultLogsRoot(opts.RunID)
	}

	if opts.LogsRoot != "" {
		logsRoot := opts.LogsRoot
		t.Cleanup(func() {
			if !t.Failed() {
				_ = os.RemoveAll(logsRoot)
			}
		})
	}

	return Run(ctx, dot, opts)
}
