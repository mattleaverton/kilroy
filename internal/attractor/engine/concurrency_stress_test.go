package engine

// concurrency_stress_test.go launches 12 (or -stress=N flag) concurrent Run() calls
// against a test_shim (fake provider) profile, all sharing one temporary parent
// git repo, and asserts:
//   - all runs succeed (no panics, no SQLITE_BUSY, no .git/index.lock errors)
//   - each run gets a unique logsRoot and git branch
//   - rundb has exactly one row per run
//   - total wall time stays under 60 s
//
// The test is gated by testing.Short() (skip if short).
//
// To run: go test -v -run TestBlock8_TwelveSiblingRuns ./internal/attractor/engine/ -stress=12 -timeout 120s

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

// stressFlag is the command-line flag for controlling concurrency level.
// Defaults to 12, can be overridden with -stress=N.
var stressFlag = flag.Int("stress", 12, "Number of concurrent sibling runs for stress test")

// getStressCount returns the number of concurrent runs to launch.
// Defaults to 12, can be overridden with -stress=N flag.
func getStressCount() int {
	// Use the flag value directly
	if *stressFlag > 0 {
		return *stressFlag
	}
	return 12
}

// simpleDOT is a minimal 3-node workflow that exercises rundb writes,
// git worktree creation, and basic execution without requiring real LLM calls.
// Uses tool_command nodes which work with test_shim profile.
var simpleDOT = []byte(`
digraph G {
  graph [goal="concurrency stress test"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  worker [shape=parallelogram, tool_command="echo 'hello from stress test'"]
  start -> worker
  worker -> exit [condition="outcome=success"]
}
`)

// runResult captures the outcome of a single sibling run
type runResult struct {
	index       int
	runID       string
	logsRoot    string
	worktreeDir string
	branch      string
	finalStatus string
	err         error
	duration    time.Duration
}

// TestBlock8_TwelveSiblingRuns launches concurrent runs against a shared parent repo
// and verifies isolation properties (no SQLITE_BUSY, no git lock errors, unique paths).
func TestBlock8_TwelveSiblingRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	count := getStressCount()
	if count < 2 {
		t.Skipf("Stress count %d too low for concurrency test, need at least 2", count)
	}

	t.Logf("Launching %d concurrent sibling runs...", count)

	// Create a single parent git repo that all runs share
	parentRepo := initStressRepo(t)
	t.Logf("Parent repo: %s", parentRepo)

	// Set up a temporary rundb path to avoid interfering with developer's real rundb
	rundbPath := filepath.Join(t.TempDir(), "stress_runs.db")
	t.Logf("Using rundb: %s", rundbPath)

	// Use a common base directory for all logs (to verify uniqueness)
	logsBaseDir := t.TempDir()

	// Launch all runs concurrently
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	startTime := time.Now()
	results := launchConcurrentRuns(t, ctx, count, parentRepo, logsBaseDir, rundbPath)
	totalDuration := time.Since(startTime)

	t.Logf("All %d runs completed in %v", count, totalDuration)

	// Verify assertions
	verifyAllRunsSucceeded(t, results)
	verifyUniqueLogsRoots(t, results)
	verifyUniqueBranches(t, results)
	verifyUniqueWorktrees(t, results)
	verifyRundbIntegrity(t, rundbPath, results)
	verifyNoContentionErrors(t, results)
	verifyRuntimeBound(t, totalDuration, 60*time.Second)
}

// initStressRepo creates a temporary git repository suitable for concurrent runs
func initStressRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	runGit(t, repo, "init")
	runGit(t, repo, "config", "user.name", "stress-tester")
	runGit(t, repo, "config", "user.email", "stress@test.com")

	// Create initial commit
	readme := filepath.Join(repo, "README.md")
	if err := os.WriteFile(readme, []byte("# Stress Test Repo\n"), 0o644); err != nil {
		t.Fatalf("Failed to write README: %v", err)
	}
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "Initial commit for stress test")

	return repo
}

// runGit executes a git command in the given directory
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
}

// launchConcurrentRuns launches N concurrent runs and collects their results
func launchConcurrentRuns(t *testing.T, ctx context.Context, count int, parentRepo, logsBaseDir, rundbPath string) []runResult {
	t.Helper()

	results := make([]runResult, count)
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine opens its own DB connection to exercise independent opens
			// on the same file, surfacing SQLite contention as intended by Block 8
			results[idx] = executeSingleRun(t, ctx, idx, parentRepo, logsBaseDir, rundbPath)
		}(i)
	}

	wg.Wait()
	return results
}

// executeSingleRun executes a single run and returns its result
func executeSingleRun(t *testing.T, ctx context.Context, idx int, parentRepo, logsBaseDir string, rundbPath string) runResult {
	// Generate unique run ID
	runID, err := NewRunID()
	if err != nil {
		return runResult{index: idx, err: fmt.Errorf("failed to generate run ID: %w", err)}
	}

	// Each run gets its own logsRoot under the base dir
	logsRoot := filepath.Join(logsBaseDir, runID)

	// Open our own DB connection for this goroutine
	db, err := rundb.Open(rundbPath)
	if err != nil {
		return runResult{index: idx, runID: runID, logsRoot: logsRoot, err: fmt.Errorf("failed to open rundb: %w", err)}
	}
	defer db.Close()

	start := time.Now()

	// Create run options
	opts := RunOptions{
		RepoPath:        parentRepo,
		RunID:           runID,
		LogsRoot:        logsRoot,
		AllowTestShim:   true,
		DisableCXDB:     true, // Skip CXDB for this stress test
		RunBranchPrefix: "attractor/run",
		RequireClean:    false,
		Labels: map[string]string{
			"source":      "stress_test",
			"stress_test": "block8",
			"run_index":   fmt.Sprintf("%d", idx),
		},
		RunDB: db,
	}

	// Execute the run
	res, err := Run(ctx, simpleDOT, opts)

	duration := time.Since(start)

	result := runResult{
		index:    idx,
		runID:    runID,
		logsRoot: logsRoot,
		duration: duration,
		err:      err,
	}

	if res != nil {
		result.worktreeDir = res.WorktreeDir
		result.branch = res.RunBranch
		result.finalStatus = string(res.FinalStatus)
	}

	t.Logf("Run %d (id=%s): duration=%v, status=%s, err=%v",
		idx, runID[:8], duration, result.finalStatus, err)

	return result
}

// verifyAllRunsSucceeded asserts that all runs completed successfully
func verifyAllRunsSucceeded(t *testing.T, results []runResult) {
	t.Helper()
	var failures []string
	for _, r := range results {
		if r.err != nil {
			failures = append(failures, fmt.Sprintf("run %d: error: %v", r.index, r.err))
		} else if r.finalStatus != string(runtime.FinalSuccess) {
			failures = append(failures, fmt.Sprintf("run %d: status=%s", r.index, r.finalStatus))
		}
	}
	if len(failures) > 0 {
		t.Errorf("%d runs failed:\n%s", len(failures), strings.Join(failures, "\n"))
	} else {
		t.Logf("All %d runs succeeded", len(results))
	}
}

// verifyUniqueLogsRoots asserts that each run has a distinct logsRoot
func verifyUniqueLogsRoots(t *testing.T, results []runResult) {
	t.Helper()
	seen := make(map[string]int)
	var collisions []string
	for _, r := range results {
		if prev, ok := seen[r.logsRoot]; ok {
			collisions = append(collisions, fmt.Sprintf("runs %d and %d share logsRoot: %s", prev, r.index, r.logsRoot))
		}
		seen[r.logsRoot] = r.index
	}
	if len(collisions) > 0 {
		t.Errorf("LogsRoot collisions detected:\n%s", strings.Join(collisions, "\n"))
	} else {
		t.Logf("All %d runs have unique logsRoot paths", len(results))
	}
}

// verifyUniqueBranches asserts that each run has a distinct git branch
func verifyUniqueBranches(t *testing.T, results []runResult) {
	t.Helper()
	seen := make(map[string]int)
	var collisions []string
	for _, r := range results {
		if r.branch == "" {
			continue // Skip if no branch (run may have failed early)
		}
		if prev, ok := seen[r.branch]; ok {
			collisions = append(collisions, fmt.Sprintf("runs %d and %d share branch: %s", prev, r.index, r.branch))
		}
		seen[r.branch] = r.index
	}
	if len(collisions) > 0 {
		t.Errorf("Branch collisions detected:\n%s", strings.Join(collisions, "\n"))
	} else {
		t.Logf("All %d runs have unique branch names", len(results))
	}
}

// verifyUniqueWorktrees asserts that each run has a distinct worktree directory
func verifyUniqueWorktrees(t *testing.T, results []runResult) {
	t.Helper()
	seen := make(map[string]int)
	var collisions []string
	for _, r := range results {
		if r.worktreeDir == "" {
			continue // Skip if no worktree (run may have failed early)
		}
		if prev, ok := seen[r.worktreeDir]; ok {
			collisions = append(collisions, fmt.Sprintf("runs %d and %d share worktree: %s", prev, r.index, r.worktreeDir))
		}
		seen[r.worktreeDir] = r.index
	}
	if len(collisions) > 0 {
		t.Errorf("Worktree collisions detected:\n%s", strings.Join(collisions, "\n"))
	} else {
		t.Logf("All %d runs have unique worktree directories", len(results))
	}
}

// verifyRundbIntegrity verifies that the rundb has exactly one row per run
func verifyRundbIntegrity(t *testing.T, rundbPath string, results []runResult) {
	t.Helper()

	db, err := rundb.Open(rundbPath)
	if err != nil {
		t.Fatalf("Failed to open rundb at %s: %v", rundbPath, err)
	}
	defer db.Close()

	// Check that each run ID appears exactly once
	for _, r := range results {
		if r.runID == "" {
			continue
		}
		record, err := db.GetRun(r.runID)
		if err != nil {
			t.Errorf("Failed to get run %s from rundb: %v", r.runID, err)
			continue
		}
		if record == nil {
			t.Errorf("Run %s not found in rundb", r.runID)
			continue
		}
		if record.LogsRoot != r.logsRoot {
			t.Errorf("Run %s: rundb logsRoot %q != expected %q", r.runID, record.LogsRoot, r.logsRoot)
		}
	}

	// Count total rows
	allRuns, err := db.ListRuns(rundb.ListFilter{})
	if err != nil {
		t.Fatalf("Failed to list runs: %v", err)
	}

	expectedCount := 0
	for _, r := range results {
		if r.runID != "" {
			expectedCount++
		}
	}

	if len(allRuns) != expectedCount {
		t.Errorf("Rundb has %d runs, expected %d", len(allRuns), expectedCount)
	} else {
		t.Logf("Rundb correctly has %d run records", len(allRuns))
	}
}

// verifyNoContentionErrors scans run logs for SQLITE_BUSY or git lock errors
func verifyNoContentionErrors(t *testing.T, results []runResult) {
	t.Helper()
	var contentionErrors []string

	for _, r := range results {
		if r.logsRoot == "" {
			continue
		}

		// Check run.log for contention errors
		runLogPath := filepath.Join(r.logsRoot, "run.log")
		if data, err := os.ReadFile(runLogPath); err == nil {
			logContent := string(data)
			if strings.Contains(logContent, "SQLITE_BUSY") ||
				strings.Contains(logContent, "database is locked") {
				contentionErrors = append(contentionErrors, fmt.Sprintf("run %d: SQLITE_BUSY in run.log", r.index))
			}
			if strings.Contains(logContent, "index.lock") ||
				strings.Contains(logContent, "Unable to create") {
				contentionErrors = append(contentionErrors, fmt.Sprintf("run %d: git lock error in run.log", r.index))
			}
		}

		// Check final.json for failure status
		finalPath := filepath.Join(r.logsRoot, "final.json")
		if data, err := os.ReadFile(finalPath); err == nil {
			var final map[string]interface{}
			if err := json.Unmarshal(data, &final); err == nil {
				if status, ok := final["status"].(string); ok && status == "fail" {
					// Check if it was due to contention
					if failure, ok := final["failure_reason"].(string); ok {
						if strings.Contains(failure, "lock") || strings.Contains(failure, "busy") {
							contentionErrors = append(contentionErrors, fmt.Sprintf("run %d: contention failure: %s", r.index, failure))
						}
					}
				}
			}
		}
	}

	if len(contentionErrors) > 0 {
		t.Errorf("Contention errors detected:\n%s", strings.Join(contentionErrors, "\n"))
	} else {
		t.Logf("No contention errors (SQLITE_BUSY, git locks) detected")
	}
}

// verifyRuntimeBound asserts that total wall time stayed under the limit
func verifyRuntimeBound(t *testing.T, duration, limit time.Duration) {
	t.Helper()
	if duration > limit {
		t.Errorf("Total runtime %v exceeds limit %v", duration, limit)
	} else {
		t.Logf("Total runtime %v is within limit %v", duration, limit)
	}
}
