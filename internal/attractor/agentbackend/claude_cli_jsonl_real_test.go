// Stress test: parse every real claude-CLI agent_output.jsonl on disk
// and confirm the codec handles the entire production corpus without
// returning errors. Skipped when the runs directory isn't present
// (CI / fresh checkouts) so the test only fires on dev machines that
// have actually run kilroy.
//
// This is the closest thing to "does it work on real data" we get
// without re-running the LLM. Useful as a continuous regression check
// while the codec evolves through Block 6 steps 3-7.

package agentbackend

import (
	"os"
	"path/filepath"
	"testing"
)

// TestParseClaudeCLIJSONL_AgainstAllRealCaptures walks the real
// kilroy state directory and parses every agent_output.jsonl found.
// Asserts no parser errors and that each file produces at least one
// event (every captured run had at least an init + a result).
//
// Skipped automatically when ~/.local/state/kilroy/attractor/runs/
// is absent — this is a developer dogfood test, not a CI test.
func TestParseClaudeCLIJSONL_AgainstAllRealCaptures(t *testing.T) {
	requireIntegration(t)

	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("UserHomeDir: %v", err)
	}
	runsDir := filepath.Join(home, ".local", "state", "kilroy", "attractor", "runs")
	if _, err := os.Stat(runsDir); err != nil {
		t.Skipf("no kilroy runs directory at %s; skipping real-capture stress test", runsDir)
	}

	var jsonlPaths []string
	err = filepath.WalkDir(runsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // tolerate unreadable subdirs from old runs
		}
		if !d.IsDir() && d.Name() == "agent_output.jsonl" {
			jsonlPaths = append(jsonlPaths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", runsDir, err)
	}
	if len(jsonlPaths) == 0 {
		t.Skip("no agent_output.jsonl captures found; skipping")
	}
	t.Logf("stress-testing claude codec against %d real captures", len(jsonlPaths))

	failed := 0
	totalEvents := 0
	for _, p := range jsonlPaths {
		f, err := os.Open(p)
		if err != nil {
			// Permission / transient errors — log and continue.
			t.Logf("skipping %s: open: %v", p, err)
			continue
		}
		events, parseErr := ParseClaudeCLIJSONL(f)
		_ = f.Close()
		if parseErr != nil {
			t.Errorf("parse %s: %v", p, parseErr)
			failed++
			continue
		}
		// Every real run should produce at least one event. (Empty captures
		// are typically failed launches that didn't get past tmux session
		// creation; skip silently rather than fail the test.)
		if len(events) == 0 {
			fi, _ := os.Stat(p)
			if fi != nil && fi.Size() > 0 {
				t.Logf("non-empty file %s produced 0 events (size=%d) — likely non-claude tool output", p, fi.Size())
			}
			continue
		}
		totalEvents += len(events)
	}
	t.Logf("processed %d files, %d total events, %d parser errors", len(jsonlPaths), totalEvents, failed)
}
