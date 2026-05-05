package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/engine"
)

// TestExtractAndValidateResearchDotFiles tests that existing research .dot files
// can be extracted and validated by the engine.
func TestExtractAndValidateResearchDotFiles(t *testing.T) {
	requireIntegration(t)
	repoRoot := findRepoRoot(t)

	dotFiles := []string{
		"research/refactor-test-vague.dot",
		"research/refactor-test-moderate.dot",
		"research/refactor-test-complex.dot",
	}

	for _, rel := range dotFiles {
		t.Run(filepath.Base(rel), func(t *testing.T) {
			path := filepath.Join(repoRoot, rel)
			content, err := os.ReadFile(path)
			if err != nil {
				t.Skipf("file not found (expected in CI): %s", path)
			}

			// Test extraction from raw content (simulates clean LLM output).
			extracted, err := ExtractDigraph(string(content))
			if err != nil {
				t.Fatalf("ExtractDigraph failed: %v", err)
			}

			// Test extraction from wrapped content (simulates LLM with commentary).
			wrapped := "Here is the pipeline:\n\n```dot\n" + string(content) + "\n```\n\nThis pipeline has multiple stages."
			extracted2, err := ExtractDigraph(wrapped)
			if err != nil {
				t.Fatalf("ExtractDigraph (wrapped) failed: %v", err)
			}

			// Both should produce the same result.
			if extracted != extracted2 {
				t.Error("wrapped extraction produced different result than raw")
			}

			// Validate via engine.Prepare.
			_, diags, err := engine.Prepare([]byte(extracted))
			if err != nil {
				t.Fatalf("engine.Prepare failed: %v", err)
			}
			for _, d := range diags {
				t.Logf("diagnostic: %s: %s (%s)", d.Severity, d.Message, d.Rule)
			}
		})
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root (no go.mod)")
		}
		dir = parent
	}
}
