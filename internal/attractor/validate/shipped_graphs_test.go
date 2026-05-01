package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/dot"
	"github.com/danshapiro/kilroy/internal/attractor/style"
)

// TestShippedWorkflowGraphs walks every *.dot file under the repo's workflows/
// directory, parses each one via the DOT parser, applies the model_stylesheet
// (exactly as the engine does at launch), and runs the full validator against it.
// Any ERROR-level diagnostic causes the sub-test to fail, clearly identifying the
// offending file and the validator's message.
//
// This is the CI gate described in §13.2 of docs/plans/2026-05-01-kilroy-v2-final-plan.md:
// if the validator rejects a shipped workflow graph, the build fails.
//
// The stylesheet is applied before validation so that attributes resolved via
// stylesheet selectors (e.g. llm_provider from "* { llm_provider: anthropic; }")
// are present on nodes before the lint rules inspect them — the same pipeline the
// engine uses at launch via engine.PrepareWithOptions.  The engine package is not
// imported here because engine imports validate (cycle); instead we replicate the
// two relevant steps: dot.Parse → style.ApplyStylesheet → validate.Validate.
func TestShippedWorkflowGraphs(t *testing.T) {
	repoRoot := findRepoRoot(t)
	workflowsDir := filepath.Join(repoRoot, "workflows")

	if _, err := os.Stat(workflowsDir); os.IsNotExist(err) {
		t.Skipf("workflows/ directory not found at %s; skipping", workflowsDir)
	}

	var dotFiles []string
	err := filepath.WalkDir(workflowsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".dot") {
			dotFiles = append(dotFiles, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking workflows/: %v", err)
	}

	if len(dotFiles) == 0 {
		t.Fatal("no *.dot files found under workflows/; expected at least one shipped graph")
	}

	for _, absPath := range dotFiles {
		absPath := absPath // capture loop variable
		// Use the path relative to the repo root as the sub-test name so the
		// failure surface clearly identifies which graph is broken.
		relPath, err := filepath.Rel(repoRoot, absPath)
		if err != nil {
			relPath = absPath
		}

		t.Run(relPath, func(t *testing.T) {
			src, err := os.ReadFile(absPath)
			if err != nil {
				t.Fatalf("read %s: %v", absPath, err)
			}

			g, err := dot.Parse(src)
			if err != nil {
				t.Fatalf("parse %s: %v", absPath, err)
			}

			// Apply the model_stylesheet if present — same step the engine performs
			// before validation at launch.  This ensures that attributes resolved
			// via stylesheet selectors (e.g. llm_provider from "* { ... }") are
			// present on nodes before the lint rules inspect them.
			if raw := strings.TrimSpace(g.Attrs["model_stylesheet"]); raw != "" {
				rules, parseErr := style.ParseStylesheet(raw)
				if parseErr != nil {
					t.Fatalf("parse stylesheet in %s: %v", relPath, parseErr)
				}
				if applyErr := style.ApplyStylesheet(g, rules); applyErr != nil {
					t.Fatalf("apply stylesheet in %s: %v", relPath, applyErr)
				}
			}

			diags := Validate(g)

			var errors []string
			for _, d := range diags {
				if d.Severity == SeverityError {
					msg := fmt.Sprintf("[%s] %s", d.Rule, d.Message)
					if d.NodeID != "" {
						msg += fmt.Sprintf(" (node: %s)", d.NodeID)
					}
					if d.EdgeFrom != "" || d.EdgeTo != "" {
						msg += fmt.Sprintf(" (edge: %s -> %s)", d.EdgeFrom, d.EdgeTo)
					}
					if d.Fix != "" {
						msg += fmt.Sprintf(" | fix: %s", d.Fix)
					}
					errors = append(errors, msg)
				}
			}

			if len(errors) > 0 {
				t.Errorf("%s: validation failed with %d error(s):\n  %s",
					relPath, len(errors), strings.Join(errors, "\n  "))
			}
		})
	}
}
