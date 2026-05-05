// Tests that `kilroy validate --graph <file>` plain-text output surfaces
// the diagnostic Fix field for stylesheet diagnostics.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidate_PlainText_SurfacesFix verifies that the validate handler
// prints the `fix:` line for stylesheet_raw_model_rejected diagnostics, and
// that the Fix points users at `kilroy policy list`.
func TestValidate_PlainText_SurfacesFix(t *testing.T) {
	bin := buildKilroyBinary(t)

	dotBody := `digraph G {
  graph [model_stylesheet="* { llm_provider: anthropic; llm_model: claude-sonnet-4.6; }"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  work  [shape=box, agent_class="hard_coding", prompt="Do work. Write $KILROY_STAGE_STATUS_PATH (fallback: $KILROY_STAGE_STATUS_FALLBACK_PATH) with outcome=success."]
  start -> work
  work -> exit [condition="outcome=success"]
}`
	path := filepath.Join(t.TempDir(), "graph.dot")
	if err := os.WriteFile(path, []byte(dotBody), 0o644); err != nil {
		t.Fatalf("write dot: %v", err)
	}
	code, out := runKilroy(t, bin, "validate", "--graph", path)
	if code != 1 {
		t.Fatalf("exit code: got %d, want 1\noutput:\n%s", code, out)
	}
	for _, sub := range []string{
		"stylesheet_raw_model_rejected",
		"  fix: ",
		"kilroy policy list",
	} {
		if !strings.Contains(out, sub) {
			t.Fatalf("expected substring %q in output, got:\n%s", sub, out)
		}
	}
}
