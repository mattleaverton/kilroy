// Tests that `kilroy validate --graph <file>` plain-text output surfaces
// the diagnostic Fix field for stylesheet model-ID rules.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidate_PlainText_SurfacesFix verifies that the validate handler
// prints the `fix:` line for stylesheet model-ID diagnostics, and that
// the Fix points users at `kilroy modeldb suggest --provider <P>`.
func TestValidate_PlainText_SurfacesFix(t *testing.T) {
	bin := buildKilroyBinary(t)

	cases := []struct {
		name              string
		dotBody           string
		wantRule          string
		wantFixSubstrings []string
		wantExitCode      int
	}{
		{
			name: "noncanonical_model_id_error",
			dotBody: `digraph G {
  graph [model_stylesheet="* { llm_provider: anthropic; llm_model: claude-opus-4-6; }"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  work  [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="Do work. Write $KILROY_STAGE_STATUS_PATH (fallback: $KILROY_STAGE_STATUS_FALLBACK_PATH) with outcome=success."]
  start -> work
  work -> exit [condition="outcome=success"]
}`,
			wantRule: "stylesheet_noncanonical_model_id",
			wantFixSubstrings: []string{
				"  fix: ",
				"kilroy modeldb suggest --provider anthropic",
				"claude-opus-4-6 → claude-opus-4.6",
			},
			wantExitCode: 1,
		},
		{
			name: "unknown_model_warning",
			dotBody: `digraph G {
  graph [model_stylesheet="* { llm_provider: anthropic; llm_model: claude-totally-bogus; }"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  work  [shape=box, llm_provider=openai, llm_model=gpt-5.4, prompt="Do work. Write $KILROY_STAGE_STATUS_PATH (fallback: $KILROY_STAGE_STATUS_FALLBACK_PATH) with outcome=success."]
  start -> work
  work -> exit [condition="outcome=success"]
}`,
			wantRule: "stylesheet_unknown_model",
			wantFixSubstrings: []string{
				"  fix: ",
				"kilroy modeldb suggest --provider anthropic",
			},
			wantExitCode: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "graph.dot")
			if err := os.WriteFile(path, []byte(tc.dotBody), 0o644); err != nil {
				t.Fatalf("write dot: %v", err)
			}
			code, out := runKilroy(t, bin, "validate", "--graph", path)
			if code != tc.wantExitCode {
				t.Fatalf("exit code: got %d, want %d\noutput:\n%s", code, tc.wantExitCode, out)
			}
			if !strings.Contains(out, tc.wantRule) {
				t.Fatalf("expected rule %q in output, got:\n%s", tc.wantRule, out)
			}
			for _, sub := range tc.wantFixSubstrings {
				if !strings.Contains(out, sub) {
					t.Fatalf("expected fix substring %q in output, got:\n%s", sub, out)
				}
			}
		})
	}
}
