package validate

import (
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/dot"
	"github.com/danshapiro/kilroy/internal/attractor/style"
)

// runClassLint parses a graph with the supplied agent-node body, applies the
// model stylesheet (mirroring the real validate path), and returns the
// diagnostics produced under the supplied class catalog.
func runClassLint(t *testing.T, body string, classes []string) []Diagnostic {
	t.Helper()
	src := []byte(`digraph G {
  graph [model_stylesheet="* { llm_provider: anthropic; llm_model: claude-sonnet-4.6; }"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  ` + body + `
}
`)
	parsed, err := dot.Parse(src)
	if err != nil {
		t.Fatalf("parse: %v\nsrc:\n%s", err, src)
	}
	if raw := strings.TrimSpace(parsed.Attrs["model_stylesheet"]); raw != "" {
		if rules, perr := style.ParseStylesheet(raw); perr == nil {
			_ = style.ApplyStylesheet(parsed, rules)
		}
	}
	return ValidateWithOptions(parsed, ValidateOptions{PolicyClasses: classes})
}

func TestUnknownAgentClass_KnownClassValidatesClean(t *testing.T) {
	body := `
  agent [shape=box, agent_class="hard_coding", prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> agent -> exit`
	diags := runClassLint(t, body, []string{"hard_coding", "quick_easy"})
	assertNoRule(t, diags, "unknown_agent_class")
}

func TestUnknownAgentClass_UnknownClassEmitsError(t *testing.T) {
	body := `
  agent [shape=box, agent_class="totally_made_up", prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> agent -> exit`
	diags := runClassLint(t, body, []string{"hard_coding", "quick_easy"})

	var found *Diagnostic
	for i := range diags {
		if diags[i].Rule == "unknown_agent_class" {
			found = &diags[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected unknown_agent_class diagnostic; got %v", diagRules(diags))
	}
	if found.Severity != SeverityError {
		t.Fatalf("expected SeverityError; got %s", found.Severity)
	}
	if !strings.Contains(found.Message, "totally_made_up") {
		t.Fatalf("expected message to mention class name; got %q", found.Message)
	}
	if !strings.Contains(found.Message, "agent") {
		t.Fatalf("expected message to mention node id; got %q", found.Message)
	}
	if found.NodeID != "agent" {
		t.Fatalf("expected NodeID=\"agent\"; got %q", found.NodeID)
	}
	if !strings.Contains(found.Fix, "kilroy policy list") {
		t.Fatalf("expected Fix to point at 'kilroy policy list'; got %q", found.Fix)
	}
}

func TestUnknownAgentClass_MultipleNodesAllSurface(t *testing.T) {
	body := `
  alpha [shape=box, agent_class="ghost_one",  prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  beta  [shape=box, agent_class="ghost_two",  prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  agent [shape=box, agent_class="hard_coding", prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> alpha -> beta -> agent -> exit`
	diags := runClassLint(t, body, []string{"hard_coding"})

	var seen []string
	for _, d := range diags {
		if d.Rule == "unknown_agent_class" {
			seen = append(seen, d.NodeID)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 unknown_agent_class diagnostics; got %d (%v)", len(seen), seen)
	}
	// Deterministic order: lint walks nodes by sorted ID.
	if seen[0] != "alpha" || seen[1] != "beta" {
		t.Fatalf("expected sorted [alpha beta]; got %v", seen)
	}
}

func TestUnknownAgentClass_NoAttributeNoError(t *testing.T) {
	body := `
  agent [shape=box, prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> agent -> exit`
	diags := runClassLint(t, body, []string{"hard_coding"})
	assertNoRule(t, diags, "unknown_agent_class")
}

func TestUnknownAgentClass_EmptyAttributeNoError(t *testing.T) {
	body := `
  agent [shape=box, agent_class="", prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> agent -> exit`
	diags := runClassLint(t, body, []string{"hard_coding"})
	assertNoRule(t, diags, "unknown_agent_class")
}

func TestUnknownAgentClass_NilCatalogSkipsLint(t *testing.T) {
	// When the caller did not supply policy classes, the lint must be a
	// no-op rather than firing on every node.
	body := `
  agent [shape=box, agent_class="totally_made_up", prompt="$KILROY_STAGE_STATUS_PATH $KILROY_STAGE_STATUS_FALLBACK_PATH"]
  start -> agent -> exit`
	diags := runClassLint(t, body, nil)
	assertNoRule(t, diags, "unknown_agent_class")
}

func diagRules(diags []Diagnostic) []string {
	out := make([]string, 0, len(diags))
	for _, d := range diags {
		out = append(out, string(d.Severity)+":"+d.Rule)
	}
	return out
}
