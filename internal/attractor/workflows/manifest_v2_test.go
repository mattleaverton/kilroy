// Tests for the v2 workflow.toml schema parser. Covers shape detection,
// the v2 happy path with every documented section, and round-trip from
// legacy [[inputs]] manifests so existing packages keep working.
package workflows

import (
	"strings"
	"testing"
)

func TestParseManifest_V2_FullSchema(t *testing.T) {
	src := `
[workflow]
name              = "review"
version           = "1"
description       = """
Review a target diff and produce a verdict.
"""
agent_description = "Review a PR / branch / patch and produce review.json + result.md."
default_class     = "hard_coding"
graph             = "graph.dot"

[inputs.target]
type        = "string"
required    = true
description = "PR URL, branch ref, or path to a .patch file."

[inputs.checklist]
type        = "path"
required    = false
default     = ""
description = "Optional review checklist."
flag        = "--checklist"

[inputs.severity]
type        = "enum"
required    = false
description = "Minimum severity to surface."
enum_values = ["info", "warn", "error"]
default     = "warn"

[outputs.result]
type        = "path"
description = "Human-readable review output."
path        = "result.md"

[outputs."review.json"]
type        = "path"
description = "Machine-readable structured findings."
optional    = false
path        = "review.json"

[side_effects]
mutates_git    = false
writes_files   = true
network_egress = false
idempotent     = true

[nodes.agent]
class = "hard_coding"

[nodes.summarize]
class = "quick_easy"

[secrets]
needs = ["github"]
`

	m, err := ParseManifest([]byte(src))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if m.Schema != "v2" {
		t.Errorf("schema = %q, want v2", m.Schema)
	}
	if m.Name != "review" {
		t.Errorf("name = %q, want review", m.Name)
	}
	if m.Version != "1" {
		t.Errorf("version = %q, want 1", m.Version)
	}
	if m.AgentDescription == "" {
		t.Error("agent_description not parsed")
	}
	if m.DefaultClass != "hard_coding" {
		t.Errorf("default_class = %q, want hard_coding", m.DefaultClass)
	}
	if m.GraphFile != "graph.dot" {
		t.Errorf("graph = %q, want graph.dot", m.GraphFile)
	}

	if len(m.Inputs) != 3 {
		t.Fatalf("inputs len = %d, want 3", len(m.Inputs))
	}
	// Inputs are sorted by name: checklist, severity, target.
	if m.Inputs[0].Name != "checklist" || m.Inputs[1].Name != "severity" || m.Inputs[2].Name != "target" {
		t.Errorf("inputs not sorted: %v", inputNames(m.Inputs))
	}
	target := findInput(m.Inputs, "target")
	if target == nil {
		t.Fatal("target input missing")
	}
	if !target.Required {
		t.Error("target should be required")
	}
	severity := findInput(m.Inputs, "severity")
	if severity == nil {
		t.Fatal("severity input missing")
	}
	if len(severity.EnumValues) != 3 {
		t.Errorf("severity enum_values = %v, want 3", severity.EnumValues)
	}
	checklist := findInput(m.Inputs, "checklist")
	if checklist == nil || checklist.Flag != "--checklist" {
		t.Errorf("checklist flag = %q, want --checklist", checklist.Flag)
	}

	if len(m.Outputs) != 2 {
		t.Fatalf("outputs len = %d, want 2", len(m.Outputs))
	}
	if !m.SideEffects.Set {
		t.Error("side_effects.Set should be true when authored")
	}
	if !m.SideEffects.WritesFiles {
		t.Error("writes_files = true expected")
	}
	if !m.SideEffects.Idempotent {
		t.Error("idempotent = true expected")
	}
	if m.SideEffects.MutatesGit {
		t.Error("mutates_git = false expected")
	}

	if got := m.Nodes["agent"].Class; got != "hard_coding" {
		t.Errorf("nodes.agent.class = %q, want hard_coding", got)
	}
	if got := m.Nodes["summarize"].Class; got != "quick_easy" {
		t.Errorf("nodes.summarize.class = %q, want quick_easy", got)
	}

	if len(m.Secrets) != 1 || m.Secrets[0] != "github" {
		t.Errorf("secrets = %v, want [github]", m.Secrets)
	}
}

func TestParseManifest_Legacy_StillWorks(t *testing.T) {
	src := `
name        = "implement"
description = "Implement a directed change with build+test verification."
version     = "1"

outputs = ["result.md"]

[[inputs]]
name        = "prompt"
description = "What to implement."
required    = true

[[inputs]]
name        = "context_files"
description = "Optional context paths."
required    = false

[defaults]
labels = { workflow = "implement" }
`
	m, err := ParseManifest([]byte(src))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if m.Schema != "legacy" {
		t.Errorf("schema = %q, want legacy", m.Schema)
	}
	if m.Name != "implement" {
		t.Errorf("name = %q, want implement", m.Name)
	}
	if len(m.Outputs) != 1 || m.Outputs[0].Path != "result.md" {
		t.Errorf("outputs = %+v, want one path-typed result.md", m.Outputs)
	}
	if m.Defaults.Labels["workflow"] != "implement" {
		t.Errorf("defaults.labels.workflow = %q, want implement", m.Defaults.Labels["workflow"])
	}
}

func TestParseManifest_V2_DetectionWinsOverLegacyFields(t *testing.T) {
	// A confused manifest with both [workflow] and top-level name/version
	// — we treat [workflow] as authoritative since it's the v2 shape.
	src := `
name = "from-legacy-fields"

[workflow]
name    = "from-v2-table"
version = "1"
description = "v2 should win"
`
	m, err := ParseManifest([]byte(src))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if m.Schema != "v2" {
		t.Errorf("schema = %q, want v2 (presence of [workflow] is the discriminator)", m.Schema)
	}
	if m.Name != "from-v2-table" {
		t.Errorf("name = %q, want from-v2-table", m.Name)
	}
}

func TestParseManifest_SideEffects_UnsetMeansNotAuthored(t *testing.T) {
	src := `
[workflow]
name = "no-side-effects"
version = "1"
description = "x"
`
	m, err := ParseManifest([]byte(src))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if m.SideEffects.Set {
		t.Error("Set should be false when [side_effects] is absent")
	}
}

func TestParseManifest_GraphDefaults(t *testing.T) {
	src := `
[workflow]
name = "default-graph"
version = "1"
description = "x"
`
	m, _ := ParseManifest([]byte(src))
	if m.GraphFile != "graph.dot" {
		t.Errorf("GraphFile = %q, want graph.dot", m.GraphFile)
	}
}

func TestLegacyFromRaw_RoundTripsManifestShape(t *testing.T) {
	src := `
[workflow]
name        = "fix"
version     = "1"
description = "y"

[inputs.issue]
required    = true
description = "Bug description."

[outputs.result]
type        = "path"
path        = "result.md"
`
	m, err := ParseManifest([]byte(src))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	pm := LegacyFromRaw(m)
	if pm.Name != "fix" {
		t.Errorf("legacy Name = %q, want fix", pm.Name)
	}
	if len(pm.Inputs) != 1 || pm.Inputs[0].Name != "issue" {
		t.Errorf("legacy Inputs = %+v, want one issue input", pm.Inputs)
	}
	if len(pm.Outputs) != 1 || pm.Outputs[0] != "result.md" {
		t.Errorf("legacy Outputs = %v, want [result.md]", pm.Outputs)
	}
}

// helpers
func findInput(in []InputSpec, name string) *InputSpec {
	for i := range in {
		if in[i].Name == name {
			return &in[i]
		}
	}
	return nil
}

func inputNames(in []InputSpec) string {
	names := make([]string, len(in))
	for i, x := range in {
		names[i] = x.Name
	}
	return strings.Join(names, ",")
}
