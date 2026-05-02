// v2 workflow.toml schema per docs/plans/2026-05-01-kilroy-v2-final-plan.md §5.2.
//
// Two manifest shapes co-exist during the v2 transition:
//
//   - **Legacy** (workflows v1): top-level `name`/`description`/`version`,
//     `[[inputs]]` array of tables, `outputs = [...]` string array.
//   - **v2**: a `[workflow]` table holds the metadata, `[inputs.<name>]`
//     and `[outputs.<name>]` tables describe IO with types and constraints,
//     `[side_effects]` declares planning signals, `[nodes.<id>]` overrides
//     class/model per node, `[secrets]` lists abstract credential names.
//
// The loader auto-detects shape (the presence of a `[workflow]` table is
// the discriminator) and converts both into a unified internal Manifest.
// No hard cut — legacy manifests keep working until they're re-authored.

package workflows

import (
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

// Manifest is the unified, in-memory representation that the rest of the
// engine consumes regardless of on-disk shape. Code that used to read
// PackageManifest moves to this; PackageManifest stays as a legacy
// transitional alias (see manifest_legacy.go's compatibility shim).
type Manifest struct {
	Name             string
	Description      string
	AgentDescription string
	Version          string
	GraphFile        string // relative to package dir; defaults to "graph.dot"
	DefaultClass     string // policy class used by agentic nodes that don't override

	Inputs      []InputSpec
	Outputs     []OutputSpec
	SideEffects SideEffects
	Nodes       map[string]NodeOverride
	Secrets     []string // abstract credential names this workflow needs

	// Experimental marks workflows that aren't part of the curated default
	// surface. `kilroy workflows list` hides experimental entries unless
	// --all is passed. Used to keep harnesses/exercises (build-test,
	// coding-loop, multi-tool-exercise) reachable but not surfaced
	// alongside the shipped tools (fix, implement, investigate, review).
	Experimental bool

	// Defaults preserves legacy [defaults] for back-compat with run-config
	// label injection. Out of v2 scope; pass-through for now.
	Defaults ManifestDefaults

	// Schema is "v2" or "legacy" — useful for tooling and tests; not part
	// of the wire format.
	Schema string
}

// InputSpec describes a single workflow input. Fields are a superset of
// what either shape provides; the legacy shape leaves type-related
// fields zero-valued.
type InputSpec struct {
	Name        string   `json:"name"`
	Type        string   `json:"type,omitempty"` // "string"|"integer"|"float"|"boolean"|"path"|"enum"; legacy → ""
	Required    bool     `json:"required"`
	Default     string   `json:"default,omitempty"`
	Description string   `json:"description,omitempty"`
	EnumValues  []string `json:"enum_values,omitempty"` // type=="enum"
	Positional  int      `json:"positional,omitempty"`  // 0 means "not positional"; v2-only
	Flag        string   `json:"flag,omitempty"`        // "--context", etc.; v2-only
}

// OutputSpec describes a workflow output (file or scalar produced).
type OutputSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type,omitempty"` // "path"|"string"|"integer"|"float"|"boolean"
	Description string `json:"description,omitempty"`
	Optional    bool   `json:"optional,omitempty"`
	Path        string `json:"path,omitempty"` // type=="path": relative to workspace root
}

// SideEffects mirrors plan §5.2's [side_effects] table — declarative
// planning signals, not runtime enforcement. Default zero values mean
// "the workflow author didn't say"; tooling decides how to treat that.
type SideEffects struct {
	MutatesGit    bool
	WritesFiles   bool
	NetworkEgress bool
	Idempotent    bool
	// Set is true when [side_effects] was explicitly authored. Tooling
	// can warn when a workflow has neither — uncertainty is a smell.
	Set bool
}

// NodeOverride lets a workflow author pin a specific node to a different
// policy class or to a strict model (mutually exclusive per §5.2).
type NodeOverride struct {
	Class string `json:"class,omitempty"`
	Model string `json:"model,omitempty"`
}

// LoadManifest reads a workflow.toml from path and returns the unified
// Manifest. Auto-detects v2 vs legacy by looking for a [workflow] table.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return ParseManifest(data)
}

// ParseManifest parses a workflow.toml from raw bytes. Splitting Load
// from Parse keeps the unit tests filesystem-free.
//
// Detection: a `[workflow]` table is the v2 discriminator. v2 and legacy
// reuse the same TOML keys (`inputs`, `outputs`) with different shapes,
// so we decode into shape-specific structs based on detection rather
// than into a union — TOML can't represent the union directly.
func ParseManifest(data []byte) (*Manifest, error) {
	// First pass: decode just enough to discriminate the shape. The
	// `[workflow]` table is the v2 marker. We need a permissive struct
	// here that doesn't care about `inputs`/`outputs` since we don't
	// know their shape yet.
	var head rawHead
	if _, err := toml.Decode(string(data), &head); err != nil {
		return nil, fmt.Errorf("parse workflow.toml: %w", err)
	}
	if head.Workflow.Name != "" || head.Workflow.Version != "" || head.Workflow.Description != "" {
		var v2 rawV2Manifest
		if _, err := toml.Decode(string(data), &v2); err != nil {
			return nil, fmt.Errorf("parse workflow.toml (v2 shape): %w", err)
		}
		return assembleV2(&v2), nil
	}
	var legacy rawLegacyManifest
	if _, err := toml.Decode(string(data), &legacy); err != nil {
		return nil, fmt.Errorf("parse workflow.toml (legacy shape): %w", err)
	}
	return assembleLegacy(&legacy), nil
}

// rawHead captures only the fields needed to discriminate v2 vs legacy.
// `inputs`/`outputs` are intentionally absent — those keys hold
// incompatible types across shapes and would fail decode here.
type rawHead struct {
	Workflow rawWorkflowSection `toml:"workflow"`
	Name     string             `toml:"name"`
	Version  string             `toml:"version"`
}

// rawV2Manifest is the strict v2 shape: every documented section.
type rawV2Manifest struct {
	Workflow    rawWorkflowSection         `toml:"workflow"`
	Inputs      map[string]rawV2Input      `toml:"inputs"`
	Outputs     map[string]rawV2Output     `toml:"outputs"`
	SideEffects rawSideEffectsSection      `toml:"side_effects"`
	Nodes       map[string]rawNodeOverride `toml:"nodes"`
	Secrets     rawSecrets                 `toml:"secrets"`
	// Defaults preserved for back-compat label injection.
	Defaults rawLegacyDefaults `toml:"defaults"`
}

// rawLegacyManifest is the v1 shape: top-level metadata + [[inputs]]
// array of tables + outputs string array.
type rawLegacyManifest struct {
	Name        string              `toml:"name"`
	Description string              `toml:"description"`
	Version     string              `toml:"version"`
	Inputs      []rawLegacyInput    `toml:"inputs"`
	Outputs     []string            `toml:"outputs"`
	Defaults    rawLegacyDefaults   `toml:"defaults"`
	Metadata    map[string]string   `toml:"metadata"`
}

type rawLegacyInput struct {
	Name        string `toml:"name"`
	Description string `toml:"description"`
	Required    bool   `toml:"required"`
	Default     string `toml:"default"`
}

type rawWorkflowSection struct {
	Name             string `toml:"name"`
	Version          string `toml:"version"`
	Description      string `toml:"description"`
	AgentDescription string `toml:"agent_description"`
	Author           string `toml:"author"`
	Tags             []string
	Graph            string `toml:"graph"`
	DefaultClass     string `toml:"default_class"`
	Experimental     bool   `toml:"experimental"`
}

type rawV2Input struct {
	Type        string   `toml:"type"`
	Required    bool     `toml:"required"`
	Default     string   `toml:"default"`
	Description string   `toml:"description"`
	EnumValues  []string `toml:"enum_values"`
	Positional  int      `toml:"positional"`
	Flag        string   `toml:"flag"`
}

type rawV2Output struct {
	Type        string `toml:"type"`
	Description string `toml:"description"`
	Optional    bool   `toml:"optional"`
	Path        string `toml:"path"`
}

type rawSideEffectsSection struct {
	MutatesGit    *bool `toml:"mutates_git"`
	WritesFiles   *bool `toml:"writes_files"`
	NetworkEgress *bool `toml:"network_egress"`
	Idempotent    *bool `toml:"idempotent"`
}

type rawNodeOverride struct {
	Class string `toml:"class"`
	Model string `toml:"model"`
}

type rawSecrets struct {
	Needs []string `toml:"needs"`
}

type rawLegacyDefaults struct {
	Labels map[string]string `toml:"labels"`
}

func assembleV2(raw *rawV2Manifest) *Manifest {
	m := &Manifest{
		Schema:           "v2",
		Name:             raw.Workflow.Name,
		Description:      raw.Workflow.Description,
		AgentDescription: raw.Workflow.AgentDescription,
		Version:          raw.Workflow.Version,
		GraphFile:        defaultStr(raw.Workflow.Graph, "graph.dot"),
		DefaultClass:     raw.Workflow.DefaultClass,
		Experimental:     raw.Workflow.Experimental,
		Nodes:            make(map[string]NodeOverride, len(raw.Nodes)),
		Secrets:          append([]string(nil), raw.Secrets.Needs...),
		Defaults:         ManifestDefaults{Labels: raw.Defaults.Labels},
	}

	for name, in := range raw.Inputs {
		m.Inputs = append(m.Inputs, InputSpec{
			Name:        name,
			Type:        in.Type,
			Required:    in.Required,
			Default:     in.Default,
			Description: in.Description,
			EnumValues:  append([]string(nil), in.EnumValues...),
			Positional:  in.Positional,
			Flag:        in.Flag,
		})
	}
	sortInputsByName(m.Inputs)

	for name, out := range raw.Outputs {
		m.Outputs = append(m.Outputs, OutputSpec{
			Name:        name,
			Type:        out.Type,
			Description: out.Description,
			Optional:    out.Optional,
			Path:        out.Path,
		})
	}
	sortOutputsByName(m.Outputs)

	se := raw.SideEffects
	m.SideEffects.Set = se.MutatesGit != nil || se.WritesFiles != nil ||
		se.NetworkEgress != nil || se.Idempotent != nil
	m.SideEffects.MutatesGit = derefBool(se.MutatesGit)
	m.SideEffects.WritesFiles = derefBool(se.WritesFiles)
	m.SideEffects.NetworkEgress = derefBool(se.NetworkEgress)
	m.SideEffects.Idempotent = derefBool(se.Idempotent)

	for id, n := range raw.Nodes {
		m.Nodes[id] = NodeOverride{Class: n.Class, Model: n.Model}
	}
	return m
}

func assembleLegacy(raw *rawLegacyManifest) *Manifest {
	m := &Manifest{
		Schema:      "legacy",
		Name:        raw.Name,
		Description: raw.Description,
		Version:     raw.Version,
		GraphFile:   "graph.dot",
		Nodes:       map[string]NodeOverride{},
		Defaults:    ManifestDefaults{Labels: raw.Defaults.Labels},
	}
	for _, in := range raw.Inputs {
		m.Inputs = append(m.Inputs, InputSpec{
			Name:        in.Name,
			Description: in.Description,
			Required:    in.Required,
			Default:     in.Default,
		})
	}
	for _, p := range raw.Outputs {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		m.Outputs = append(m.Outputs, OutputSpec{
			Name: p,
			Type: "path",
			Path: p,
		})
	}
	return m
}

// LegacyFromRaw is exposed for the loader's compatibility path: it lets
// LoadPackage continue to populate the older PackageManifest struct from
// either shape during the transition. Callers reaching for the v2-only
// fields should use LoadManifest directly.
func LegacyFromRaw(m *Manifest) *PackageManifest {
	if m == nil {
		return nil
	}
	pm := &PackageManifest{
		Name:        m.Name,
		Description: m.Description,
		Version:     m.Version,
		Outputs:     make([]string, 0, len(m.Outputs)),
		Defaults:    m.Defaults,
	}
	for _, in := range m.Inputs {
		pm.Inputs = append(pm.Inputs, ManifestInput{
			Name:        in.Name,
			Description: in.Description,
			Required:    in.Required,
			Default:     in.Default,
		})
	}
	for _, out := range m.Outputs {
		if out.Path != "" {
			pm.Outputs = append(pm.Outputs, out.Path)
		} else if out.Type == "path" {
			pm.Outputs = append(pm.Outputs, out.Name)
		}
	}
	return pm
}

// helpers — tiny local conveniences, not exported.

func defaultStr(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func derefBool(p *bool) bool {
	if p == nil {
		return false
	}
	return *p
}

func sortInputsByName(in []InputSpec) {
	// Insertion-sort is fine — input lists are tiny.
	for i := 1; i < len(in); i++ {
		for j := i; j > 0 && in[j-1].Name > in[j].Name; j-- {
			in[j-1], in[j] = in[j], in[j-1]
		}
	}
}

func sortOutputsByName(out []OutputSpec) {
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Name > out[j].Name; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
}
