// `kilroy workflows` subcommand — discovery-side inspection. Surfaces
// the unified v2 manifest (works for legacy manifests too via the
// transition shim in workflows.LegacyFromRaw / LoadManifest).

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/dot"
	"github.com/danshapiro/kilroy/internal/attractor/engine"
	"github.com/danshapiro/kilroy/internal/attractor/modeldb"
	"github.com/danshapiro/kilroy/internal/attractor/projectroot"
	"github.com/danshapiro/kilroy/internal/attractor/style"
	"github.com/danshapiro/kilroy/internal/attractor/validate"
	"github.com/danshapiro/kilroy/internal/attractor/workflows"
)

func workflowsCmd(args []string) {
	if len(args) == 0 {
		workflowsUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "list":
		workflowsList(args[1:])
	case "describe":
		workflowsDescribe(args[1:])
	case "validate":
		workflowsValidate(args[1:])
	case "-h", "--help", "help":
		workflowsUsage()
		os.Exit(0)
	default:
		workflowsUsage()
		fmt.Fprintf(os.Stderr, "unknown workflows subcommand: %q\n", args[0])
		os.Exit(1)
	}
}

func workflowsUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy workflows list [--pretty] [--all]    (JSON by default; v2 only unless --all)")
	fmt.Fprintln(os.Stderr, "  kilroy workflows describe <name> [--pretty] (JSON by default)")
	fmt.Fprintln(os.Stderr, "  kilroy workflows validate <name> [--pretty] (JSON by default)")
}

// workflowsListEntry is the JSON shape for a single workflow in `list`
// output. Keep it small — list is for "what's available?", not a full dump.
type workflowsListEntry struct {
	Name         string `json:"name"`
	Dir          string `json:"dir"`
	Source       string `json:"source"` // search-path root this workflow was found under
	Description  string `json:"description,omitempty"`
	DefaultClass string `json:"default_class,omitempty"`
	Schema       string `json:"schema,omitempty"` // "v2" or "legacy"
	Version      string `json:"version,omitempty"`
	Experimental bool   `json:"experimental,omitempty"`
}

func workflowsList(args []string) {
	// JSON is the default per plan §2.2 (agent-primary surface).
	// --pretty switches to the human-readable column view.
	asJSON := true
	// Curated by default: hide workflows marked [workflow].experimental
	// = true (build-test, coding-loop, multi-tool-exercise — useful as
	// harnesses but not what you'd point a user at). Experimental
	// workflows stay reachable via `kilroy run <name>` if you know the
	// name, and via `--all` here.
	includeAll := false
	for _, a := range args {
		switch a {
		case "--json":
			asJSON = true
		case "--pretty":
			asJSON = false
		case "--all":
			includeAll = true
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: kilroy workflows list [--pretty] [--all]")
			fmt.Fprintln(os.Stderr, "  default: hides workflows with [workflow].experimental = true")
			fmt.Fprintln(os.Stderr, "  --all:   include experimental workflows too")
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unexpected argument %q\n", a)
			os.Exit(1)
		}
	}

	cwd, _ := os.Getwd()
	projectRoot, _, err := projectroot.Find(cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kilroy workflows list: %v\n", err)
		os.Exit(1)
	}

	found, err := workflows.Discover(projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover workflows: %v\n", err)
		os.Exit(1)
	}

	entries := make([]workflowsListEntry, 0, len(found))
	for _, d := range found {
		e := workflowsListEntry{
			Name:   d.Name,
			Dir:    d.Dir,
			Source: d.Source,
		}
		if m, err := workflows.LoadManifest(filepath.Join(d.Dir, "workflow.toml")); err == nil && m != nil {
			e.Description = firstLine(m.Description)
			e.DefaultClass = m.DefaultClass
			e.Schema = m.Schema
			e.Version = m.Version
			e.Experimental = m.Experimental
		}
		// Default view hides workflows marked experimental. Pre-v2
		// (legacy `[[inputs]]`) packages are also hidden by default
		// since they can't carry the experimental flag — same intent.
		if !includeAll {
			if e.Experimental || e.Schema != "v2" {
				continue
			}
		}
		entries = append(entries, e)
	}

	if asJSON {
		out := map[string]any{
			"workflows":    entries,
			"search_paths": workflows.SearchPaths(projectRoot),
			"project_root": projectRoot,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		return
	}

	if len(entries) == 0 {
		fmt.Println("No workflows found.")
		fmt.Println()
		fmt.Println("Searched (highest precedence first):")
		for _, p := range workflows.SearchPaths(projectRoot) {
			fmt.Printf("  %s\n", p)
		}
		fmt.Println()
		fmt.Println("Set KILROY_WORKFLOW_PATHS to a directory of workflow packages,")
		fmt.Println("or place them under .kilroy/workflows/ in your project.")
		return
	}

	// Compute column widths so the human view stays readable.
	maxName := len("NAME")
	maxClass := len("CLASS")
	for _, e := range entries {
		if n := len(e.Name); n > maxName {
			maxName = n
		}
		if n := len(e.DefaultClass); n > maxClass {
			maxClass = n
		}
	}
	fmt.Printf("%-*s  %-*s  %s\n", maxName, "NAME", maxClass, "CLASS", "DESCRIPTION")
	fmt.Printf("%-*s  %-*s  %s\n", maxName, strings.Repeat("-", maxName), maxClass, strings.Repeat("-", maxClass), strings.Repeat("-", 30))
	for _, e := range entries {
		fmt.Printf("%-*s  %-*s  %s\n", maxName, e.Name, maxClass, e.DefaultClass, e.Description)
	}
}

// workflowsDescribeEntry is the JSON shape for `describe <name>`.
type workflowsDescribeEntry struct {
	Name             string                            `json:"name"`
	Description      string                            `json:"description"`
	AgentDescription string                            `json:"agent_description,omitempty"`
	Version          string                            `json:"version,omitempty"`
	Schema           string                            `json:"schema"`
	Source           string                            `json:"source"`
	Dir              string                            `json:"dir"`
	GraphFile        string                            `json:"graph_file"`
	DefaultClass     string                            `json:"default_class,omitempty"`
	Inputs           []workflows.InputSpec             `json:"inputs,omitempty"`
	Outputs          []workflows.OutputSpec            `json:"outputs,omitempty"`
	SideEffects      *sideEffectsView                  `json:"side_effects,omitempty"`
	Nodes            map[string]workflows.NodeOverride `json:"nodes,omitempty"`
	Secrets          []string                          `json:"secrets,omitempty"`
}

// sideEffectsView surfaces the four flags only when the workflow author
// explicitly declared them; emitting all-false when [side_effects] is
// absent would imply a stronger guarantee than the manifest gave.
type sideEffectsView struct {
	MutatesGit    bool `json:"mutates_git"`
	WritesFiles   bool `json:"writes_files"`
	NetworkEgress bool `json:"network_egress"`
	Idempotent    bool `json:"idempotent"`
}

func workflowsDescribe(args []string) {
	// JSON is the default per plan §2.2 (agent-primary surface).
	// --pretty switches to the human-readable section view.
	asJSON := true
	var name string
	for _, a := range args {
		switch a {
		case "--json":
			asJSON = true
		case "--pretty":
			asJSON = false
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: kilroy workflows describe <name> [--pretty]")
			os.Exit(0)
		default:
			if name != "" {
				fmt.Fprintf(os.Stderr, "unexpected argument %q\n", a)
				os.Exit(1)
			}
			name = a
		}
	}
	if name == "" {
		fmt.Fprintln(os.Stderr, "workflow name required")
		fmt.Fprintln(os.Stderr, "usage: kilroy workflows describe <name> [--pretty]")
		os.Exit(1)
	}

	cwd, _ := os.Getwd()
	projectRoot, _, err := projectroot.Find(cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kilroy workflows describe: %v\n", err)
		os.Exit(1)
	}

	d, err := workflows.Find(name, projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "find: %v\n", err)
		os.Exit(1)
	}
	if d == nil {
		fmt.Fprintf(os.Stderr, "workflow %q not found\n", name)
		os.Exit(1)
	}

	manifestPath := filepath.Join(d.Dir, "workflow.toml")
	m, err := workflows.LoadManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest %s: %v\n", manifestPath, err)
		os.Exit(1)
	}
	if m == nil {
		fmt.Fprintf(os.Stderr, "workflow %q has no workflow.toml\n", name)
		os.Exit(1)
	}

	view := workflowsDescribeEntry{
		Name:             m.Name,
		Description:      m.Description,
		AgentDescription: m.AgentDescription,
		Version:          m.Version,
		Schema:           m.Schema,
		Source:           d.Source,
		Dir:              d.Dir,
		GraphFile:        m.GraphFile,
		DefaultClass:     m.DefaultClass,
		Inputs:           m.Inputs,
		Outputs:          m.Outputs,
		Nodes:            m.Nodes,
		Secrets:          m.Secrets,
	}
	if m.SideEffects.Set {
		view.SideEffects = &sideEffectsView{
			MutatesGit:    m.SideEffects.MutatesGit,
			WritesFiles:   m.SideEffects.WritesFiles,
			NetworkEgress: m.SideEffects.NetworkEgress,
			Idempotent:    m.SideEffects.Idempotent,
		}
	}

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(view)
		return
	}

	// Human-text rendering. Stable section order; missing fields are skipped.
	fmt.Printf("name:        %s\n", view.Name)
	if view.Version != "" {
		fmt.Printf("version:     %s\n", view.Version)
	}
	fmt.Printf("schema:      %s\n", view.Schema)
	fmt.Printf("source:      %s\n", view.Source)
	fmt.Printf("dir:         %s\n", view.Dir)
	if view.DefaultClass != "" {
		fmt.Printf("default class: %s\n", view.DefaultClass)
	}
	if view.AgentDescription != "" {
		fmt.Printf("\nagent description:\n  %s\n", strings.ReplaceAll(strings.TrimSpace(view.AgentDescription), "\n", "\n  "))
	}
	if view.Description != "" {
		fmt.Printf("\ndescription:\n  %s\n", strings.ReplaceAll(strings.TrimSpace(view.Description), "\n", "\n  "))
	}

	if len(view.Inputs) > 0 {
		fmt.Println("\ninputs:")
		for _, in := range view.Inputs {
			req := "optional"
			if in.Required {
				req = "REQUIRED"
			}
			line := fmt.Sprintf("  %s (%s)", in.Name, req)
			if in.Type != "" {
				line += " type=" + in.Type
			}
			if in.Default != "" {
				line += fmt.Sprintf(" default=%q", in.Default)
			}
			fmt.Println(line)
			if in.Description != "" {
				fmt.Printf("    %s\n", in.Description)
			}
		}
	}

	if len(view.Outputs) > 0 {
		fmt.Println("\noutputs:")
		for _, out := range view.Outputs {
			tag := out.Type
			if tag == "" {
				tag = "?"
			}
			line := fmt.Sprintf("  %s (%s)", out.Name, tag)
			if out.Path != "" && out.Path != out.Name {
				line += " path=" + out.Path
			}
			if out.Optional {
				line += " optional"
			}
			fmt.Println(line)
			if out.Description != "" {
				fmt.Printf("    %s\n", out.Description)
			}
		}
	}

	if view.SideEffects != nil {
		fmt.Println("\nside effects:")
		fmt.Printf("  mutates_git    = %t\n", view.SideEffects.MutatesGit)
		fmt.Printf("  writes_files   = %t\n", view.SideEffects.WritesFiles)
		fmt.Printf("  network_egress = %t\n", view.SideEffects.NetworkEgress)
		fmt.Printf("  idempotent     = %t\n", view.SideEffects.Idempotent)
	}

	if len(view.Nodes) > 0 {
		fmt.Println("\nnode overrides:")
		for id, n := range view.Nodes {
			if n.Class != "" {
				fmt.Printf("  %s: class=%s\n", id, n.Class)
			} else if n.Model != "" {
				fmt.Printf("  %s: model=%s (strict)\n", id, n.Model)
			}
		}
	}

	if len(view.Secrets) > 0 {
		fmt.Printf("\nsecrets needed: %s\n", strings.Join(view.Secrets, ", "))
	}
}

// workflowsValidateResult is the JSON shape for `validate <name>`.
type workflowsValidateResult struct {
	Name      string                  `json:"name"`
	Source    string                  `json:"source"`
	Dir       string                  `json:"dir"`
	GraphFile string                  `json:"graph_file"`
	Schema    string                  `json:"schema,omitempty"`
	DOTIssues []validateDOTIssue      `json:"dot_issues,omitempty"`
	PreLaunch *engine.PreLaunchReport `json:"prelaunch,omitempty"`
	Status    string                  `json:"status"` // "ok"|"fail"
}

type validateDOTIssue struct {
	Severity string `json:"severity"`
	Rule     string `json:"rule"`
	Message  string `json:"message"`
	NodeID   string `json:"node_id,omitempty"`
	EdgeFrom string `json:"edge_from,omitempty"`
	EdgeTo   string `json:"edge_to,omitempty"`
	Fix      string `json:"fix,omitempty"`
}

// workflowsValidate runs the same checks the runtime runs at launch:
// DOT-level validation + package integrity + class resolution + auth +
// CLI binary presence. No LLM calls. Useful for development without
// kicking off a real run.
func workflowsValidate(args []string) {
	asJSON := true
	var name string
	for _, a := range args {
		switch a {
		case "--json":
			asJSON = true
		case "--pretty":
			asJSON = false
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: kilroy workflows validate <name> [--pretty]")
			os.Exit(0)
		default:
			if name != "" {
				fmt.Fprintf(os.Stderr, "unexpected argument %q\n", a)
				os.Exit(1)
			}
			name = a
		}
	}
	if name == "" {
		fmt.Fprintln(os.Stderr, "workflow name required")
		fmt.Fprintln(os.Stderr, "usage: kilroy workflows validate <name> [--pretty]")
		os.Exit(1)
	}

	cwd, _ := os.Getwd()
	projectRoot, _, err := projectroot.Find(cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kilroy workflows validate: %v\n", err)
		os.Exit(1)
	}

	d, err := workflows.Find(name, projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "find: %v\n", err)
		os.Exit(1)
	}
	if d == nil {
		fmt.Fprintf(os.Stderr, "workflow %q not found\n", name)
		os.Exit(1)
	}

	manifestPath := filepath.Join(d.Dir, "workflow.toml")
	m, err := workflows.LoadManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}

	graphFile := "graph.dot"
	if m != nil && m.GraphFile != "" {
		graphFile = m.GraphFile
	}
	graphPath := filepath.Join(d.Dir, graphFile)
	graphSrc, err := os.ReadFile(graphPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read graph: %v\n", err)
		os.Exit(1)
	}

	g, err := dot.Parse(graphSrc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse graph: %v\n", err)
		os.Exit(1)
	}

	if raw := strings.TrimSpace(g.Attrs["model_stylesheet"]); raw != "" {
		rules, parseErr := style.ParseStylesheet(raw)
		if parseErr == nil {
			_ = style.ApplyStylesheet(g, rules)
		}
	}

	out := workflowsValidateResult{
		Name:      name,
		Source:    d.Source,
		Dir:       d.Dir,
		GraphFile: graphFile,
	}
	if m != nil {
		out.Schema = m.Schema
	}

	// Load embedded model catalog so stylesheet model ID lint rules fire
	// (mirrors the --graph fallback in run_with_config.go:144-148). On
	// failure, fall back to nil catalog (degraded mode: model ID checks are
	// skipped; all other rules still run).
	cat, catErr := modeldb.LoadEmbeddedCatalog()
	if catErr != nil {
		fmt.Fprintf(os.Stderr, "WARNING: model catalog unavailable, model ID checks skipped: %v\n", catErr)
		cat = nil
	}

	// DOT-level validation (semantic rules, terminal-edge gates, etc.).
	for _, diag := range validate.ValidateWithOptions(g, validate.ValidateOptions{Catalog: cat}) {
		out.DOTIssues = append(out.DOTIssues, validateDOTIssue{
			Severity: severityString(diag.Severity),
			Rule:     diag.Rule,
			Message:  diag.Message,
			NodeID:   diag.NodeID,
			EdgeFrom: diag.EdgeFrom,
			EdgeTo:   diag.EdgeTo,
			Fix:      diag.Fix,
		})
	}

	// Package + class + auth + binary + secrets check (no LLM cost).
	var requiredSecrets []string
	if m != nil {
		requiredSecrets = append(requiredSecrets, m.Secrets...)
	}
	report, _ := engine.ValidatePreLaunch(g, engine.RunOptions{
		PackageDir:      d.Dir,
		RequiredSecrets: requiredSecrets,
	}, engine.PolicyDeps{})
	out.PreLaunch = report

	failed := false
	for _, issue := range out.DOTIssues {
		if issue.Severity == "error" {
			failed = true
			break
		}
	}
	if report != nil && report.Summary.Fail > 0 {
		failed = true
	}
	if report != nil && report.Package != nil && report.Package.Status == "fail" {
		failed = true
	}
	if failed {
		out.Status = "fail"
	} else {
		out.Status = "ok"
	}

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	} else {
		fmt.Printf("name:      %s\n", out.Name)
		fmt.Printf("dir:       %s\n", out.Dir)
		fmt.Printf("status:    %s\n", out.Status)
		if len(out.DOTIssues) > 0 {
			fmt.Println("\ndot issues:")
			for _, d := range out.DOTIssues {
				fmt.Printf("  [%s] %s: %s", d.Severity, d.Rule, d.Message)
				if d.NodeID != "" {
					fmt.Printf(" (node: %s)", d.NodeID)
				}
				fmt.Println()
			}
		}
		if out.PreLaunch != nil {
			if out.PreLaunch.Package != nil && out.PreLaunch.Package.Status == "fail" {
				fmt.Println("\npackage:")
				for _, e := range out.PreLaunch.Package.Errors {
					fmt.Printf("  ERROR: %s\n", e)
				}
			}
			if len(out.PreLaunch.Nodes) > 0 {
				fmt.Println("\nnode resolutions:")
				for _, n := range out.PreLaunch.Nodes {
					marker := "✓"
					if n.Status == "fail" {
						marker = "✗"
					}
					fmt.Printf("  %s %s", marker, n.NodeID)
					if n.Class != "" {
						fmt.Printf(" class=%s", n.Class)
					}
					if n.ResolvedModel != "" {
						fmt.Printf(" → %s via %s", n.ResolvedModel, n.ResolvedDriver)
					}
					fmt.Println()
					for _, e := range n.Errors {
						fmt.Printf("    %s\n", e)
					}
				}
			}
			if len(out.PreLaunch.Secrets) > 0 {
				fmt.Println("\nsecrets:")
				for _, s := range out.PreLaunch.Secrets {
					marker := "✓"
					if s.Status == "fail" {
						marker = "✗"
					}
					fmt.Printf("  %s %s\n", marker, s.Name)
					for _, e := range s.Errors {
						fmt.Printf("    %s\n", e)
					}
				}
			}
		}
	}

	if failed {
		os.Exit(1)
	}
}

func severityString(s validate.Severity) string {
	switch s {
	case validate.SeverityError:
		return "error"
	case validate.SeverityWarning:
		return "warn"
	default:
		return "info"
	}
}

// firstLine returns the first non-empty trimmed line of s, or "" if none.
// Used so list output is one row per workflow even when the manifest's
// description is a multi-line """ block.
func firstLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
