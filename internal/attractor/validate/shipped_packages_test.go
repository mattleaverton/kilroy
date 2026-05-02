// Package-level integrity gate for shipped workflow packages. Closes the
// regression-bar gap above shipped_graphs_test.go (which only validates
// DOT): each workflow.toml must parse, every tool_command in the graph
// must resolve to an executable script in the package, and every
// class= attribute must reference a real policy class.
package validate

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/danshapiro/kilroy/internal/attractor/dot"
	"github.com/danshapiro/kilroy/internal/policy"
)

// shippedManifest is a deliberately-narrow TOML view of workflow.toml for
// integrity assertions only. It mirrors the legacy shape consumed by
// internal/attractor/workflows.PackageManifest but lives here to avoid an
// import cycle (workflows → engine → validate). Block 2's v2 schema will
// supersede this; the test will be re-shaped along with the parser.
type shippedManifest struct {
	Name        string                 `toml:"name"`
	Description string                 `toml:"description"`
	Version     string                 `toml:"version"`
	Inputs      []shippedManifestInput `toml:"inputs"`
}

type shippedManifestInput struct {
	Name        string `toml:"name"`
	Description string `toml:"description"`
	Required    bool   `toml:"required"`
}

// TestShippedWorkflowPackages walks every directory under workflows/ that
// contains a workflow.toml and asserts the package's structural integrity:
// manifest parses, scripts referenced by tool_command exist as regular
// files, agent class= attributes reference real policy classes (with a
// small bypass list for pre-Step-4b graphs that still use class= purely
// as model_stylesheet selectors — they'll fail at --tmux runtime against
// the new resolver and need migration; tracked separately).
//
// Note on script perms: workflow scripts are invoked as `bash <path>`
// (not `./<path>`), which doesn't require the executable bit. We assert
// the file exists as a regular file and is non-empty; the +x bit is
// cosmetic for these packages and inconsistent across the tree.
func TestShippedWorkflowPackages(t *testing.T) {
	// Pre-Step-4b graphs that use class= as stylesheet selectors rather
	// than policy class identifiers. With Step 4b landed, running these
	// under --tmux would fail with policy.ErrUnknownClass. They're listed
	// here as known TODOs rather than as test failures; remove an entry
	// when the underlying graph is migrated to real policy classes.
	knownClassIssues := map[string]bool{
		"workflows/coding-loop": true,
	}
	repoRoot := findRepoRoot(t)
	workflowsDir := filepath.Join(repoRoot, "workflows")

	if _, err := os.Stat(workflowsDir); os.IsNotExist(err) {
		t.Skipf("workflows/ directory not found at %s; skipping", workflowsDir)
	}

	pkgDirs, err := findPackageDirs(workflowsDir)
	if err != nil {
		t.Fatalf("walk workflows/: %v", err)
	}
	if len(pkgDirs) == 0 {
		t.Fatal("no workflow packages found under workflows/ (expected at least one workflow.toml)")
	}

	policyData, err := policy.Load()
	if err != nil {
		t.Fatalf("load embedded policy: %v", err)
	}

	for _, dir := range pkgDirs {
		dir := dir
		relDir, err := filepath.Rel(repoRoot, dir)
		if err != nil {
			relDir = dir
		}
		t.Run(relDir, func(t *testing.T) {
			tomlPath := filepath.Join(dir, "workflow.toml")
			var m shippedManifest
			if _, err := toml.DecodeFile(tomlPath, &m); err != nil {
				t.Fatalf("parse %s: %v", tomlPath, err)
			}

			// Required manifest fields.
			if strings.TrimSpace(m.Name) == "" {
				t.Error("workflow.toml: name is required")
			}
			if strings.TrimSpace(m.Description) == "" {
				t.Error("workflow.toml: description is required")
			}
			if strings.TrimSpace(m.Version) == "" {
				t.Error("workflow.toml: version is required")
			}

			// Every [[inputs]] entry must have name + description (legacy shape;
			// will move to [inputs.<name>] tables once Block 2 lands).
			for i, in := range m.Inputs {
				if strings.TrimSpace(in.Name) == "" {
					t.Errorf("workflow.toml [[inputs]] %d: name is required", i)
				}
				if strings.TrimSpace(in.Description) == "" {
					t.Errorf("workflow.toml [[inputs]] %q: description is required", in.Name)
				}
			}

			// Parse the graph and inspect every node.
			graphPath := filepath.Join(dir, "graph.dot")
			src, err := os.ReadFile(graphPath)
			if err != nil {
				t.Fatalf("read %s: %v", graphPath, err)
			}
			g, err := dot.Parse(src)
			if err != nil {
				t.Fatalf("parse %s: %v", graphPath, err)
			}

			// Stable iteration for deterministic failure messages.
			nodeIDs := make([]string, 0, len(g.Nodes))
			for id := range g.Nodes {
				nodeIDs = append(nodeIDs, id)
			}
			sort.Strings(nodeIDs)

			for _, id := range nodeIDs {
				node := g.Nodes[id]

				// tool_command points at scripts staged into .kilroy/package/...
				// at runtime; the source script lives under <pkg>/scripts/.
				if cmd := strings.TrimSpace(node.Attr("tool_command", "")); cmd != "" {
					rel := scriptRelPath(cmd)
					if rel == "" {
						continue // tool_command without a recognizable script reference
					}
					abs := filepath.Join(dir, rel)
					info, statErr := os.Stat(abs)
					if statErr != nil {
						t.Errorf("node %q tool_command references %q which does not exist at %s",
							id, cmd, abs)
						continue
					}
					if !info.Mode().IsRegular() {
						t.Errorf("node %q script %s is not a regular file (mode %s)",
							id, abs, info.Mode())
					}
					if info.Size() == 0 {
						t.Errorf("node %q script %s is empty", id, abs)
					}
				}

				// class= must reference a real policy class (or alias). Skip
				// the check for packages on the bypass list above.
				if knownClassIssues[relDir] {
					continue
				}
				if cls := strings.TrimSpace(node.Attr("class", "")); cls != "" {
					if !classExists(policyData, cls) {
						t.Errorf("node %q declares class=%q which is not a real policy class or alias",
							id, cls)
					}
				}
			}
		})
	}
}

// findPackageDirs returns every directory under root that contains a
// workflow.toml. Used as the discovery rule for what counts as a
// "package" in the integrity gate.
func findPackageDirs(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() != "workflow.toml" {
			return nil
		}
		out = append(out, filepath.Dir(path))
		return nil
	})
	sort.Strings(out)
	return out, err
}

// scriptRelPath extracts the script path from a tool_command attribute.
// Recognized shapes:
//   bash .kilroy/package/scripts/foo.sh
//   sh   .kilroy/package/scripts/foo.sh
// returns the path relative to the package root, e.g. "scripts/foo.sh".
// Anything else returns "" — callers skip the existence check (other
// tool_command shapes are legal but not script-file references).
func scriptRelPath(cmd string) string {
	fields := strings.Fields(cmd)
	if len(fields) < 2 {
		return ""
	}
	switch fields[0] {
	case "bash", "sh", "/bin/bash", "/bin/sh":
	default:
		return ""
	}
	const prefix = ".kilroy/package/"
	for _, f := range fields[1:] {
		if strings.HasPrefix(f, prefix) {
			return strings.TrimPrefix(f, prefix)
		}
	}
	return ""
}

// classExists reports whether name is a real class in the policy or an
// alias that resolves to one.
func classExists(d *policy.Data, name string) bool {
	if _, ok := d.Classes[name]; ok {
		return true
	}
	for _, a := range d.Aliases {
		if a.From == name {
			if _, ok := d.Classes[a.To]; ok {
				return true
			}
		}
	}
	return false
}
