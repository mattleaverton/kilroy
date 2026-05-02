// Filesystem-based workflow discovery. Resolves workflow names to package
// directories via search-path order — used by `kilroy run <name>` and the
// `kilroy workflows list/describe` inspection commands. No embedding.

package workflows

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SearchPaths returns the list of directories where workflow packages live,
// in resolution order (highest precedence first). Each entry is the parent
// directory under which workflows live as <dir>/<name>/{workflow.toml,graph.dot,...}.
//
// Order:
//
//  1. KILROY_WORKFLOW_PATHS (colon-separated; left-most wins). Dev escape
//     hatch — set this to the repo's workflows/ directory to use the
//     in-tree definitions without copying.
//  2. <projectRoot>/.kilroy/workflows/  (when projectRoot != "")
//  3. $XDG_CONFIG_HOME/kilroy/workflows/  (or ~/.config/kilroy/workflows/)
//
// Empty entries (env var unset, no project root, no home dir) are
// skipped silently. Duplicates are collapsed in their first appearance
// so the same path doesn't show up twice from different sources.
func SearchPaths(projectRoot string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			abs = p
		}
		if seen[abs] {
			return
		}
		seen[abs] = true
		out = append(out, abs)
	}

	for _, p := range strings.Split(os.Getenv("KILROY_WORKFLOW_PATHS"), ":") {
		add(p)
	}
	if projectRoot != "" {
		add(filepath.Join(projectRoot, ".kilroy", "workflows"))
	}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		add(filepath.Join(xdg, "kilroy", "workflows"))
	} else if home, err := os.UserHomeDir(); err == nil {
		add(filepath.Join(home, ".config", "kilroy", "workflows"))
	}
	return out
}

// Discovered describes a workflow found by Discover.
type Discovered struct {
	Name string // workflow name (last path component of its directory)
	Dir  string // absolute path to the package directory
	// Source is the search-path root this workflow was found under;
	// useful for "describe" output to show where the package came from.
	Source string
}

// Find returns the highest-precedence package matching name, or nil if no
// search path holds a directory called name with a workflow.toml or graph.dot.
func Find(name string, projectRoot string) (*Discovered, error) {
	if name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	for _, root := range SearchPaths(projectRoot) {
		d := filepath.Join(root, name)
		if isWorkflowDir(d) {
			return &Discovered{Name: name, Dir: d, Source: root}, nil
		}
	}
	return nil, nil
}

// Discover returns every workflow found across all search paths in
// precedence order. When the same name exists at multiple levels, only
// the highest-precedence entry is included. Out is sorted by name so
// callers get stable ordering for list output.
func Discover(projectRoot string) ([]Discovered, error) {
	seen := map[string]bool{}
	var out []Discovered
	for _, root := range SearchPaths(projectRoot) {
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", root, err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if seen[name] {
				continue
			}
			d := filepath.Join(root, name)
			if !isWorkflowDir(d) {
				continue
			}
			seen[name] = true
			out = append(out, Discovered{Name: name, Dir: d, Source: root})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// isWorkflowDir reports whether dir looks like a workflow package — i.e.
// contains a workflow.toml or a graph.dot. Lenient on purpose: the
// existing LoadPackage falls back to *.dot when graph.dot is absent.
func isWorkflowDir(dir string) bool {
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, "workflow.toml")); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(dir, "graph.dot")); err == nil {
		return true
	}
	return false
}

// FindProjectRoot walks up from start looking for the first directory
// containing a .kilroy/ marker. Returns "" if no marker is found before
// $HOME or filesystem root, mirroring the upward-search rule in
// docs/plans/2026-05-01-kilroy-v2-final-plan.md §7.2.
func FindProjectRoot(start string) string {
	if start == "" {
		var err error
		start, err = os.Getwd()
		if err != nil {
			return ""
		}
	}
	abs, err := filepath.Abs(start)
	if err != nil {
		abs = start
	}
	home, _ := os.UserHomeDir()
	dir := abs
	for {
		if info, err := os.Stat(filepath.Join(dir, ".kilroy")); err == nil && info.IsDir() {
			return dir
		}
		if dir == home {
			return ""
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
