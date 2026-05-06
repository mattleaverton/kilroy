// Filesystem-based workflow discovery. Resolves workflow names to package
// directories via search-path order — used by `kilroy run <name>` and the
// `kilroy workflows list/describe` inspection commands.

package workflows

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/projectroot"
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
//  4. $XDG_DATA_HOME/kilroy/workflows/   (or ~/.local/share/kilroy/workflows/ on Unix,
//     %LOCALAPPDATA%\kilroy\workflows\ on Windows) — where install scripts copy built-ins.
//  5. The source checkout's workflows/ directory when the binary was built from
//     a local checkout and that checkout still exists. This is a development
//     fallback so `go build ./cmd/kilroy` works from downstream repos without
//     manual KILROY_WORKFLOW_PATHS wiring.
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
	add(dataDir())
	add(sourceWorkflowRoot())
	return out
}

// dataDir returns the platform-appropriate installed-workflows directory.
func dataDir() string {
	// Windows: %LOCALAPPDATA%\kilroy\workflows
	if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
		return filepath.Join(localAppData, "kilroy", "workflows")
	}
	// Unix: $XDG_DATA_HOME/kilroy/workflows or ~/.local/share/kilroy/workflows
	if xdgData := os.Getenv("XDG_DATA_HOME"); xdgData != "" {
		return filepath.Join(xdgData, "kilroy", "workflows")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "share", "kilroy", "workflows")
	}
	return ""
}

var sourceWorkflowRoot = defaultSourceWorkflowRoot

func defaultSourceWorkflowRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "workflows"))
	if isDir(root) {
		return root
	}
	return ""
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
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
	if !isDir(dir) {
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

// FindProjectRoot returns the project root resolved by upward search
// for a .kilroy/ marker, or "" when nothing is found. Thin shim around
// projectroot.Find for callers that don't need source/error
// information; if KILROY_PROJECT_ROOT is set but invalid, this returns
// "" silently. CLI entry points should call projectroot.Find directly
// to surface the loud-failure semantics from §7.4.
func FindProjectRoot(start string) string {
	root, _, _ := projectroot.Find(start)
	return root
}
