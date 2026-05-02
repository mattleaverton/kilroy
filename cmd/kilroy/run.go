// `kilroy run <name>` resolves a workflow name through filesystem
// discovery and dispatches to the existing engine entry point. No bare
// form, no embedding — discovery searches KILROY_WORKFLOW_PATHS, then
// the project's .kilroy/workflows/, then the user's $XDG_CONFIG_HOME.

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/workflows"
)

// runCmd implements `kilroy run <workflow-name> [flags]`. It resolves
// <workflow-name> against the workflow registry and forwards the rest
// of the args to attractorRun via --package <dir>. This is intentionally
// a thin shell over the existing engine entry — the v2 surface change is
// in name resolution, not in run mechanics.
func runCmd(args []string) {
	if len(args) == 0 {
		runUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "-h", "--help", "help":
		runUsage()
		os.Exit(0)
	}

	// First positional arg is the workflow name; everything after passes
	// through unchanged. Reject extra positionals so users don't get
	// silent-mismatch behavior when they meant a flag.
	name := args[0]
	rest := args[1:]
	if strings.HasPrefix(name, "-") {
		fmt.Fprintf(os.Stderr, "kilroy run: first argument must be a workflow name, got flag %q\n", name)
		runUsage()
		os.Exit(1)
	}

	cwd, _ := os.Getwd()
	projectRoot := workflows.FindProjectRoot(cwd)

	pkg, err := workflows.Find(name, projectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kilroy run: %v\n", err)
		os.Exit(1)
	}
	if pkg == nil {
		fmt.Fprintf(os.Stderr, "kilroy run: workflow %q not found\n\n", name)
		printSearchedPaths(projectRoot)
		os.Exit(1)
	}

	// Forward to attractor run with --package resolved. Insert at the
	// front so user-supplied flags downstream of the name still parse
	// correctly (no positional arg is required by attractor run beyond
	// the package itself).
	forwarded := append([]string{"--package", pkg.Dir}, rest...)
	attractorRun(forwarded)
}

func runUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy run <workflow-name> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Flags pass through to the engine; see `kilroy attractor run --help`.")
	fmt.Fprintln(os.Stderr, "Common flags: --tmux, --detach, --workspace <dir>, --input <path|json>, --label KEY=VALUE")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Workflow discovery searches (highest precedence first):")
	fmt.Fprintln(os.Stderr, "  1. KILROY_WORKFLOW_PATHS  (colon-separated env var)")
	fmt.Fprintln(os.Stderr, "  2. <project-root>/.kilroy/workflows/  (when a .kilroy/ marker is found)")
	fmt.Fprintln(os.Stderr, "  3. $XDG_CONFIG_HOME/kilroy/workflows/  (default: ~/.config/kilroy/workflows/)")
}

func printSearchedPaths(projectRoot string) {
	fmt.Fprintln(os.Stderr, "searched (highest precedence first):")
	for _, p := range workflows.SearchPaths(projectRoot) {
		_, err := os.Stat(p)
		marker := "  ✗ (does not exist)"
		if err == nil {
			marker = "  ✓"
		}
		fmt.Fprintf(os.Stderr, "  %s%s\n", p, marker)
	}
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "set KILROY_WORKFLOW_PATHS to a directory containing the workflow,")
	fmt.Fprintln(os.Stderr, "or place it under .kilroy/workflows/ in this project's root.")
}
