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

// runCmd implements `kilroy run` with two argument shapes:
//
//  1. Workflow mode: `kilroy run <workflow-name> [flags]` — resolves
//     <workflow-name> via filesystem discovery and forwards with
//     --package <dir>.
//
//  2. Direct mode: `kilroy run --graph <file.dot>` or
//     `kilroy run --package <dir>` — flags pass straight through to
//     the engine. Used by automation that targets ad-hoc graphs
//     without packaging them as workflows.
//
// A leading flag (starts with "-") routes to direct mode; otherwise the
// first positional is treated as a workflow name. Direct mode exposes
// the engine's full flag surface (--tmux, --detach, --config, --validate,
// etc.) — `kilroy run --help` documents the common ones.
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

	// Direct mode: first arg is a flag (--graph / --package / --config /
	// etc.) — pass straight through to the engine. Same flag set the
	// pre-v2 `attractor run` accepted.
	if strings.HasPrefix(args[0], "-") {
		attractorRun(args)
		return
	}

	// Workflow mode: first arg is a workflow name; resolve via discovery
	// and forward with --package <dir> followed by the remaining args.
	name := args[0]
	rest := args[1:]

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

	forwarded := append([]string{"--package", pkg.Dir}, rest...)
	attractorRun(forwarded)
}

func runUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy run <workflow-name> [flags]      (workflow mode — resolves <name> via discovery)")
	fmt.Fprintln(os.Stderr, "  kilroy run --graph <file.dot> [flags]   (direct mode — ad-hoc graph)")
	fmt.Fprintln(os.Stderr, "  kilroy run --package <dir> [flags]      (direct mode — ad-hoc package)")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Flags:")
	fmt.Fprintln(os.Stderr, "  --graph <file.dot>              direct mode: ad-hoc graph (alternative to <workflow-name>)")
	fmt.Fprintln(os.Stderr, "  --package <dir>                 direct mode: ad-hoc package (alternative to <workflow-name>)")
	fmt.Fprintln(os.Stderr, "  --tmux                          run agent nodes in tmux sessions (CLI driver)")
	fmt.Fprintln(os.Stderr, "  --detach                        background the run; print run_id + logs_root")
	fmt.Fprintln(os.Stderr, "  --workspace <dir>               source repo (default: cwd)")
	fmt.Fprintln(os.Stderr, "  --input <path|json>             inputs map (file path or inline JSON)")
	fmt.Fprintln(os.Stderr, "  --prompt-file <file>            convenience: file → input.prompt")
	fmt.Fprintln(os.Stderr, "  --label KEY=VALUE               attach labels to the run record (repeatable)")
	fmt.Fprintln(os.Stderr, "  --validate                      prelaunch-only; don't execute")
	fmt.Fprintln(os.Stderr, "  --no-cxdb                       skip CXDB streaming")
	fmt.Fprintln(os.Stderr, "  --force-model <provider=model>  override the resolved model for one provider")
	fmt.Fprintln(os.Stderr, "  --confirm-stale-build           bypass the dev-build-vs-source-tree check")
	fmt.Fprintln(os.Stderr, "  --allow-test-shim               permit test_shim provider profile (CI use)")
	fmt.Fprintln(os.Stderr, "  --config <run.yaml>             explicit run config (advanced; default is auto)")
	fmt.Fprintln(os.Stderr, "  --run-id <id>                   pin a specific ULID (default: generate)")
	fmt.Fprintln(os.Stderr, "  --logs-root <dir>               override logs_root location (default: XDG state)")
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
