// Tests for `kilroy run <workflow-name>`. We assert the discovery surface
// only — the engine path is the same one attractor run already covers.
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// makePackageDir creates a minimal workflow package — just a workflow.toml
// and a graph.dot — at root/<name> so the discovery layer recognizes it.
// graph.dot is intentionally invalid (no required structure); we won't
// reach the engine in these tests, only the resolution step.
func makePackageDir(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"),
		[]byte("name = \""+name+"\"\n"), 0o644); err != nil {
		t.Fatalf("toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"),
		[]byte("digraph "+name+" {}\n"), 0o644); err != nil {
		t.Fatalf("dot: %v", err)
	}
	return dir
}

func TestRunCmd_UnknownWorkflow_Exit1WithSearchPaths(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "run", "definitely-not-a-workflow")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+t.TempDir(),
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1, got exit 0")
	}
	for _, want := range []string{
		`workflow "definitely-not-a-workflow" not found`,
		"searched (highest precedence first)",
		"KILROY_WORKFLOW_PATHS",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q\nfull stderr:\n%s", want, stderr.String())
		}
	}
}

func TestRunCmd_NoArgs_Exit1WithUsage(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "run")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1, got exit 0")
	}
	if !strings.Contains(stderr.String(), "kilroy run <workflow-name>") {
		t.Errorf("stderr missing usage line; got:\n%s", stderr.String())
	}
}

func TestRunCmd_WorkflowHelpDescribesWorkflow(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	writePackage(t, pkgRoot, "tiny-investigate", v2InvestigateToml)

	cmd := exec.Command(bin, "run", "tiny-investigate", "--help")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kilroy run tiny-investigate --help failed: %v\n%s", err, out)
	}
	for _, want := range []string{"name:", "tiny-investigate", "inputs:", "question"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("output missing %q\n%s", want, out)
		}
	}
}

// `kilroy run --flag` now routes to direct mode (ad-hoc graph/package
// invocation) instead of erroring. This test exercises the direct-mode
// dispatch with no graph: the engine surfaces a missing-input error,
// which proves we entered direct mode rather than the workflow-name
// path. The pre-v2 behavior (rejecting flag-as-first-arg) is retired.
func TestRunCmd_FlagAsFirstArg_DispatchesToDirectMode(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "run", "--detach")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit (no graph specified), got exit 0")
	}
	// Direct mode reaches attractorRun, which errors on missing
	// --graph/--package. The pre-v2 "first argument must be a workflow
	// name" rejection is gone.
	if strings.Contains(stderr.String(), "first argument must be a workflow name") {
		t.Errorf("stderr still has the pre-v2 reject message; should route to direct mode now:\n%s", stderr.String())
	}
}

// TestRunCmd_ResolvesViaKILROYWorkflowPaths exercises the discovery happy
// path up to the point the engine takes over: with KILROY_WORKFLOW_PATHS
// pointing at a directory containing <name>/workflow.toml + graph.dot,
// the resolver finds it and forwards to attractor run, which then fails
// with a different (engine-side) error proving the resolution worked.
func TestRunCmd_ResolvesViaKILROYWorkflowPaths(t *testing.T) {
	bin := buildTestBinary(t)
	pkgRoot := t.TempDir()
	makePackageDir(t, pkgRoot, "smoke")

	cmd := exec.Command(bin, "run", "smoke", "--no-cxdb", "--allow-test-shim")
	cmd.Env = append(os.Environ(),
		"KILROY_WORKFLOW_PATHS="+pkgRoot,
		"XDG_CONFIG_HOME="+t.TempDir(),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	_ = cmd.Run()
	// We don't care about exit code — the empty graph.dot will fail engine
	// validation. We DO care that the error is engine-side, not "workflow
	// not found", which proves resolution succeeded and forwarded.
	s := stderr.String()
	if strings.Contains(s, `workflow "smoke" not found`) {
		t.Errorf("resolver failed to find smoke; stderr:\n%s", s)
	}
}
