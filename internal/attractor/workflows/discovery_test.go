// Tests for filesystem-based workflow discovery.
package workflows

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// makeWorkflow creates a minimal package directory with a workflow.toml
// containing only `name = <name>` so the discovery layer recognizes it.
func makeWorkflow(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := "name = \"" + name + "\"\n"
	if err := os.WriteFile(filepath.Join(dir, "workflow.toml"), []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "graph.dot"), []byte("digraph "+name+" {}\n"), 0o644); err != nil {
		t.Fatalf("write dot: %v", err)
	}
	return dir
}

func TestSearchPaths_KILROYWorkflowPathsBeatsProjectAndUser(t *testing.T) {
	envRoot := t.TempDir()
	projRoot := t.TempDir()
	userRoot := t.TempDir()
	t.Setenv("KILROY_WORKFLOW_PATHS", envRoot)
	t.Setenv("XDG_CONFIG_HOME", userRoot)

	paths := SearchPaths(projRoot)
	if len(paths) < 3 {
		t.Fatalf("expected at least 3 search paths, got %d: %v", len(paths), paths)
	}
	wantOrder := []string{
		envRoot,
		filepath.Join(projRoot, ".kilroy", "workflows"),
		filepath.Join(userRoot, "kilroy", "workflows"),
	}
	for i, want := range wantOrder {
		if paths[i] != want {
			t.Errorf("paths[%d] = %q, want %q\nfull: %v", i, paths[i], want, paths)
		}
	}
}

func TestSearchPaths_DedupesSamePathFromDifferentSources(t *testing.T) {
	root := t.TempDir()
	t.Setenv("KILROY_WORKFLOW_PATHS", root)
	t.Setenv("XDG_CONFIG_HOME", root)

	// Both KILROY_WORKFLOW_PATHS and XDG_CONFIG_HOME would expand to
	// paths that share the same root in this contrived case. The
	// XDG expansion is "$XDG_CONFIG_HOME/kilroy/workflows" so it
	// won't actually collide with the env var, but we test the
	// dedupe path explicitly by setting the env twice.
	t.Setenv("KILROY_WORKFLOW_PATHS", root+":"+root)

	paths := SearchPaths("")
	count := 0
	for _, p := range paths {
		if p == root {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected %q to appear exactly once, got %d times: %v", root, count, paths)
	}
}

func TestFind_PrecedenceProjectShadowsUser(t *testing.T) {
	projRoot := t.TempDir()
	userRoot := t.TempDir()
	t.Setenv("KILROY_WORKFLOW_PATHS", "")
	t.Setenv("XDG_CONFIG_HOME", userRoot)

	makeWorkflow(t, filepath.Join(userRoot, "kilroy", "workflows"), "review")
	projDir := makeWorkflow(t, filepath.Join(projRoot, ".kilroy", "workflows"), "review")

	got, err := Find("review", projRoot)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got == nil {
		t.Fatal("Find: got nil, want a hit")
	}
	if got.Dir != projDir {
		t.Errorf("Find returned dir=%q, want %q (project should shadow user)", got.Dir, projDir)
	}
}

func TestFind_KILROYWorkflowPathsBeatsProject(t *testing.T) {
	envRoot := t.TempDir()
	projRoot := t.TempDir()
	t.Setenv("KILROY_WORKFLOW_PATHS", envRoot)
	t.Setenv("XDG_CONFIG_HOME", "")

	envDir := makeWorkflow(t, envRoot, "fix")
	makeWorkflow(t, filepath.Join(projRoot, ".kilroy", "workflows"), "fix")

	got, err := Find("fix", projRoot)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got == nil || got.Dir != envDir {
		t.Errorf("Find dir = %v, want %q (KILROY_WORKFLOW_PATHS should win)", got, envDir)
	}
}

func TestFind_NotFoundReturnsNil(t *testing.T) {
	t.Setenv("KILROY_WORKFLOW_PATHS", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	got, err := Find("does-not-exist", "")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got != nil {
		t.Errorf("Find returned %+v, want nil for missing workflow", got)
	}
}

func TestDiscover_AggregatesAndShadows(t *testing.T) {
	envRoot := t.TempDir()
	projRoot := t.TempDir()
	userRoot := t.TempDir()
	t.Setenv("KILROY_WORKFLOW_PATHS", envRoot)
	t.Setenv("XDG_CONFIG_HOME", userRoot)

	// Three workflows total: env-only, project-only, user-only.
	envDir := makeWorkflow(t, envRoot, "alpha")
	projDir := makeWorkflow(t, filepath.Join(projRoot, ".kilroy", "workflows"), "beta")
	userDir := makeWorkflow(t, filepath.Join(userRoot, "kilroy", "workflows"), "gamma")
	// Plus a shadow: alpha also exists in user, but env wins.
	makeWorkflow(t, filepath.Join(userRoot, "kilroy", "workflows"), "alpha")

	got, err := Discover(projRoot)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Discover returned %d, want 3: %+v", len(got), got)
	}
	// Sorted by name.
	wantNames := []string{"alpha", "beta", "gamma"}
	wantDirs := []string{envDir, projDir, userDir}
	gotNames := make([]string, len(got))
	gotDirs := make([]string, len(got))
	for i, d := range got {
		gotNames[i] = d.Name
		gotDirs[i] = d.Dir
	}
	if !equal(gotNames, wantNames) {
		t.Errorf("names = %v, want %v", gotNames, wantNames)
	}
	if !equal(gotDirs, wantDirs) {
		t.Errorf("dirs = %v, want %v", gotDirs, wantDirs)
	}
}

func TestFindProjectRoot_StopsAtKilroyMarker(t *testing.T) {
	deep := t.TempDir()
	nested := filepath.Join(deep, "a", "b", "c")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(deep, ".kilroy"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := FindProjectRoot(nested)
	// Resolve symlinks since macOS /tmp is /private/tmp.
	wantResolved, _ := filepath.EvalSymlinks(deep)
	gotResolved, _ := filepath.EvalSymlinks(got)
	if gotResolved != wantResolved {
		t.Errorf("FindProjectRoot(%q) = %q, want %q", nested, got, deep)
	}
}

func TestFindProjectRoot_NoMarkerReturnsEmpty(t *testing.T) {
	dir := t.TempDir()
	got := FindProjectRoot(dir)
	if got != "" {
		t.Errorf("FindProjectRoot in marker-less tree = %q, want empty", got)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string{}, a...)
	bb := append([]string{}, b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
