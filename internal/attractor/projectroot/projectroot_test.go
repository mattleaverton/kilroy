// Tests for project-root discovery rules.
package projectroot

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// makeMarker creates dir/.kilroy/ so hasMarker(dir) is true.
func makeMarker(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, ".kilroy"), 0o755); err != nil {
		t.Fatalf("mkdir .kilroy: %v", err)
	}
}

// resolved returns the symlink-resolved form of p so /tmp vs
// /private/tmp on macOS doesn't break path equality.
func resolved(t *testing.T, p string) string {
	t.Helper()
	if p == "" {
		return p
	}
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		return p
	}
	return r
}

// clearEnv removes KILROY_PROJECT_ROOT for the duration of a test so
// upward-search cases aren't affected by an inherited override.
func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv(EnvVar, "")
}

func TestFind_UpwardHitsImmediateCwd(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	makeMarker(t, dir)

	root, source, err := Find(dir)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if source != SourceUpward {
		t.Errorf("source = %q, want %q", source, SourceUpward)
	}
	if resolved(t, root) != resolved(t, dir) {
		t.Errorf("root = %q, want %q", root, dir)
	}
}

func TestFind_UpwardHits3LevelsUp(t *testing.T) {
	clearEnv(t)
	top := t.TempDir()
	nested := filepath.Join(top, "a", "b", "c")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	makeMarker(t, top)

	root, source, err := Find(nested)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if source != SourceUpward {
		t.Errorf("source = %q, want %q", source, SourceUpward)
	}
	if resolved(t, root) != resolved(t, top) {
		t.Errorf("root = %q, want %q", root, top)
	}
}

func TestFind_NoMarkerReturnsNone(t *testing.T) {
	clearEnv(t)
	// Point HOME at the temp dir so the walk terminates at HOME
	// before escaping into the real user tree (which may contain
	// a .kilroy/ from prior runs).
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	nested := filepath.Join(dir, "x", "y")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}

	root, source, err := Find(nested)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if root != "" {
		t.Errorf("root = %q, want empty", root)
	}
	if source != SourceNone {
		t.Errorf("source = %q, want %q", source, SourceNone)
	}
}

func TestFind_EnvOverrideWithMarker(t *testing.T) {
	dir := t.TempDir()
	makeMarker(t, dir)
	t.Setenv(EnvVar, dir)

	// start is intentionally a different directory with no marker —
	// the env override should be used regardless.
	other := t.TempDir()
	root, source, err := Find(other)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if source != SourceEnv {
		t.Errorf("source = %q, want %q", source, SourceEnv)
	}
	if resolved(t, root) != resolved(t, dir) {
		t.Errorf("root = %q, want %q", root, dir)
	}
}

func TestFind_EnvOverrideMissingMarkerErrors(t *testing.T) {
	dir := t.TempDir() // no .kilroy/ inside
	t.Setenv(EnvVar, dir)

	root, _, err := Find("")
	if err == nil {
		t.Fatal("Find: want error, got nil")
	}
	if !errors.Is(err, ErrEnvRootMissingMarker) {
		t.Errorf("error = %v, want wrap of ErrEnvRootMissingMarker", err)
	}
	if root != "" {
		t.Errorf("root = %q, want empty on error", root)
	}
}

func TestFind_StopsAtHome(t *testing.T) {
	clearEnv(t)
	// Build a tree where the marker exists ABOVE $HOME. The walk
	// must not cross $HOME, so we expect SourceNone.
	above := t.TempDir()
	makeMarker(t, above)
	home := filepath.Join(above, "home")
	nested := filepath.Join(home, "user", "proj")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)

	root, source, err := Find(nested)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if root != "" || source != SourceNone {
		t.Errorf("Find = (%q, %q), want (\"\", %q) — walk should stop at $HOME",
			root, source, SourceNone)
	}
}
