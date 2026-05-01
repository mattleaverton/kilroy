package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDetectStaleKilroyBuildFor_SameRepo_Stale verifies that when the binary
// lives inside a git repo and the invocation CWD is that same repo, the
// function returns ok=true and Stale=true (when the embedded revision differs
// from HEAD — the pre-existing dev-mode behavior).
func TestDetectStaleKilroyBuildFor_SameRepo_Stale(t *testing.T) {
	const builtRev = "deadbeef000000000000000000000000deadbeef"
	orig := embeddedBuildRevision
	embeddedBuildRevision = builtRev
	t.Cleanup(func() { embeddedBuildRevision = orig })

	repo := initTestRepo(t)
	fakeExe := filepath.Join(repo, "kilroy")
	if err := os.WriteFile(fakeExe, []byte("fake-binary"), 0o755); err != nil {
		t.Fatalf("write fake exe: %v", err)
	}

	status, ok := detectStaleKilroyBuildFor(fakeExe, repo)
	if !ok {
		t.Fatal("detectStaleKilroyBuildFor returned ok=false; expected ok=true for same-repo CWD")
	}
	if !status.Stale {
		t.Fatalf("expected Stale=true (built=%q head=%q)", status.BuiltRevision, status.HeadRevision)
	}
	if status.BuiltRevision != builtRev {
		t.Fatalf("BuiltRevision: got %q want %q", status.BuiltRevision, builtRev)
	}
}

// TestDetectStaleKilroyBuildFor_SubdirOfSameRepo_Stale verifies that a CWD
// that is a subdirectory of the binary's source repo also triggers the check
// (the common case when working inside a nested package directory).
func TestDetectStaleKilroyBuildFor_SubdirOfSameRepo_Stale(t *testing.T) {
	const builtRev = "deadbeef000000000000000000000000deadbeef"
	orig := embeddedBuildRevision
	embeddedBuildRevision = builtRev
	t.Cleanup(func() { embeddedBuildRevision = orig })

	repo := initTestRepo(t)
	fakeExe := filepath.Join(repo, "kilroy")
	if err := os.WriteFile(fakeExe, []byte("fake-binary"), 0o755); err != nil {
		t.Fatalf("write fake exe: %v", err)
	}

	// Simulate running from cmd/kilroy/ inside the same repo.
	subdir := filepath.Join(repo, "cmd", "kilroy")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	status, ok := detectStaleKilroyBuildFor(fakeExe, subdir)
	if !ok {
		t.Fatal("detectStaleKilroyBuildFor returned ok=false; expected ok=true for subdir of same repo")
	}
	if !status.Stale {
		t.Fatalf("expected Stale=true (built=%q head=%q)", status.BuiltRevision, status.HeadRevision)
	}
}

// TestDetectStaleKilroyBuildFor_DifferentDirNoGit_NoOp verifies that when the
// invocation CWD is a plain directory with no git repo, the function returns
// (staleBuildStatus{}, false) — a silent no-op — even if the binary itself is
// stale relative to its source repo.
func TestDetectStaleKilroyBuildFor_DifferentDirNoGit_NoOp(t *testing.T) {
	const builtRev = "deadbeef000000000000000000000000deadbeef"
	orig := embeddedBuildRevision
	embeddedBuildRevision = builtRev
	t.Cleanup(func() { embeddedBuildRevision = orig })

	repo := initTestRepo(t)
	fakeExe := filepath.Join(repo, "kilroy")
	if err := os.WriteFile(fakeExe, []byte("fake-binary"), 0o755); err != nil {
		t.Fatalf("write fake exe: %v", err)
	}

	// plainDir is outside any git repo (a raw temp directory, no .git ancestor).
	plainDir := t.TempDir()

	// Sanity-check: plainDir should not be inside a git repo.  If it is (e.g.
	// the OS temp dir is inside some repo), skip rather than fail with a
	// misleading assertion.
	if _, ok := gitTopLevel(plainDir); ok {
		t.Skip("temp dir appears to be inside a git repo; skipping no-git test")
	}

	status, ok := detectStaleKilroyBuildFor(fakeExe, plainDir)
	if ok {
		t.Fatalf("expected ok=false (no-op) for CWD outside any git repo; got status=%+v", status)
	}
	if status != (staleBuildStatus{}) {
		t.Fatalf("expected zero staleBuildStatus for no-op; got %+v", status)
	}
}

// TestDetectStaleKilroyBuildFor_DifferentGitRepo_NoOp verifies that when the
// invocation CWD is inside a *different* git repo than the binary's source repo,
// the function returns (staleBuildStatus{}, false) — the core bug fix: a user
// running kilroy from /tmp/some-other-project should never see a stale-build
// error triggered by the kilroy dev repo having uncommitted work.
func TestDetectStaleKilroyBuildFor_DifferentGitRepo_NoOp(t *testing.T) {
	const builtRev = "deadbeef000000000000000000000000deadbeef"
	orig := embeddedBuildRevision
	embeddedBuildRevision = builtRev
	t.Cleanup(func() { embeddedBuildRevision = orig })

	// Binary lives in one git repo.
	binaryRepo := initTestRepo(t)
	fakeExe := filepath.Join(binaryRepo, "kilroy")
	if err := os.WriteFile(fakeExe, []byte("fake-binary"), 0o755); err != nil {
		t.Fatalf("write fake exe: %v", err)
	}

	// User is invoking from a completely different git repo.
	otherRepo := initTestRepo(t)

	status, ok := detectStaleKilroyBuildFor(fakeExe, otherRepo)
	if ok {
		t.Fatalf("expected ok=false (no-op) for CWD in a different git repo; got status=%+v", status)
	}
	if status != (staleBuildStatus{}) {
		t.Fatalf("expected zero staleBuildStatus for no-op; got %+v", status)
	}
}
