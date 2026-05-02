// Regression coverage for workflows/review/scripts/stage-context.sh.
// The reviewer caught two bugs that broke the review workflow's core
// path:
//
//  1. Branch-ref targets diffed BASE..HEAD instead of BASE..$TARGET, so
//     "review feature from main" produced an empty diff (caller's branch
//     was main; main..main is empty) instead of the feature's changes.
//  2. A one-section INPUT.md (only `## target` plus a value) crashed the
//     script before .kilroy/diff.patch was initialized, because the
//     sed-then-grep extraction returned no matches and pipefail aborted.
//
// These tests stand up a real git repo, drive the script via bash, and
// assert observable behavior. Living in internal/attractor/validate/
// keeps us out of the workflows → engine → validate import cycle.
package validate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// makeReviewTestRepo creates a git repo with two branches:
//   - main:    one commit (initial, file a.txt)
//   - feature: branched from initial, adds file b.txt
//
// HEAD ends on main. Returns the absolute repo path.
func makeReviewTestRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
	}
	run("git", "init", "-q", "-b", "main")
	run("git", "config", "user.email", "test@kilroy.local")
	run("git", "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("git", "add", "a.txt")
	run("git", "commit", "-q", "-m", "initial")
	run("git", "checkout", "-q", "-b", "feature")
	if err := os.WriteFile(filepath.Join(repo, "b.txt"), []byte("b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("git", "add", "b.txt")
	run("git", "commit", "-q", "-m", "add b on feature")
	run("git", "checkout", "-q", "main")
	return repo
}

// runStageContext executes the stage-context.sh script against the
// given repo with the given INPUT.md content, returns (exitCode,
// diffPath).
func runStageContext(t *testing.T, repo, inputContent string) (int, string) {
	t.Helper()
	repoRoot := findRepoRoot(t)
	script := filepath.Join(repoRoot, "workflows", "review", "scripts", "stage-context.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("script not found at %s: %v", script, err)
	}
	kilroyDir := filepath.Join(repo, ".kilroy")
	if err := os.MkdirAll(kilroyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(kilroyDir, "INPUT.md")
	if err := os.WriteFile(inputPath, []byte(inputContent), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "INPUT_FILE="+inputPath)
	out, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
		} else {
			t.Fatalf("unexpected error running script: %v\n%s", err, out)
		}
	}
	return exitCode, filepath.Join(kilroyDir, "diff.patch")
}

// TestReviewStageContext_BranchRef_DiffsTargetNotHEAD locks in the
// reviewer-flagged fix: when target is a git ref, the diff must show
// the target's changes (BASE..$TARGET), not the caller's HEAD's.
func TestReviewStageContext_BranchRef_DiffsTargetNotHEAD(t *testing.T) {
	repo := makeReviewTestRepo(t)
	// HEAD is main; target is feature. The diff must include feature's
	// b.txt commit.
	exitCode, diffPath := runStageContext(t, repo, `## target

feature

## context_files

`)
	if exitCode != 0 {
		t.Fatalf("script exited %d, want 0", exitCode)
	}
	diff, err := os.ReadFile(diffPath)
	if err != nil {
		t.Fatalf("read diff.patch: %v", err)
	}
	for _, want := range []string{
		"add b on feature", // commit subject from the feature branch
		"b.txt",            // new file introduced on feature
		"+b",               // hunk content
	} {
		if !strings.Contains(string(diff), want) {
			t.Errorf("diff.patch should contain %q (proves target's changes are diffed, not HEAD's)\nfull diff:\n%s",
				want, string(diff))
		}
	}
	// And the diff must NOT be the empty-range case (BASE..HEAD when HEAD
	// IS main, which the buggy version produced).
	if strings.Contains(string(diff), "feature..HEAD") {
		t.Errorf("diff.patch still references feature..HEAD; the fix should diff BASE..target\nfull:\n%s", string(diff))
	}
}

// TestReviewStageContext_OneSectionInput_DoesNotCrash locks in the
// reviewer-flagged fix: a minimal INPUT.md with only `## target` and a
// value (no trailing section) must not abort the script. The fragile
// sed/grep pipeline aborted under pipefail when grep had no matches;
// awk replaced it.
func TestReviewStageContext_OneSectionInput_DoesNotCrash(t *testing.T) {
	repo := makeReviewTestRepo(t)
	exitCode, diffPath := runStageContext(t, repo, `## target

feature
`)
	if exitCode != 0 {
		t.Fatalf("script exited %d, want 0 (one-section INPUT.md should not crash)", exitCode)
	}
	if _, err := os.Stat(diffPath); err != nil {
		t.Errorf(".kilroy/diff.patch was not created: %v", err)
	}
	diff, err := os.ReadFile(diffPath)
	if err != nil {
		t.Fatalf("read diff.patch: %v", err)
	}
	if !strings.Contains(string(diff), "b.txt") {
		t.Errorf("expected feature's diff in diff.patch, got:\n%s", string(diff))
	}
}

// TestReviewStageContext_NoTarget_NoDiff_NoCrash exercises the
// no-target degenerate case: an empty INPUT.md must produce an empty
// diff.patch and exit 0 (so the workflow can route through summary).
func TestReviewStageContext_NoTarget_NoDiff_NoCrash(t *testing.T) {
	repo := makeReviewTestRepo(t)
	exitCode, diffPath := runStageContext(t, repo, `## something_else

unrelated
`)
	if exitCode != 0 {
		t.Fatalf("script exited %d, want 0", exitCode)
	}
	info, err := os.Stat(diffPath)
	if err != nil {
		t.Fatalf("diff.patch missing: %v", err)
	}
	if info.Size() != 0 {
		raw, _ := os.ReadFile(diffPath)
		t.Errorf("diff.patch should be empty when no target, got %d bytes:\n%s", info.Size(), string(raw))
	}
}

// TestReviewStageContext_PatchTargetCopiedVerbatim covers the third
// target shape (a .patch file path) — verbatim copy into diff.patch.
func TestReviewStageContext_PatchTargetCopiedVerbatim(t *testing.T) {
	repo := makeReviewTestRepo(t)
	patchPath := filepath.Join(repo, "external.patch")
	patchContent := "diff --git a/x.go b/x.go\n+hello\n"
	if err := os.WriteFile(patchPath, []byte(patchContent), 0o644); err != nil {
		t.Fatal(err)
	}
	exitCode, diffPath := runStageContext(t, repo, `## target

`+patchPath+`
`)
	if exitCode != 0 {
		t.Fatalf("exit = %d, want 0", exitCode)
	}
	got, err := os.ReadFile(diffPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != patchContent {
		t.Errorf(".patch target should be copied verbatim. got:\n%q\nwant:\n%q", string(got), patchContent)
	}
}
