package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureGitignoreKilroy_UsesLocalExcludeNotProjectGitignore(t *testing.T) {
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")
	originalGitignore := "dist/\n"
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(originalGitignore), 0o644); err != nil {
		t.Fatal(err)
	}

	ensureGitignoreKilroy(repo)

	got, err := os.ReadFile(filepath.Join(repo, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != originalGitignore {
		t.Fatalf(".gitignore changed: got %q want %q", string(got), originalGitignore)
	}

	cmd := exec.Command("git", "-C", repo, "check-ignore", ".kilroy/INPUT.md")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf(".kilroy/INPUT.md should be ignored by local exclude: %v\n%s", err, out)
	}
}

func TestEnsureGitignoreKilroy_DoesNotDuplicateLocalExclude(t *testing.T) {
	repo := t.TempDir()
	runCmd(t, repo, "git", "init")

	ensureGitignoreKilroy(repo)
	ensureGitignoreKilroy(repo)

	excludePath := strings.TrimSpace(runCmdOut(t, repo, "git", "rev-parse", "--git-path", "info/exclude"))
	if !filepath.IsAbs(excludePath) {
		excludePath = filepath.Join(repo, excludePath)
	}
	data, err := os.ReadFile(excludePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), ".kilroy/"); got != 1 {
		t.Fatalf("local exclude .kilroy/ count = %d, want 1\n%s", got, data)
	}
}
