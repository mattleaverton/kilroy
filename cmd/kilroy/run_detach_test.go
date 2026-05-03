package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLaunchDetached_SetsCmdDirToLogsRoot(t *testing.T) {
	logsRoot := t.TempDir()
	cwdPath := filepath.Join(logsRoot, "cwd.txt")

	oldExec := detachedExecCommand
	t.Cleanup(func() { detachedExecCommand = oldExec })
	detachedExecCommand = func(name string, args ...string) *exec.Cmd {
		_ = name
		_ = args
		return exec.Command("bash", "-c", fmt.Sprintf("pwd > %q", cwdPath))
	}

	if err := launchDetached([]string{"run"}, logsRoot); err != nil {
		t.Fatalf("launchDetached: %v", err)
	}

	waitForFile(t, cwdPath, 5*time.Second)
	gotRaw, err := os.ReadFile(cwdPath)
	if err != nil {
		t.Fatalf("read cwd file: %v", err)
	}
	got := strings.TrimSpace(string(gotRaw))
	if got != logsRoot {
		t.Fatalf("child cwd mismatch: got %q want %q", got, logsRoot)
	}
}

func TestLaunchDetached_UsesAbsoluteExecutablePath(t *testing.T) {
	logsRoot := t.TempDir()

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"./kilroy", "run", "--detach"}

	var gotName string
	oldExec := detachedExecCommand
	t.Cleanup(func() { detachedExecCommand = oldExec })
	detachedExecCommand = func(name string, args ...string) *exec.Cmd {
		gotName = name
		return exec.Command("bash", "-c", "sleep 0.1")
	}

	if err := launchDetached([]string{"run"}, logsRoot); err != nil {
		t.Fatalf("launchDetached: %v", err)
	}

	if gotName == "" {
		t.Fatal("detached executable path was not captured")
	}
	if !filepath.IsAbs(gotName) {
		t.Fatalf("detached executable path must be absolute, got %q", gotName)
	}
	if gotName == "./kilroy" {
		t.Fatalf("detached executable path must not use relative argv0, got %q", gotName)
	}
}

func TestDetachedExecutablePath_NormalizesRelativeOSExecutable(t *testing.T) {
	oldOSExecutable := detachedOSExecutable
	t.Cleanup(func() { detachedOSExecutable = oldOSExecutable })
	detachedOSExecutable = func() (string, error) {
		return "./kilroy", nil
	}

	path, err := detachedExecutablePath()
	if err != nil {
		t.Fatalf("detachedExecutablePath: %v", err)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("detachedExecutablePath must return absolute path, got %q", path)
	}
}
