package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAttractorRun_DetachedModeSurvivesLauncherExit(t *testing.T) {
	bin := buildKilroyBinary(t)
	cxdb := newCXDBTestServer(t)
	repo := initTestRepo(t)
	catalog := writePinnedCatalog(t)
	cfg := writeRunConfig(t, repo, cxdb.URL(), cxdb.BinaryAddr(), catalog)
	graph := writeDetachGraph(t)
	logs := filepath.Join(t.TempDir(), "logs")

	cmd := exec.Command(
		bin,
		"attractor", "run",
		"--detach",
		"--graph", graph,
		"--config", cfg,
		"--run-id", "detach-smoke",
		"--logs-root", logs,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("detached launch failed: %v\n%s", err, out)
	}

	pidPath := filepath.Join(logs, "run.pid")
	waitForFile(t, pidPath, 5*time.Second)
	pid := readPIDFile(t, pidPath)
	waitForFile(t, filepath.Join(logs, "final.json"), 20*time.Second)
	waitForProcessExit(t, pid, 10*time.Second)
}

func TestAttractorRun_DetachedWritesPIDFile(t *testing.T) {
	bin := buildKilroyBinary(t)
	cxdb := newCXDBTestServer(t)
	repo := initTestRepo(t)
	catalog := writePinnedCatalog(t)
	cfg := writeRunConfig(t, repo, cxdb.URL(), cxdb.BinaryAddr(), catalog)
	graph := writeDetachGraph(t)
	logs := filepath.Join(t.TempDir(), "logs")

	cmd := exec.Command(
		bin,
		"attractor", "run",
		"--detach",
		"--graph", graph,
		"--config", cfg,
		"--run-id", "detach-pid",
		"--logs-root", logs,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("detached launch failed: %v\n%s", err, out)
	}

	pidPath := filepath.Join(logs, "run.pid")
	waitForFile(t, pidPath, 5*time.Second)
	pid := readPIDFile(t, pidPath)

	waitForFile(t, filepath.Join(logs, "final.json"), 20*time.Second)
	waitForProcessExit(t, pid, 10*time.Second)
}

func TestAttractorRun_DetachedMode_DeletedLauncherCWDDoesNotAbortRun(t *testing.T) {
	bin := buildKilroyBinary(t)
	cxdb := newCXDBTestServer(t)
	repo := initTestRepo(t)
	catalog := writePinnedCatalog(t)
	configAbs := writeRunConfig(t, repo, cxdb.URL(), cxdb.BinaryAddr(), catalog)
	graphAbs := writeDetachGraph(t)
	logs := filepath.Join(t.TempDir(), "logs")

	root := t.TempDir()
	inputsDir := filepath.Join(root, "inputs")
	if err := os.MkdirAll(inputsDir, 0o755); err != nil {
		t.Fatalf("mkdir inputs: %v", err)
	}
	launcherDir := filepath.Join(root, "launcher")
	if err := os.MkdirAll(launcherDir, 0o755); err != nil {
		t.Fatalf("mkdir launcher: %v", err)
	}
	graphBytes, err := os.ReadFile(graphAbs)
	if err != nil {
		t.Fatalf("read graph: %v", err)
	}
	stableGraphPath := filepath.Join(inputsDir, "g.dot")
	if err := os.WriteFile(stableGraphPath, graphBytes, 0o644); err != nil {
		t.Fatalf("write graph: %v", err)
	}
	configBytes, err := os.ReadFile(configAbs)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	stableConfigPath := filepath.Join(inputsDir, "run.yaml")
	if err := os.WriteFile(stableConfigPath, configBytes, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	relGraphPath, err := filepath.Rel(launcherDir, stableGraphPath)
	if err != nil {
		t.Fatalf("rel graph: %v", err)
	}
	relConfigPath, err := filepath.Rel(launcherDir, stableConfigPath)
	if err != nil {
		t.Fatalf("rel config: %v", err)
	}

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(launcherDir); err != nil {
		t.Fatalf("chdir launcher: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	cmd := exec.Command(
		bin,
		"attractor", "run",
		"--detach",
		"--graph", relGraphPath,
		"--config", relConfigPath,
		"--run-id", "detach-deleted-cwd",
		"--logs-root", logs,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("detached launch failed: %v\n%s", err, out)
	}

	if err := os.Chdir(oldWD); err != nil {
		t.Fatalf("restore cwd: %v", err)
	}
	if err := os.RemoveAll(launcherDir); err != nil {
		t.Fatalf("remove launcher dir: %v", err)
	}

	waitForFile(t, filepath.Join(logs, "run.pid"), 5*time.Second)
	pid := readPIDFile(t, filepath.Join(logs, "run.pid"))
	waitForFile(t, filepath.Join(logs, "final.json"), 20*time.Second)
	waitForProcessExit(t, pid, 10*time.Second)

	runOutBytes, err := os.ReadFile(filepath.Join(logs, "run.out"))
	if err != nil {
		t.Fatalf("read run.out: %v", err)
	}
	runOut := string(runOutBytes)
	if strings.Contains(runOut, "tool read_file schema: getwd: no such file or directory") {
		t.Fatalf("detached run should not fail due to deleted launcher cwd:\n%s", runOut)
	}

	var final map[string]any
	finalBytes, err := os.ReadFile(filepath.Join(logs, "final.json"))
	if err != nil {
		t.Fatalf("read final.json: %v", err)
	}
	if err := json.Unmarshal(finalBytes, &final); err != nil {
		t.Fatalf("decode final.json: %v", err)
	}
	if strings.TrimSpace(anyToString(final["status"])) == "fail" {
		t.Fatalf("expected detached run to complete successfully; final=%v", final)
	}
}

func writeDetachGraph(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "g.dot")
	_ = os.WriteFile(path, []byte(`
digraph G {
  start [shape=Mdiamond]
  t [shape=parallelogram, tool_command="sleep 1"]
  exit [shape=Msquare]
  start -> t
  t -> exit [condition="outcome=success"]
}`), 0o644)
	return path
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("file not written within %s: %s", timeout, path)
}

func readPIDFile(t *testing.T, pidPath string) int {
	t.Helper()
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatalf("read %s: %v", pidPath, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("run.pid should contain a positive integer pid, got %q (err=%v)", strings.TrimSpace(string(raw)), err)
	}
	return pid
}

func waitForProcessExit(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	procPath := filepath.Join("/proc", strconv.Itoa(pid))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(procPath); os.IsNotExist(err) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("detached process %d still running after %s", pid, timeout)
}
