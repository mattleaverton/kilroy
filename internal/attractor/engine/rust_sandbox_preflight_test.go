package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/attractor/model"
	"github.com/danshapiro/kilroy/internal/attractor/runtime"
)

func TestMaybeRunRustSandboxPreflight_RetrysOnCargoMetadataRegistryFailure(t *testing.T) {
	requireIntegration(t)

	worktree := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "stage")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		t.Fatalf("mkdir stageDir: %v", err)
	}

	crateDir := filepath.Join(worktree, "demo", "rogue", "rogue-wasm")
	if err := os.MkdirAll(crateDir, 0o755); err != nil {
		t.Fatalf("mkdir crateDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(crateDir, "Cargo.toml"), []byte("[package]\nname = \"x\"\nversion = \"0.1.0\"\n"), 0o644); err != nil {
		t.Fatalf("write Cargo.toml: %v", err)
	}

	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir binDir: %v", err)
	}
	cargoPath := filepath.Join(binDir, "cargo")
	script := `#!/usr/bin/env bash
if [ "${1:-}" = "metadata" ]; then
  echo "error: failed to download from https://index.crates.io/config.json" >&2
  echo "Caused by: Could not resolve host: index.crates.io" >&2
  exit 101
fi
echo "unexpected args: $*" >&2
exit 2
`
	if err := os.WriteFile(cargoPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cargo: %v", err)
	}
	t.Setenv("PATH", binDir+string(filepath.ListSeparator)+os.Getenv("PATH"))

	node := model.NewNode("verify_bridge")
	node.Attrs["prompt"] = "Build wasm in `demo/rogue/rogue-wasm` and verify Cargo artifacts."

	meta, out := maybeRunRustSandboxPreflight(context.Background(), node, worktree, stageDir, os.Environ())
	if meta == nil {
		t.Fatal("expected rust preflight metadata")
	}
	if strings.TrimSpace(anyToString(meta["status"])) != "fail" {
		t.Fatalf("preflight status: got %q want fail", anyToString(meta["status"]))
	}
	if out == nil {
		t.Fatal("expected retry outcome on registry failure")
	}
	if out.Status != runtime.StatusRetry {
		t.Fatalf("status: got %q want %q", out.Status, runtime.StatusRetry)
	}
	if got := anyToString(out.ContextUpdates["failure_class"]); got != failureClassTransientInfra {
		t.Fatalf("failure_class: got %q want %q", got, failureClassTransientInfra)
	}
	if !strings.Contains(strings.ToLower(out.FailureReason), "registry") {
		t.Fatalf("failure_reason should mention registry unavailability, got %q", out.FailureReason)
	}
	if _, err := os.Stat(filepath.Join(stageDir, "rust_sandbox_preflight.json")); err != nil {
		t.Fatalf("expected rust_sandbox_preflight.json artifact: %v", err)
	}
}

func TestMaybeRunRustSandboxPreflight_SkipsWithoutManifest(t *testing.T) {
	worktree := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "stage")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		t.Fatalf("mkdir stageDir: %v", err)
	}

	node := model.NewNode("verify_bridge")
	node.Attrs["prompt"] = "Run rust verification for `demo/rogue/rogue-wasm`."

	meta, out := maybeRunRustSandboxPreflight(context.Background(), node, worktree, stageDir, os.Environ())
	if out != nil {
		t.Fatalf("expected nil outcome when no manifest exists, got %v", out)
	}
	if meta == nil {
		t.Fatal("expected metadata for skipped rust preflight")
	}
	if got := strings.TrimSpace(anyToString(meta["status"])); got != "skipped" {
		t.Fatalf("status: got %q want skipped", got)
	}
	if got := strings.TrimSpace(anyToString(meta["reason"])); got != "no_manifest" {
		t.Fatalf("reason: got %q want no_manifest", got)
	}
}
