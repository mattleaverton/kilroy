package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

func authManageEnv(tmpHome string, extra ...string) []string {
	add := []string{
		"HOME=" + tmpHome,
		"XDG_CONFIG_HOME=" + filepath.Join(tmpHome, ".config"),
	}
	add = append(add, extra...)
	return envWithout([]string{
		"HOME",
		"XDG_CONFIG_HOME",
		"OPENAI_API_KEY",
		"OPENAI_API_KEY_KILROY",
		"ANTHROPIC_API_KEY",
		"ANTHROPIC_API_KEY_KILROY",
	}, add...)
}

func readUserAuthConfig(t *testing.T, tmpHome string) binding.Config {
	t.Helper()
	return parseAuthTOML(t, filepath.Join(tmpHome, ".config", "kilroy", "auth.toml"))
}

func TestAuthSet_CreatesGlobalAPIKeySource(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()

	cmd := exec.Command(bin, "auth", "set", "openai", "--env", "OPENAI_API_KEY_KILROY")
	cmd.Env = authManageEnv(tmpHome)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("auth set failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	cfg := readUserAuthConfig(t, tmpHome)
	chainName := cfg.Bindings["openai/api_key"]
	if chainName == "" {
		t.Fatalf("openai/api_key binding missing: %+v", cfg.Bindings)
	}
	chain := cfg.Chains[chainName]
	if len(chain.Sources) == 0 || chain.Sources[0].Name != "OPENAI_API_KEY_KILROY" {
		t.Fatalf("first openai source = %+v, want env OPENAI_API_KEY_KILROY", chain.Sources)
	}
	if !strings.Contains(stdout.String(), "global auth") {
		t.Fatalf("stdout should say it wrote global auth; got %q", stdout.String())
	}
}

func TestAuthPrefer_MovesEnvSourceToFront(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, `
[bindings]
"openai/api_key" = "openai_api_key"

[chains.openai_api_key]
requires = { provider = "openai", method = "api_key" }
sources = [
  { kind = "env_var", name = "OPENAI_API_KEY" },
  { kind = "env_var", name = "OPENAI_API_KEY_KILROY" },
]
`)

	cmd := exec.Command(bin, "auth", "prefer", "openai/api_key", "OPENAI_API_KEY_KILROY")
	cmd.Env = authManageEnv(tmpHome)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if out, err := cmd.Output(); err != nil {
		t.Fatalf("auth prefer failed: %v\nstdout: %s\nstderr: %s", err, out, stderr.String())
	}

	cfg := readUserAuthConfig(t, tmpHome)
	chain := cfg.Chains[cfg.Bindings["openai/api_key"]]
	if len(chain.Sources) < 2 {
		t.Fatalf("sources = %+v, want two sources", chain.Sources)
	}
	if chain.Sources[0].Name != "OPENAI_API_KEY_KILROY" {
		t.Fatalf("first source = %q, want OPENAI_API_KEY_KILROY", chain.Sources[0].Name)
	}
}

func TestAuthRemoveSource_RemovesEnvSource(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, `
[bindings]
"openai/api_key" = "openai_api_key"

[chains.openai_api_key]
requires = { provider = "openai", method = "api_key" }
sources = [
  { kind = "env_var", name = "OPENAI_API_KEY_KILROY" },
  { kind = "env_var", name = "OPENAI_API_KEY" },
]
`)

	cmd := exec.Command(bin, "auth", "remove-source", "openai/api_key", "OPENAI_API_KEY")
	cmd.Env = authManageEnv(tmpHome)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if out, err := cmd.Output(); err != nil {
		t.Fatalf("auth remove-source failed: %v\nstdout: %s\nstderr: %s", err, out, stderr.String())
	}

	cfg := readUserAuthConfig(t, tmpHome)
	chain := cfg.Chains[cfg.Bindings["openai/api_key"]]
	for _, src := range chain.Sources {
		if src.Name == "OPENAI_API_KEY" {
			t.Fatalf("OPENAI_API_KEY should have been removed; sources: %+v", chain.Sources)
		}
	}
}

func TestAuthInitRescan_AddsNewDetectedSource(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, `
[bindings]
"openai/api_key" = "openai_api_key"

[chains.openai_api_key]
requires = { provider = "openai", method = "api_key" }
sources = []
`)

	cmd := exec.Command(bin, "auth", "init", "--rescan", "--json")
	cmd.Env = authManageEnv(tmpHome, "OPENAI_API_KEY_KILROY=present")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("auth init --rescan failed: %v\nstdout: %s", err, out)
	}
	var summary struct {
		AddedSources []string `json:"added_sources"`
	}
	if err := json.Unmarshal(out, &summary); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	if len(summary.AddedSources) == 0 {
		t.Fatalf("expected at least one added source in JSON summary: %s", out)
	}

	cfg := readUserAuthConfig(t, tmpHome)
	chain := cfg.Chains[cfg.Bindings["openai/api_key"]]
	if len(chain.Sources) == 0 || chain.Sources[0].Name != "OPENAI_API_KEY_KILROY" {
		t.Fatalf("sources after rescan = %+v, want OPENAI_API_KEY_KILROY", chain.Sources)
	}
}
