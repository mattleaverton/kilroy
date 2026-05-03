// Tests for the auth config loader (A3). Covers user-only, project-only,
// merged, overlapping, missing, malformed, empty, Chain.Name population,
// and default-path resolution.

package binding

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestFile creates parent directories and writes content to path.
func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// Sample TOML fixtures used across multiple tests.
const userAuthTOML = `
[bindings]
"anthropic/api_key" = "anthropic_kilroy_api"

[chains.anthropic_kilroy_api]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "ANTHROPIC_API_KEY_KILROY" },
]
`

const projectAuthTOML = `
[bindings]
"openai/api_key" = "openai_kilroy_api"

[chains.openai_kilroy_api]
requires = { provider = "openai", method = "api_key" }
sources = [
  { kind = "env_var", name = "OPENAI_API_KEY_KILROY" },
]
`

// Test 1: user file only — project path skipped (empty string).
func TestLoadConfigFromPaths_UserOnly(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "auth.toml")
	writeTestFile(t, userPath, userAuthTOML)

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Bindings["anthropic/api_key"]; got != "anthropic_kilroy_api" {
		t.Errorf("bindings[anthropic/api_key] = %q, want anthropic_kilroy_api", got)
	}
	if _, ok := cfg.Chains["anthropic_kilroy_api"]; !ok {
		t.Errorf("chain anthropic_kilroy_api missing from %v", cfg.Chains)
	}
}

// Test 2: project file only — user path skipped.
func TestLoadConfigFromPaths_ProjectOnly(t *testing.T) {
	dir := t.TempDir()
	projectPath := filepath.Join(dir, "auth.toml")
	writeTestFile(t, projectPath, projectAuthTOML)

	cfg, err := LoadConfigFromPaths("", projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Bindings["openai/api_key"]; got != "openai_kilroy_api" {
		t.Errorf("bindings[openai/api_key] = %q, want openai_kilroy_api", got)
	}
	if _, ok := cfg.Chains["openai_kilroy_api"]; !ok {
		t.Errorf("chain openai_kilroy_api missing from %v", cfg.Chains)
	}
}

// Test 3: both present, no overlapping keys — all entries appear.
func TestLoadConfigFromPaths_BothNoOverlap(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userAuthTOML)
	writeTestFile(t, projectPath, projectAuthTOML)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Bindings["anthropic/api_key"] != "anthropic_kilroy_api" {
		t.Errorf("user binding missing: %v", cfg.Bindings)
	}
	if cfg.Bindings["openai/api_key"] != "openai_kilroy_api" {
		t.Errorf("project binding missing: %v", cfg.Bindings)
	}
	if _, ok := cfg.Chains["anthropic_kilroy_api"]; !ok {
		t.Errorf("user chain missing: %v", cfg.Chains)
	}
	if _, ok := cfg.Chains["openai_kilroy_api"]; !ok {
		t.Errorf("project chain missing: %v", cfg.Chains)
	}
}

// Test 4: overlapping chain name — project version replaces user version wholesale.
func TestLoadConfigFromPaths_OverlappingChain_ProjectWins(t *testing.T) {
	dir := t.TempDir()

	userContent := `
[chains.shared_chain]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "USER_KEY" },
]
`
	projectContent := `
[chains.shared_chain]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "PROJECT_KEY" },
]
`
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	chain, ok := cfg.Chains["shared_chain"]
	if !ok {
		t.Fatal("shared_chain missing from merged config")
	}
	if len(chain.Sources) == 0 {
		t.Fatal("shared_chain has no sources after merge")
	}
	if chain.Sources[0].Name != "PROJECT_KEY" {
		t.Errorf("project sources should win; got Name = %q, want PROJECT_KEY", chain.Sources[0].Name)
	}
	// There should be exactly one source — not a merged list.
	if len(chain.Sources) != 1 {
		t.Errorf("source list should not be merged across layers; got %d sources", len(chain.Sources))
	}
}

// Test 5: overlapping binding key — project value replaces user value.
func TestLoadConfigFromPaths_OverlappingBinding_ProjectWins(t *testing.T) {
	dir := t.TempDir()

	userContent := `
[bindings]
"anthropic/api_key" = "user_chain"
`
	projectContent := `
[bindings]
"anthropic/api_key" = "project_chain"
`
	userPath := filepath.Join(dir, "user.toml")
	projectPath := filepath.Join(dir, "project.toml")
	writeTestFile(t, userPath, userContent)
	writeTestFile(t, projectPath, projectContent)

	cfg, err := LoadConfigFromPaths(userPath, projectPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Bindings["anthropic/api_key"]; got != "project_chain" {
		t.Errorf("bindings[anthropic/api_key] = %q, want project_chain (project wins)", got)
	}
}

// Test 6: neither file present — LoadConfig returns *ErrNoConfig with both paths.
func TestLoadConfig_NeitherPresent(t *testing.T) {
	xdgDir := t.TempDir() // valid dir, but no auth.toml inside
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	projectRoot := t.TempDir() // valid dir, but no .kilroy/auth.toml inside

	_, err := LoadConfig(projectRoot)
	if err == nil {
		t.Fatal("expected *ErrNoConfig, got nil")
	}

	var noConf *ErrNoConfig
	if !errors.As(err, &noConf) {
		t.Fatalf("expected *ErrNoConfig, got %T: %v", err, err)
	}

	wantUser := filepath.Join(xdgDir, "kilroy", "auth.toml")
	wantProject := filepath.Join(projectRoot, ".kilroy", "auth.toml")

	if noConf.UserPath != wantUser {
		t.Errorf("UserPath = %q, want %q", noConf.UserPath, wantUser)
	}
	if noConf.ProjectPath != wantProject {
		t.Errorf("ProjectPath = %q, want %q", noConf.ProjectPath, wantProject)
	}
}

// Test 6b: LoadConfigFromPaths("","") also returns *ErrNoConfig.
func TestLoadConfigFromPaths_BothEmpty(t *testing.T) {
	_, err := LoadConfigFromPaths("", "")
	var noConf *ErrNoConfig
	if !errors.As(err, &noConf) {
		t.Fatalf("expected *ErrNoConfig, got %T: %v", err, err)
	}
}

// Test 7: malformed user file — error wraps the file path.
func TestLoadConfigFromPaths_MalformedFile(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "auth.toml")
	writeTestFile(t, userPath, "key = @@@invalid_value\n")

	_, err := LoadConfigFromPaths(userPath, "")
	if err == nil {
		t.Fatal("expected error for malformed TOML, got nil")
	}
	if !strings.Contains(err.Error(), userPath) {
		t.Errorf("error should mention the file path %q; got: %v", userPath, err)
	}
}

// Test 8: empty file — yields empty Config without error.
func TestLoadConfigFromPaths_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "auth.toml")
	writeTestFile(t, userPath, "")

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error for empty file: %v", err)
	}
	if len(cfg.Bindings) != 0 {
		t.Errorf("expected empty Bindings, got %v", cfg.Bindings)
	}
	if len(cfg.Chains) != 0 {
		t.Errorf("expected empty Chains, got %v", cfg.Chains)
	}
}

// Test 9: Chain.Name is populated from the map key after loading.
func TestLoadConfigFromPaths_ChainNamePopulated(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, "auth.toml")
	writeTestFile(t, userPath, userAuthTOML)

	cfg, err := LoadConfigFromPaths(userPath, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for mapKey, chain := range cfg.Chains {
		if chain.Name == "" {
			t.Errorf("chain %q has empty Name field after loading", mapKey)
		}
		if chain.Name != mapKey {
			t.Errorf("chain.Name = %q, want %q (map key)", chain.Name, mapKey)
		}
	}
}

// Test 10: default user path respects XDG_CONFIG_HOME vs HOME.
func TestDefaultUserConfigPath(t *testing.T) {
	t.Run("XDG_CONFIG_HOME_set", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/custom/xdg")
		got := defaultUserConfigPath()
		want := "/custom/xdg/kilroy/auth.toml"
		if got != want {
			t.Errorf("defaultUserConfigPath() = %q, want %q", got, want)
		}
	})

	t.Run("XDG_CONFIG_HOME_empty_falls_back_to_HOME", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		home := os.Getenv("HOME")
		got := defaultUserConfigPath()
		want := filepath.Join(home, ".config", "kilroy", "auth.toml")
		if got != want {
			t.Errorf("defaultUserConfigPath() = %q, want %q", got, want)
		}
	})
}
