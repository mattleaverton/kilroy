// Tests for kilroy auth list enhancements (A8c):
//   - referenced_by annotation
//   - --chains flag (chain-centric view)
//   - graceful degradation when no config
//
// Uses subprocess tests (buildTestBinary) consistent with the rest of cmd/kilroy.
package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// authTOMLWithAnthropicRef returns an auth.toml that references
// ANTHROPIC_API_KEY via a chain. Used by referenced_by tests.
func authTOMLWithAnthropicRef() string {
	return `
[bindings]
"anthropic/api_key" = "test_list_chain"

[chains.test_list_chain]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "ANTHROPIC_API_KEY" },
]
`
}

// writeAuthConfigAt writes <projectRoot>/.kilroy/auth.toml with the given
// content and returns the project root path.
func writeAuthConfigAt(t *testing.T, projectRoot, content string) {
	t.Helper()
	kilroyDir := filepath.Join(projectRoot, ".kilroy")
	if err := os.MkdirAll(kilroyDir, 0o755); err != nil {
		t.Fatalf("mkdir kilroy dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(kilroyDir, "auth.toml"), []byte(content), 0o644); err != nil {
		t.Fatalf("write project auth.toml: %v", err)
	}
}

// TestAuthList_Chains_Pretty verifies that --chains produces chain-centric
// output with the binding key and chain name for each configured binding.
func TestAuthList_Chains_Pretty(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLAllOK())

	cmd := exec.Command(bin, "auth", "list", "--chains", "--pretty")
	cmd.Env = envWithout(
		[]string{"HOME", "KILROY_TEST_CHECK_KEY"},
		"HOME="+tmpHome,
		"KILROY_TEST_CHECK_KEY=present-value",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\noutput: %s", err, out)
	}
	output := string(out)
	// Should show the binding key and the chain name.
	if !strings.Contains(output, "anthropic/api_key") {
		t.Errorf("expected binding key in --chains output, got:\n%s", output)
	}
	if !strings.Contains(output, "test_anthropic") {
		t.Errorf("expected chain name in --chains output, got:\n%s", output)
	}
	// Resolved source should appear.
	if !strings.Contains(output, "KILROY_TEST_CHECK_KEY") {
		t.Errorf("expected env var name in --chains output, got:\n%s", output)
	}
}

// TestAuthList_Chains_JSON verifies that --chains --json produces parseable
// output mirroring auth check (without exit-1 on error).
func TestAuthList_Chains_JSON(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLAllOK())

	cmd := exec.Command(bin, "auth", "list", "--chains", "--json")
	cmd.Env = envWithout(
		[]string{"HOME", "KILROY_TEST_CHECK_KEY"},
		"HOME="+tmpHome,
		"KILROY_TEST_CHECK_KEY=present",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\noutput: %s", err, out)
	}
	var result struct {
		Chains []struct {
			BindingKey string `json:"binding_key"`
			ChainName  string `json:"chain_name"`
			Status     string `json:"status"`
		} `json:"chains"`
		Summary struct {
			OK    int `json:"ok"`
			Error int `json:"error"`
			Total int `json:"total"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	if len(result.Chains) != 1 {
		t.Fatalf("chains len = %d, want 1", len(result.Chains))
	}
	if result.Chains[0].Status != "ok" {
		t.Errorf("chain status = %q, want ok", result.Chains[0].Status)
	}
	if result.Chains[0].BindingKey != "anthropic/api_key" {
		t.Errorf("binding_key = %q, want anthropic/api_key", result.Chains[0].BindingKey)
	}
	if result.Chains[0].ChainName != "test_anthropic" {
		t.Errorf("chain_name = %q, want test_anthropic", result.Chains[0].ChainName)
	}
}

// TestAuthList_ReferencedBy_JSON verifies that JSON output contains
// referenced_by for entries that are referenced by a configured chain.
//
// We use ANTHROPIC_API_KEY because EnvVarDetector scans for it specifically.
// The config references it explicitly so the entry gets annotated.
func TestAuthList_ReferencedBy_JSON(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLWithAnthropicRef())

	cmd := exec.Command(bin, "auth", "list", "--json")
	cmd.Env = envWithout(
		[]string{"HOME", "ANTHROPIC_API_KEY"},
		"HOME="+tmpHome,
		"ANTHROPIC_API_KEY=sk-ant-testvalue1234",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\noutput: %s", err, out)
	}
	var result struct {
		Entries []struct {
			ID           string   `json:"id"`
			Provider     string   `json:"provider"`
			Kind         string   `json:"kind"`
			ReferencedBy []string `json:"referenced_by"`
			Unreferenced bool     `json:"unreferenced"`
		} `json:"entries"`
		ConfigState string `json:"config_state"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	// Config was loaded, so config_state should not be "uninitialized".
	if result.ConfigState == "uninitialized" {
		t.Errorf("expected config loaded, got config_state=%q", result.ConfigState)
	}

	// Find the ANTHROPIC_API_KEY entry.
	found := false
	for _, e := range result.Entries {
		if e.Provider == "anthropic" && e.Kind == "env_var" {
			found = true
			referenced := false
			for _, ch := range e.ReferencedBy {
				if ch == "test_list_chain" {
					referenced = true
					break
				}
			}
			if !referenced {
				t.Errorf("anthropic env_var entry has referenced_by=%v, want [test_list_chain]",
					e.ReferencedBy)
			}
			if e.Unreferenced {
				t.Errorf("anthropic env_var entry has unreferenced=true but should be referenced")
			}
		}
	}
	if !found {
		t.Errorf("no anthropic env_var entry in output; entries: %+v", result.Entries)
	}
}

// TestAuthList_NoConfig_Degrades verifies that auth list exits 0 with
// config_state="uninitialized" when no auth config exists.
func TestAuthList_NoConfig_Degrades(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir() // empty — no .config/kilroy/auth.toml
	tmpProject := t.TempDir()
	// Ensure project dir exists but has no .kilroy/auth.toml.

	cmd := exec.Command(bin, "auth", "list", "--json")
	cmd.Env = envWithout(
		[]string{"HOME"},
		"HOME="+tmpHome,
		// Pass a project dir that has no auth.toml so findProjectRoot doesn't
		// pick up an auth.toml from somewhere in the test tree.
		// We can't pass --project to auth list, so instead set HOME only.
	)
	// Also ensure the binary runs in a temp dir so findProjectRoot doesn't
	// walk up to the worktree's .kilroy/.
	cmd.Dir = tmpProject

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0 (graceful degradation), got: %v\noutput: %s", err, out)
	}
	var result struct {
		ConfigState string `json:"config_state"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	if result.ConfigState != "uninitialized" {
		t.Errorf("config_state = %q, want %q", result.ConfigState, "uninitialized")
	}
}

// TestAuthList_Pretty_StillWorks verifies that basic auth list --pretty
// continues to work after the enhancement changes.
func TestAuthList_Pretty_StillWorks(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()

	cmd := exec.Command(bin, "auth", "list", "--pretty")
	cmd.Env = envWithout([]string{"HOME"}, "HOME="+tmpHome)

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\noutput: %s", err, out)
	}
	// Should always print a header line.
	if !strings.Contains(string(out), "Kilroy auth scan") {
		t.Errorf("expected scan header in output, got:\n%s", out)
	}
}

// TestAuthList_NoConfig_Pretty_Degrades verifies that --pretty output
// includes the uninitialized hint when no config is present.
func TestAuthList_NoConfig_Pretty_Degrades(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	tmpProject := t.TempDir()

	cmd := exec.Command(bin, "auth", "list", "--pretty")
	cmd.Env = envWithout([]string{"HOME"}, "HOME="+tmpHome)
	cmd.Dir = tmpProject

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "kilroy auth init") {
		t.Errorf("expected 'kilroy auth init' hint in output, got:\n%s", out)
	}
}
