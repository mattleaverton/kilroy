// Tests for kilroy auth check (A8d).
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

// authTOMLAllOK returns a minimal auth.toml with one fully-resolvable binding.
// The caller must set KILROY_TEST_CHECK_KEY=<anything> in the subprocess env
// for the chain to resolve.
func authTOMLAllOK() string {
	return `
[bindings]
"anthropic/api_key" = "test_anthropic"

[chains.test_anthropic]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "KILROY_TEST_CHECK_KEY" },
]
`
}

// authTOMLOneExhausted returns a minimal auth.toml with two bindings: one
// resolvable (set KILROY_TEST_CHECK_PRESENT) and one always exhausted
// (KILROY_TEST_CHECK_ABSENT is never set by tests).
func authTOMLOneExhausted() string {
	return `
[bindings]
"anthropic/api_key" = "test_anthropic"
"openai/api_key"    = "test_openai"

[chains.test_anthropic]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "KILROY_TEST_CHECK_PRESENT" },
]

[chains.test_openai]
requires = { provider = "openai", method = "api_key" }
sources = [
  { kind = "env_var", name = "KILROY_TEST_CHECK_ABSENT_XXXXXXXX" },
]
`
}

// envWithout returns os.Environ() with the specified keys removed, then
// appends the provided key=value pairs. This ensures each key appears exactly
// once with the desired value in the subprocess environment.
func envWithout(exclude []string, add ...string) []string {
	excl := make(map[string]bool, len(exclude)+len(add))
	for _, k := range exclude {
		excl[k] = true
	}
	for _, kv := range add {
		if idx := strings.IndexByte(kv, '='); idx >= 0 {
			excl[kv[:idx]] = true
		}
	}
	env := make([]string, 0, len(os.Environ())+len(add))
	for _, e := range os.Environ() {
		key := e
		if idx := strings.IndexByte(e, '='); idx >= 0 {
			key = e[:idx]
		}
		if !excl[key] {
			env = append(env, e)
		}
	}
	return append(env, add...)
}

// writeAuthConfig creates <dir>/.config/kilroy/auth.toml with the given
// content and returns the config dir path.
func writeAuthConfig(t *testing.T, dir, content string) {
	t.Helper()
	cfgDir := filepath.Join(dir, ".config", "kilroy")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "auth.toml"), []byte(content), 0o644); err != nil {
		t.Fatalf("write auth.toml: %v", err)
	}
}

// TestAuthCheck_AllOK verifies that auth check exits 0 and reports OK for
// every binding when all sources are present.
func TestAuthCheck_AllOK(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLAllOK())

	cmd := exec.Command(bin, "auth", "check", "--pretty")
	cmd.Env = envWithout(
		[]string{"HOME", "KILROY_TEST_CHECK_KEY"},
		"HOME="+tmpHome,
		"KILROY_TEST_CHECK_KEY=test-value-present",
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("expected exit 0, got error: %v\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "OK") {
		t.Errorf("expected 'OK' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "anthropic/api_key") {
		t.Errorf("expected binding key in output, got:\n%s", out)
	}
	if strings.Contains(out, "ERR") {
		t.Errorf("unexpected ERR in output:\n%s", out)
	}
}

// TestAuthCheck_ChainExhausted verifies that auth check exits 1, reports OK
// for the resolvable binding, and ERR+ErrChainExhausted for the exhausted one.
func TestAuthCheck_ChainExhausted(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLOneExhausted())

	cmd := exec.Command(bin, "auth", "check", "--pretty")
	cmd.Env = envWithout(
		[]string{"HOME", "KILROY_TEST_CHECK_PRESENT", "KILROY_TEST_CHECK_ABSENT_XXXXXXXX"},
		"HOME="+tmpHome,
		"KILROY_TEST_CHECK_PRESENT=present-value",
		// KILROY_TEST_CHECK_ABSENT_XXXXXXXX intentionally not set
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit code when a chain is exhausted")
	}
	out := stdout.String()
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK for the resolvable binding, got:\n%s", out)
	}
	if !strings.Contains(out, "ERR") {
		t.Errorf("expected ERR for the exhausted binding, got:\n%s", out)
	}
	// ErrChainExhausted renders as "chain <name> has no usable source: ..."
	if !strings.Contains(out, "no usable source") {
		t.Errorf("expected ErrChainExhausted message ('no usable source') in output, got:\n%s", out)
	}
}

// TestAuthCheck_NoConfig_ExitsOne verifies that auth check exits 1 and
// mentions "auth init" when no auth config exists.
func TestAuthCheck_NoConfig_ExitsOne(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir() // empty — no .config/kilroy/auth.toml
	tmpProject := t.TempDir()

	cmd := exec.Command(bin, "auth", "check", "--project", tmpProject)
	cmd.Env = envWithout([]string{"HOME"}, "HOME="+tmpHome)

	var stderr strings.Builder
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1 when auth config is absent")
	}
	errOut := stderr.String()
	if !strings.Contains(errOut, "auth init") {
		t.Errorf("expected 'auth init' remediation in stderr, got:\n%s", errOut)
	}
}

// TestAuthCheck_JSON verifies that --json produces parseable output with
// the expected shape when all checks pass.
func TestAuthCheck_JSON(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir()
	writeAuthConfig(t, tmpHome, authTOMLAllOK())

	cmd := exec.Command(bin, "auth", "check", "--json")
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
		Checks []struct {
			BindingKey string `json:"binding_key"`
			Status     string `json:"status"`
		} `json:"checks"`
		Summary struct {
			OK    int `json:"ok"`
			Error int `json:"error"`
			Total int `json:"total"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, out)
	}
	if result.Summary.OK != 1 || result.Summary.Error != 0 || result.Summary.Total != 1 {
		t.Errorf("summary = %+v, want ok=1 error=0 total=1", result.Summary)
	}
	if len(result.Checks) != 1 {
		t.Fatalf("checks len = %d, want 1", len(result.Checks))
	}
	if result.Checks[0].Status != "ok" {
		t.Errorf("check status = %q, want ok", result.Checks[0].Status)
	}
	if result.Checks[0].BindingKey != "anthropic/api_key" {
		t.Errorf("binding_key = %q, want anthropic/api_key", result.Checks[0].BindingKey)
	}
}
