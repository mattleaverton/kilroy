package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildTestBinary builds the kilroy binary into a temp file and returns its path.
// If the binary cannot be built, the test is skipped with a descriptive message.
func buildTestBinary(t *testing.T) string {
	t.Helper()
	bin, err := os.CreateTemp("", "kilroy-test-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	bin.Close()
	binPath := bin.Name()
	t.Cleanup(func() { os.Remove(binPath) })

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build kilroy binary: %v\n%s", err, out)
	}
	return binPath
}

func TestPolicyList_HumanOutput_IncludesEveryClass(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "policy", "list")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kilroy policy list failed: %v\nstdout: %s", err, out)
	}
	if !strings.Contains(string(out), "hard_coding") {
		t.Errorf("expected output to contain %q, got:\n%s", "hard_coding", out)
	}
}

func TestPolicyShow_KnownClass(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "policy", "show", "hard_coding")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kilroy policy show hard_coding failed: %v\nstdout: %s", err, out)
	}
	if !strings.Contains(string(out), "claude-opus-4-7") {
		t.Errorf("expected output to contain %q, got:\n%s", "claude-opus-4-7", out)
	}
}

func TestPolicyShow_UnknownClass_Exit1(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "policy", "show", "definitely_not_a_class")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit 1, got exit 0")
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() != 1 {
			t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
		}
	}
	if !strings.Contains(stderr.String(), "available classes") {
		t.Errorf("expected stderr to contain %q, got:\n%s", "available classes", stderr.String())
	}
}

// projectAuthTOMLForResolveProbe overrides the bindings for both anthropic
// auth methods so the test is deterministic regardless of what CLI sessions
// happen to be logged in on the host:
//   - cli_oauth/claude is routed to a chain whose tool is unreachable, forcing
//     rank 0 of hard_coding to skip.
//   - api_key is routed to my_test_chain referencing MY_TEST_KEY_PROBE_F3,
//     which the test sets so rank 1 (anthropic_sdk Opus) resolves.
func projectAuthTOMLForResolveProbe() string {
	return `
[bindings]
"anthropic/cli_oauth/claude" = "test_unreachable_cli"
"anthropic/api_key"          = "my_test_chain"

[chains.test_unreachable_cli]
requires = { provider = "anthropic", method = "cli_oauth", tool = "claude" }
sources = [
  { kind = "cli_session", tool = "definitely_not_a_real_cli_tool_xxx" },
]

[chains.my_test_chain]
requires = { provider = "anthropic", method = "api_key" }
sources = [
  { kind = "env_var", name = "MY_TEST_KEY_PROBE_F3" },
]
`
}

// writeProjectAuth creates <projectRoot>/.kilroy/auth.toml with the given content.
func writeProjectAuth(t *testing.T, projectRoot, content string) {
	t.Helper()
	dir := filepath.Join(projectRoot, ".kilroy")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir .kilroy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "auth.toml"), []byte(content), 0o644); err != nil {
		t.Fatalf("write auth.toml: %v", err)
	}
}

// resolveProbeEnv returns a clean env that strips host credentials so the
// project-layer override is the only chain that can resolve.
func resolveProbeEnv(tmpHome string, extra ...string) []string {
	stripped := []string{
		"HOME",
		"XDG_CONFIG_HOME",
		"KILROY_PROJECT_ROOT",
		"ANTHROPIC_API_KEY",
		"ANTHROPIC_API_KEY_KILROY",
		"OPENAI_API_KEY",
		"OPENAI_API_KEY_KILROY",
		"GOOGLE_API_KEY",
		"GEMINI_API_KEY",
		"MY_TEST_KEY_PROBE_F3",
	}
	add := []string{"HOME=" + tmpHome}
	add = append(add, extra...)
	return envWithout(stripped, add...)
}

// TestPolicyResolve_ProjectLayerOverride_ExplicitFlag verifies that
// `kilroy policy resolve --project <dir>` honors a project-layer auth.toml.
func TestPolicyResolve_ProjectLayerOverride_ExplicitFlag(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir() // no user auth.toml
	projectRoot := t.TempDir()
	writeProjectAuth(t, projectRoot, projectAuthTOMLForResolveProbe())

	cmd := exec.Command(bin, "policy", "resolve", "hard_coding",
		"--json", "--project", projectRoot)
	cmd.Env = resolveProbeEnv(tmpHome, "MY_TEST_KEY_PROBE_F3=present-value")

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("expected exit 0, got %v\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}

	var out struct {
		Resolved struct {
			ModelID      string `json:"model_id"`
			Driver       string `json:"driver"`
			AuthMethod   string `json:"auth_method"`
			AuthSource   string `json:"auth_source"`
			FallbackRank int    `json:"fallback_rank"`
		} `json:"resolved"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(stdout.String()), &out); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, stdout.String())
	}
	if out.Error != "" {
		t.Fatalf("resolve error: %s", out.Error)
	}
	if out.Resolved.AuthMethod != "api_key" {
		t.Errorf("auth_method = %q, want api_key", out.Resolved.AuthMethod)
	}
	if out.Resolved.AuthSource != "MY_TEST_KEY_PROBE_F3" {
		t.Errorf("auth_source = %q, want MY_TEST_KEY_PROBE_F3 (project-layer chain not honored)",
			out.Resolved.AuthSource)
	}
	if out.Resolved.FallbackRank != 1 {
		t.Errorf("fallback_rank = %d, want 1 (rank 0 should have been skipped)", out.Resolved.FallbackRank)
	}
}

// TestPolicyResolve_ProjectLayerOverride_CwdDiscovery verifies that
// `kilroy policy resolve` (no --project flag) walks upward from cwd to find
// the project root, the same way `kilroy run` does.
func TestPolicyResolve_ProjectLayerOverride_CwdDiscovery(t *testing.T) {
	bin := buildTestBinary(t)
	tmpHome := t.TempDir() // no user auth.toml
	projectRoot := t.TempDir()
	writeProjectAuth(t, projectRoot, projectAuthTOMLForResolveProbe())

	// Run from a subdirectory of the project root to exercise upward walk.
	subDir := filepath.Join(projectRoot, "sub", "deep")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}

	cmd := exec.Command(bin, "policy", "resolve", "hard_coding", "--json")
	cmd.Dir = subDir
	cmd.Env = resolveProbeEnv(tmpHome, "MY_TEST_KEY_PROBE_F3=present-value")

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("expected exit 0, got %v\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}

	var out struct {
		Resolved struct {
			AuthSource string `json:"auth_source"`
		} `json:"resolved"`
	}
	if err := json.Unmarshal([]byte(stdout.String()), &out); err != nil {
		t.Fatalf("parse JSON: %v\nraw: %s", err, stdout.String())
	}
	if out.Resolved.AuthSource != "MY_TEST_KEY_PROBE_F3" {
		t.Errorf("auth_source = %q, want MY_TEST_KEY_PROBE_F3 (cwd discovery did not find .kilroy/)",
			out.Resolved.AuthSource)
	}
}

func TestPolicyList_JSONOutput_ParsesAsJSON(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "policy", "list", "--json")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kilroy policy list --json failed: %v\nstdout: %s", err, out)
	}
	var v policyListJSON
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if _, ok := v.Classes["hard_coding"]; !ok {
		t.Errorf("expected JSON to contain class %q, classes: %v", "hard_coding", v.Classes)
	}
}
