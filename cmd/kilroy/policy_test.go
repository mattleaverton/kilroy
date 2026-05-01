package main

import (
	"encoding/json"
	"os"
	"os/exec"
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
