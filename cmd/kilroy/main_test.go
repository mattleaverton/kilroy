package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestVersion_JSONOutput verifies that version --json returns valid JSON with a version field.
func TestVersion_JSONOutput(t *testing.T) {
	bin := buildKilroyBinary(t)

	tests := []struct {
		name string
		args []string
	}{
		{"version --json", []string{"version", "--json"}},
		{"--version --json", []string{"--version", "--json"}},
		{"-v --json", []string{"-v", "--json"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runKilroy(t, bin, tt.args...)
			if code != 0 {
				t.Fatalf("exit code: got %d want 0\n%s", code, out)
			}

			// Verify it's valid JSON
			var result map[string]string
			if err := json.Unmarshal([]byte(out), &result); err != nil {
				t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
			}

			// Verify version field exists and is non-empty
			version, ok := result["version"]
			if !ok {
				t.Fatalf("JSON output missing 'version' field: %s", out)
			}
			if version == "" {
				t.Fatalf("JSON output 'version' field is empty: %s", out)
			}
		})
	}
}

// TestVersion_TextOutput verifies that version without --json still outputs plain text.
func TestVersion_TextOutput(t *testing.T) {
	bin := buildKilroyBinary(t)

	tests := []struct {
		name string
		args []string
	}{
		{"version", []string{"version"}},
		{"--version", []string{"--version"}},
		{"-v", []string{"-v"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runKilroy(t, bin, tt.args...)
			if code != 0 {
				t.Fatalf("exit code: got %d want 0\n%s", code, out)
			}

			// Verify plain text output starts with expected prefix
			if !strings.HasPrefix(out, "kilroy ") {
				t.Fatalf("expected output to start with 'kilroy ', got: %s", out)
			}
		})
	}
}
