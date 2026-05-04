// Acceptance tests asserting deleted CLI flags stay deleted.
// Per the v2 architecture closure (commits eb0c5b4, 31cbbd9, 8b170ec,
// 0c1ab81): --tmux, --force-model, --validate, --prompt-file, and
// inline JSON in --input are gone from kilroy run.
package main

import (
	"strings"
	"testing"
)

// --tmux must not appear in `kilroy run --help` and must be rejected
// at parse time. Dispatch is by resolved driver, never by a flag.
func TestRunSurface_NoTmuxFlag(t *testing.T) {
	bin := buildKilroyBinary(t)

	code, out := runKilroy(t, bin, "run", "--help")
	if code != 0 {
		t.Fatalf("kilroy run --help: exit %d\n%s", code, out)
	}
	if strings.Contains(out, "--tmux") {
		t.Fatalf("kilroy run --help should not mention --tmux:\n%s", out)
	}

	code, out = runKilroy(t, bin, "run", "--tmux", "anything")
	if code == 0 {
		t.Fatalf("kilroy run --tmux: should fail; got exit 0\n%s", out)
	}
	if !strings.Contains(out, "--tmux") && !strings.Contains(out, "unknown") {
		t.Fatalf("kilroy run --tmux: expected unknown-arg error mentioning --tmux:\n%s", out)
	}
}

// --force-model must not appear in `kilroy run --help` and must be
// rejected at parse time.
func TestRunSurface_NoForceModelFlag(t *testing.T) {
	bin := buildKilroyBinary(t)

	code, out := runKilroy(t, bin, "run", "--help")
	if code != 0 {
		t.Fatalf("kilroy run --help: exit %d\n%s", code, out)
	}
	if strings.Contains(out, "--force-model") {
		t.Fatalf("kilroy run --help should not mention --force-model:\n%s", out)
	}

	code, out = runKilroy(t, bin, "run", "--force-model", "openai=gpt-5.4")
	if code == 0 {
		t.Fatalf("kilroy run --force-model: should fail; got exit 0\n%s", out)
	}
}

// --validate must not appear in `kilroy run --help` and must be rejected
// at parse time. Validation is now `kilroy workflows validate <name>`.
func TestRunSurface_NoValidateFlag(t *testing.T) {
	bin := buildKilroyBinary(t)

	code, out := runKilroy(t, bin, "run", "--help")
	if code != 0 {
		t.Fatalf("kilroy run --help: exit %d\n%s", code, out)
	}
	if strings.Contains(out, "--validate") {
		t.Fatalf("kilroy run --help should not mention --validate:\n%s", out)
	}

	code, out = runKilroy(t, bin, "run", "--validate", "anything")
	if code == 0 {
		t.Fatalf("kilroy run --validate: should fail; got exit 0\n%s", out)
	}
}

// --prompt-file must not appear in `kilroy run --help` and must be
// rejected at parse time. --input-file KEY=PATH supersedes it.
func TestRunSurface_NoPromptFileFlag(t *testing.T) {
	bin := buildKilroyBinary(t)

	code, out := runKilroy(t, bin, "run", "--help")
	if code != 0 {
		t.Fatalf("kilroy run --help: exit %d\n%s", code, out)
	}
	if strings.Contains(out, "--prompt-file") {
		t.Fatalf("kilroy run --help should not mention --prompt-file:\n%s", out)
	}

	code, out = runKilroy(t, bin, "run", "--prompt-file", "anything")
	if code == 0 {
		t.Fatalf("kilroy run --prompt-file: should fail; got exit 0\n%s", out)
	}
}

// --input no longer accepts inline JSON. Passing `--input '{...}'` must
// fail with a clear error rather than silently parsing the literal.
// Use direct mode (--graph) so workflow resolution doesn't fail first.
func TestRunSurface_InputRejectsInlineJSON(t *testing.T) {
	bin := buildKilroyBinary(t)

	// Direct mode: leading flag goes to attractorRun, which is where the
	// inline-JSON check fires.
	code, out := runKilroy(t, bin, "run", "--input", `{"prompt":"hi"}`, "--graph", "/dev/null")
	if code == 0 {
		t.Fatalf("kilroy run --input '{json}': should fail; got exit 0\n%s", out)
	}
	if !strings.Contains(out, "inline JSON") && !strings.Contains(out, "file path") {
		t.Fatalf("expected error message about inline JSON / file path; got:\n%s", out)
	}
}

// --input-file is the canonical injection path; a missing argument
// errors cleanly.
func TestRunSurface_InputFileRequiresKEYEqualsPATH(t *testing.T) {
	bin := buildKilroyBinary(t)

	code, _ := runKilroy(t, bin, "run", "--input-file")
	if code == 0 {
		t.Fatalf("kilroy run --input-file (no value): should fail")
	}
}
