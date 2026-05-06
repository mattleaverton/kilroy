package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// internalFlags are flags that are intentionally absent from user-facing
// help text (e.g. for inter-process communication between kilroy processes).
var internalFlags = map[string]bool{}

// extractFuncBody returns the source text of the named function by tracking
// brace depth from the opening "func funcName(" declaration. Tracks Go
// lexical state (line comments, block comments, string literals,
// rune literals, raw strings) so that braces inside those constructs
// don't confuse the depth counter — `if strings.HasPrefix(s, "{")` was
// silently counted as an extra open and made the body overshoot into
// adjacent functions.
func extractFuncBody(src, funcName string) (string, bool) {
	needle := "func " + funcName + "("
	idx := strings.Index(src, needle)
	if idx == -1 {
		return "", false
	}
	depth := 0
	started := false
	state := codeState
	for i := idx; i < len(src); i++ {
		c := src[i]
		switch state {
		case codeState:
			switch c {
			case '/':
				if i+1 < len(src) {
					switch src[i+1] {
					case '/':
						state = lineCommentState
						i++
						continue
					case '*':
						state = blockCommentState
						i++
						continue
					}
				}
			case '"':
				state = stringState
			case '\'':
				state = runeState
			case '`':
				state = rawStringState
			case '{':
				depth++
				started = true
			case '}':
				depth--
				if started && depth == 0 {
					return src[idx : i+1], true
				}
			}
		case lineCommentState:
			if c == '\n' {
				state = codeState
			}
		case blockCommentState:
			if c == '*' && i+1 < len(src) && src[i+1] == '/' {
				state = codeState
				i++
			}
		case stringState:
			if c == '\\' && i+1 < len(src) {
				i++ // skip escape
				continue
			}
			if c == '"' {
				state = codeState
			}
		case runeState:
			if c == '\\' && i+1 < len(src) {
				i++
				continue
			}
			if c == '\'' {
				state = codeState
			}
		case rawStringState:
			if c == '`' {
				state = codeState
			}
		}
	}
	return src[idx:], true
}

// Lexical states for extractFuncBody.
const (
	codeState = iota
	lineCommentState
	blockCommentState
	stringState
	runeState
	rawStringState
)

// caseFlagLineRe matches a `case "--foo"[, "--bar"]*:` arm.
// It deliberately does NOT match `case someVariable:` so internal flags
// represented by named constants are excluded automatically.
var caseFlagLineRe = regexp.MustCompile(`(?m)^\s+case\s+((?:"--[^"]+",?\s*)+):`)

// quotedFlagRe extracts `--flag` values from a string of quoted tokens.
var quotedFlagRe = regexp.MustCompile(`"(--[^"]+)"`)

// parseCaseFlags returns the sorted set of --flag names found in case arms
// within the given function body.
func parseCaseFlags(body string) []string {
	seen := map[string]bool{}
	for _, m := range caseFlagLineRe.FindAllStringSubmatch(body, -1) {
		for _, fm := range quotedFlagRe.FindAllStringSubmatch(m[1], -1) {
			seen[fm[1]] = true
		}
	}
	var out []string
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// usageLineRe matches fmt.Fprintln lines that emit indented usage text —
// either `"  kilroy ..."` command shapes or `"  --flag ..."` flag-detail
// shapes. The two-space indent is the convention; that's how we
// distinguish help/usage prose from incidental Fprintln calls.
var usageLineRe = regexp.MustCompile(`(?m)fmt\.Fprintln\(os\.Stderr,\s+"  (?:kilroy|--)[^"]*"\)`)

// dashFlagRe extracts --flag-name tokens (including hyphens in the name).
var dashFlagRe = regexp.MustCompile(`--([\w-]+)`)

// parseUsageFlags returns the set of --flag names mentioned in usage Fprintln
// lines within the given function body.
func parseUsageFlags(body string) map[string]bool {
	seen := map[string]bool{}
	for _, line := range usageLineRe.FindAllString(body, -1) {
		for _, m := range dashFlagRe.FindAllStringSubmatch(line, -1) {
			seen["--"+m[1]] = true
		}
	}
	return seen
}

// checkDrift asserts that every --flag handled by parserFunc is also mentioned
// in the usage text emitted by usageFunc. Parser and usage may live in
// different files (parser in main.go's attractorRun, usage in run.go's
// runUsage, etc.) — pass usageFile="" to fall back to file.
func checkDrift(t *testing.T, file, parserFunc, usageFunc string, usageFile ...string) {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	src := string(data)

	parserBody, ok := extractFuncBody(src, parserFunc)
	if !ok {
		t.Fatalf("func %s not found in %s", parserFunc, file)
	}

	usageSrc := src
	usagePath := file
	if len(usageFile) > 0 && usageFile[0] != "" && usageFile[0] != file {
		usagePath = usageFile[0]
		ud, err := os.ReadFile(usagePath)
		if err != nil {
			t.Fatalf("read %s: %v", usagePath, err)
		}
		usageSrc = string(ud)
	}
	usageBody, ok := extractFuncBody(usageSrc, usageFunc)
	if !ok {
		t.Fatalf("func %s not found in %s", usageFunc, usagePath)
	}

	parserFlags := parseCaseFlags(parserBody)
	usageFlags := parseUsageFlags(usageBody)

	for _, flag := range parserFlags {
		if internalFlags[flag] {
			continue
		}
		if !usageFlags[flag] {
			t.Errorf("%s: %s handles %q but %s in %s does not mention it — add it to the help text",
				file, parserFunc, flag, usageFunc, usagePath)
		}
	}
}

// TestHelpUsageDrift ensures that every --flag handled by the parser is also
// documented in the corresponding usage/help function.
//
// When you add a new case "--foo": arm to a parser, you MUST also add --foo to
// the usage function — otherwise this test will fail and remind you.
func TestHelpUsageDrift(t *testing.T) {
	t.Run("attractorRun", func(t *testing.T) {
		// Parser lives in main.go (attractorRun); user-facing flag
		// documentation now lives in run.go's runUsage (the new v2
		// surface), not the top-level usage().
		checkDrift(t, "main.go", "attractorRun", "runUsage", "run.go")
	})
	t.Run("attractorRunsList", func(t *testing.T) {
		checkDrift(t, "attractor_runs.go", "attractorRunsList", "runsUsage")
	})
	t.Run("attractorRunsShow", func(t *testing.T) {
		checkDrift(t, "attractor_runs.go", "attractorRunsShow", "runsUsage")
	})
	t.Run("attractorRunsWait", func(t *testing.T) {
		checkDrift(t, "attractor_runs.go", "attractorRunsWait", "runsUsage")
	})
	t.Run("attractorRunsPrune", func(t *testing.T) {
		checkDrift(t, "attractor_runs.go", "attractorRunsPrune", "runsUsage")
	})
}
