package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestUsingKilroySkillFrontmatterAndRunCommandsStayAgentSafe(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(filepath.Dir(wd))
	path := filepath.Join(root, "skills", "using-kilroy", "SKILL.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read skill: %v", err)
	}
	text := string(raw)

	if !regexp.MustCompile(`(?m)^description:\s+"[^"\n]+"$`).MatchString(text) {
		t.Fatalf("description frontmatter must be quoted YAML so colons in text do not break skill loading")
	}
	for _, forbidden := range []string{
		"kilroy runs show <run-id> --pretty",
		"quick-launch workflow",
		"kilroy run investigate --help",
		"conversation=<slug>",
		"/tmp/kilroy-tasks",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("using-kilroy skill still contains unsafe/stale instruction %q", forbidden)
		}
	}
	for _, required := range []string{
		"session=<same-id-for-the-whole-conversation>",
		"phase=plan|implement|validate",
		"kilroy run plan",
		"kilroy run implement",
		"kilroy run validate",
		"${XDG_STATE_HOME:-$HOME/.local/state}/kilroy/sessions/$SESSION",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("using-kilroy skill missing required three-workflow guidance %q", required)
		}
	}
}
