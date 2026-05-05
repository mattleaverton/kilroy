//go:build ignore

// Command gen_classes_doc generates docs/reference/classes.md from
// internal/policy/data/policy.toml. Run via:
//
//	go generate ./internal/policy/...
//
// Or directly:
//
//	go run ./internal/policy/cmd/gen_classes_doc/main.go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Data mirrors policy.Data from the main package
type Data struct {
	SchemaVersion string           `toml:"schema_version"`
	PolicyVersion string           `toml:"policy_version"`
	Classes       map[string]Class `toml:"classes"`
	Aliases       []ClassAlias     `toml:"aliases"`
}

type Class struct {
	Description string      `toml:"description"`
	Chain       []Candidate `toml:"chain"`
}

type Candidate struct {
	ModelID     string   `toml:"model_id"`
	Driver      string   `toml:"driver"`
	Transport   string   `toml:"transport"`
	HistorySink string   `toml:"history_sink"`
	Tags        []string `toml:"tags"`
	Requires    struct {
		Provider string `toml:"provider"`
		Method   string `toml:"method"`
		Tool     string `toml:"tool,omitempty"`
	} `toml:"requires"`
}

type ClassAlias struct {
	From string `toml:"from"`
	To   string `toml:"to"`
}

func main() {
	// Find repo root (walk up from current file location)
	repoRoot := findRepoRoot()

	policyPath := filepath.Join(repoRoot, "internal", "policy", "data", "policy.toml")
	outputPath := filepath.Join(repoRoot, "docs", "reference", "classes.md")

	// Read and parse policy.toml
	var data Data
	if _, err := toml.DecodeFile(policyPath, &data); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing policy.toml: %v\n", err)
		os.Exit(1)
	}

	// Generate documentation
	var buf strings.Builder
	now := time.Now().UTC().Format("2006-01-02")

	buf.WriteString("# Policy Classes Reference\n\n")
	buf.WriteString(fmt.Sprintf("> Generated from `internal/policy/data/policy.toml` (schema_version=%s, policy_version=%s) on %s\n\n",
		data.SchemaVersion, data.PolicyVersion, now))

	buf.WriteString("This document describes all policy classes available for routing agent nodes. ")
	buf.WriteString("Each class defines a fallback chain of candidates ordered by preference. ")
	buf.WriteString("The resolver walks the chain and picks the first candidate whose authentication requirements are satisfied.\n\n")

	// Sort class names for consistent output
	classNames := make([]string, 0, len(data.Classes))
	for name := range data.Classes {
		classNames = append(classNames, name)
	}
	sort.Strings(classNames)

	// Document each class
	for _, name := range classNames {
		class := data.Classes[name]
		documentClass(&buf, name, class, data.Aliases)
	}

	// Document aliases section
	if len(data.Aliases) > 0 {
		buf.WriteString("---\n\n")
		buf.WriteString("## Class Aliases\n\n")
		buf.WriteString("Aliases provide shorter or alternative names for classes:\n\n")
		buf.WriteString("| Alias | Resolves To |\n")
		buf.WriteString("|-------|-------------|\n")
		for _, alias := range data.Aliases {
			buf.WriteString(fmt.Sprintf("| `%s` | `%s` |\n", alias.From, alias.To))
		}
		buf.WriteString("\n")
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	// Write output
	if err := os.WriteFile(outputPath, []byte(buf.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %s\n", outputPath)
}

func findRepoRoot() string {
	// Start from the current working directory
	dir, err := os.Getwd()
	if err != nil {
		dir = "."
	}

	// Walk up looking for go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// Default to current directory
	return "."
}

func documentClass(buf *strings.Builder, name string, class Class, aliases []ClassAlias) {
	buf.WriteString(fmt.Sprintf("## `%s`\n\n", name))

	// Description
	if class.Description != "" {
		buf.WriteString(class.Description)
		buf.WriteString("\n\n")
	}

	// Show aliases that point to this class
	var classAliases []string
	for _, alias := range aliases {
		if alias.To == name {
			classAliases = append(classAliases, alias.From)
		}
	}
	if len(classAliases) > 0 {
		sort.Strings(classAliases)
		buf.WriteString(fmt.Sprintf("**Aliases:** `%s`\n\n", strings.Join(classAliases, "`, `")))
	}

	// When to use this class (inferred from description and chain)
	buf.WriteString("### When to Use\n\n")
	buf.WriteString(inferUsageGuidance(name, class))
	buf.WriteString("\n")

	// Fallback chain
	buf.WriteString("### Fallback Chain\n\n")
	buf.WriteString("The resolver tries candidates in order until one passes authentication checks:\n\n")
	buf.WriteString("| Rank | Model | Driver | Provider | Auth Method | Tags |\n")
	buf.WriteString("|------|-------|--------|----------|-------------|------|\n")

	for i, candidate := range class.Chain {
		tags := strings.Join(candidate.Tags, ", ")
		if tags == "" {
			tags = "-"
		}

		authMethod := candidate.Requires.Method
		if candidate.Requires.Method == "cli_oauth" && candidate.Requires.Tool != "" {
			authMethod = fmt.Sprintf("%s (%s)", candidate.Requires.Method, candidate.Requires.Tool)
		}

		buf.WriteString(fmt.Sprintf("| %d | `%s` | `%s` | `%s` | %s | %s |\n",
			i,
			candidate.ModelID,
			candidate.Driver,
			candidate.Requires.Provider,
			authMethod,
			tags))
	}
	buf.WriteString("\n")
}

func inferUsageGuidance(name string, class Class) string {
	// Use class description as the primary guidance
	if class.Description != "" {
		return class.Description
	}

	// Fallback guidance based on class name
	switch name {
	case "hard_coding", "coding":
		return "Use for complex multi-file coding tasks requiring deep reasoning."
	case "quick_easy", "fast":
		return "Use for low-latency, low-cost tasks where speed is more important than depth."
	case "deep_investigation", "research":
		return "Use for long-context research and synthesis requiring 1M+ token windows."
	case "frontend_aesthetic":
		return "Use for UI/UX design critique and frontend component generation."
	case "architectural_critique":
		return "Use for system design review and architectural trade-off analysis."
	default:
		return "See description above for appropriate usage."
	}
}
