// `kilroy policy` subcommand: inspect the embedded routing policy.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/danshapiro/kilroy/internal/policy"
)

func policyCmd(args []string) {
	if len(args) == 0 {
		policyUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "list":
		policyList(args[1:])
	case "show":
		policyShow(args[1:])
	case "-h", "--help", "help":
		policyUsage()
		os.Exit(0)
	default:
		policyUsage()
		fmt.Fprintf(os.Stderr, "unknown policy subcommand: %q\n", args[0])
		os.Exit(1)
	}
}

func policyUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy policy list [--json]")
	fmt.Fprintln(os.Stderr, "  kilroy policy show <class-name> [--json]")
}

// ── JSON-serialization view types ────────────────────────────────────────────

type policyListJSON struct {
	SchemaVersion string                `json:"schema_version"`
	PolicyVersion string                `json:"policy_version"`
	Classes       map[string]classJSON  `json:"classes"`
	Aliases       []aliasJSON           `json:"aliases,omitempty"`
}

type classJSON struct {
	Description string          `json:"description"`
	Chain       []candidateJSON `json:"chain"`
}

type candidateJSON struct {
	ModelID     string   `json:"model_id"`
	Driver      string   `json:"driver"`
	Transport   string   `json:"transport"`
	HistorySink string   `json:"history_sink"`
	Tags        []string `json:"tags"`
	Auth        authJSON `json:"auth"`
}

type authJSON struct {
	Kind   string `json:"kind"`
	EnvVar string `json:"env_var,omitempty"`
	CLI    string `json:"cli,omitempty"`
}

type aliasJSON struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ── helpers ──────────────────────────────────────────────────────────────────

func dataToPolicyListJSON(d *policy.Data) policyListJSON {
	classes := make(map[string]classJSON, len(d.Classes))
	for name, cls := range d.Classes {
		chain := make([]candidateJSON, len(cls.Chain))
		for i, c := range cls.Chain {
			chain[i] = candidateJSON{
				ModelID:     c.ModelID,
				Driver:      c.Driver,
				Transport:   c.Transport,
				HistorySink: c.HistorySink,
				Tags:        c.Tags,
				Auth: authJSON{
					Kind:   c.Auth.Kind,
					EnvVar: c.Auth.EnvVar,
					CLI:    c.Auth.CLI,
				},
			}
		}
		classes[name] = classJSON{
			Description: cls.Description,
			Chain:       chain,
		}
	}
	aliases := make([]aliasJSON, 0, len(d.Aliases))
	for _, a := range d.Aliases {
		aliases = append(aliases, aliasJSON{From: a.From, To: a.To})
	}
	return policyListJSON{
		SchemaVersion: d.SchemaVersion,
		PolicyVersion: d.PolicyVersion,
		Classes:       classes,
		Aliases:       aliases,
	}
}

// resolveAlias looks up className against the alias table; returns (resolved, aliasUsed, originalFrom).
func resolveAlias(d *policy.Data, className string) (string, bool) {
	for _, a := range d.Aliases {
		if a.From == className {
			return a.To, true
		}
	}
	return className, false
}

func sortedClassNames(d *policy.Data) []string {
	names := make([]string, 0, len(d.Classes))
	for name := range d.Classes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ── policyList ───────────────────────────────────────────────────────────────

func policyList(args []string) {
	var jsonOut bool
	for _, a := range args {
		switch a {
		case "--json":
			jsonOut = true
		case "-h", "--help":
			policyUsage()
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
			os.Exit(1)
		}
	}

	d, err := policy.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "policy load error: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		v := dataToPolicyListJSON(d)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Human-readable: one block per class, sorted by name.
	names := sortedClassNames(d)
	for _, name := range names {
		cls := d.Classes[name]
		fmt.Printf("%s\n", name)
		fmt.Printf("  %s\n", cls.Description)
		fmt.Printf("  Fallback chain: %d candidates\n", len(cls.Chain))
		for i, c := range cls.Chain {
			// Render tags only — Auth kind is implicit from the tags
			// (`subscription` / `api_key`) and from the per-class detail
			// view. Avoid synthesizing duplicates of those tag values.
			tagStr := strings.Join(c.Tags, ", ")
			fmt.Printf("    rank %d: %s / %s (%s)\n", i, c.ModelID, c.Driver, tagStr)
		}
		fmt.Println()
	}
}

// ── policyShow ───────────────────────────────────────────────────────────────

func policyShow(args []string) {
	var jsonOut bool
	var className string

	for _, a := range args {
		switch a {
		case "--json":
			jsonOut = true
		case "-h", "--help":
			policyUsage()
			os.Exit(0)
		default:
			if strings.HasPrefix(a, "--") {
				fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
				os.Exit(1)
			}
			className = a
		}
	}

	if className == "" {
		policyUsage()
		os.Exit(1)
	}

	d, err := policy.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "policy load error: %v\n", err)
		os.Exit(1)
	}

	// Resolve alias.
	resolved, wasAlias := resolveAlias(d, className)
	if wasAlias {
		fmt.Fprintf(os.Stderr, "Resolved alias %q -> %q\n", className, resolved)
		className = resolved
	}

	cls, ok := d.Classes[className]
	if !ok {
		names := sortedClassNames(d)
		fmt.Fprintf(os.Stderr, "class %q not found\n", className)
		fmt.Fprintf(os.Stderr, "available classes: %s\n", strings.Join(names, ", "))
		os.Exit(1)
	}

	if jsonOut {
		cj := classJSON{
			Description: cls.Description,
			Chain:       make([]candidateJSON, len(cls.Chain)),
		}
		for i, c := range cls.Chain {
			cj.Chain[i] = candidateJSON{
				ModelID:     c.ModelID,
				Driver:      c.Driver,
				Transport:   c.Transport,
				HistorySink: c.HistorySink,
				Tags:        c.Tags,
				Auth: authJSON{
					Kind:   c.Auth.Kind,
					EnvVar: c.Auth.EnvVar,
					CLI:    c.Auth.CLI,
				},
			}
		}
		out := map[string]classJSON{className: cj}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Human-readable detail.
	fmt.Printf("%s\n", className)
	fmt.Printf("  %s\n\n", cls.Description)
	fmt.Printf("  Fallback chain (%d candidates, in order):\n\n", len(cls.Chain))

	for i, c := range cls.Chain {
		fmt.Printf("  Rank %d\n", i)
		fmt.Printf("    Model:        %s\n", c.ModelID)
		fmt.Printf("    Driver:       %s\n", c.Driver)
		fmt.Printf("    Transport:    %s\n", c.Transport)
		fmt.Printf("    History sink: %s\n", c.HistorySink)
		switch c.Auth.Kind {
		case "env_var":
			fmt.Printf("    Auth:         env_var %s\n", c.Auth.EnvVar)
		case "cli_session":
			fmt.Printf("    Auth:         cli_session via `%s`\n", c.Auth.CLI)
		default:
			fmt.Printf("    Auth:         %s\n", c.Auth.Kind)
		}
		fmt.Printf("    Tags:         %s\n", strings.Join(c.Tags, ", "))
		fmt.Println()
	}
}
