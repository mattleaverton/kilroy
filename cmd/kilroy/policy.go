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
	case "resolve":
		policyResolve(args[1:])
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
	fmt.Fprintln(os.Stderr, "  kilroy policy resolve <class-name> [--json]")
	fmt.Fprintln(os.Stderr, "    resolve runs the resolver against the current machine state")
	fmt.Fprintln(os.Stderr, "    and reports which candidate would be picked for the class.")
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

// ── kilroy policy resolve ────────────────────────────────────────────────────

func policyResolve(args []string) {
	className, asJSON, err := parsePolicyClassArgs(args, "resolve")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	data, err := policy.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load policy: %v\n", err)
		os.Exit(1)
	}

	state := policy.CollectMachineState()
	res, rerr := policy.Resolve(policy.ResolveRequest{ClassID: className}, data, state)

	if asJSON {
		out := map[string]any{}
		if rerr != nil {
			out["error"] = rerr.Error()
		} else {
			out["resolved"] = map[string]any{
				"model_id":      res.ModelID,
				"driver":        res.Driver,
				"transport":     res.Transport,
				"history_sink":  res.HistorySink,
				"auth_method":   res.AuthMethod,
				"auth_source":   res.AuthSource,
				"fallback_rank": res.FallbackRank,
				"skipped":       res.Skipped,
				"request_type":  res.RequestType,
				"request_value": res.RequestValue,
			}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		if rerr != nil {
			os.Exit(1)
		}
		return
	}

	if rerr != nil {
		fmt.Fprintf(os.Stderr, "resolve %q: %v\n", className, rerr)
		os.Exit(1)
	}

	fmt.Printf("class:       %s\n", className)
	if res.RequestValue != className {
		fmt.Printf("  (resolved alias %q -> %q)\n", className, res.RequestValue)
	}
	fmt.Printf("\nResolved (rank %d of class chain):\n", res.FallbackRank)
	fmt.Printf("  model:        %s\n", res.ModelID)
	fmt.Printf("  driver:       %s\n", res.Driver)
	fmt.Printf("  transport:    %s\n", res.Transport)
	fmt.Printf("  history sink: %s\n", res.HistorySink)
	fmt.Printf("  auth:         %s", res.AuthMethod)
	if res.AuthSource != "" {
		fmt.Printf(" (%s)", res.AuthSource)
	}
	fmt.Println()

	if len(res.Skipped) > 0 {
		fmt.Println("\nSkipped candidates:")
		for _, s := range res.Skipped {
			fmt.Printf("  rank %d: %s / %s — %s\n", s.Rank, s.ModelID, s.Driver, s.Reason)
		}
	}
}

// parsePolicyClassArgs is a small helper for show/resolve that share the same
// "<class-name> [--json]" argument shape.
func parsePolicyClassArgs(args []string, subcmd string) (className string, asJSON bool, err error) {
	for _, a := range args {
		switch a {
		case "--json":
			asJSON = true
		case "-h", "--help":
			return "", false, fmt.Errorf("usage: kilroy policy %s <class-name> [--json]", subcmd)
		default:
			if className != "" {
				return "", false, fmt.Errorf("unexpected extra argument %q", a)
			}
			className = a
		}
	}
	if className == "" {
		return "", false, fmt.Errorf("class name required\nusage: kilroy policy %s <class-name> [--json]", subcmd)
	}
	return className, asJSON, nil
}
