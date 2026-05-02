// `kilroy policy` subcommand: inspect the embedded routing policy.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danshapiro/kilroy/internal/attractor/rundb"
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
	case "explain":
		policyExplain(args[1:])
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
	fmt.Fprintln(os.Stderr, "  kilroy policy explain <run-id> [--json]")
	fmt.Fprintln(os.Stderr, "    explain reports which candidate each agentic node in the run")
	fmt.Fprintln(os.Stderr, "    actually picked, reading per-step resolution.json artifacts.")
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

// ── kilroy policy explain ────────────────────────────────────────────────────

// policyExplainResult is the JSON output shape for `policy explain`. It
// mirrors what the engine wrote to each per-step resolution.json without
// re-deriving anything; the tool is a faithful renderer, not a re-resolver.
type policyExplainResult struct {
	RunID       string                   `json:"run_id"`
	GraphName   string                   `json:"graph_name,omitempty"`
	LogsRoot    string                   `json:"logs_root"`
	Resolutions []map[string]interface{} `json:"resolutions"`
}

func policyExplain(args []string) {
	runID, asJSON, err := parsePolicyExplainArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "open run database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	summary, err := db.GetRun(runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lookup run %q: %v\n", runID, err)
		os.Exit(1)
	}
	if summary == nil {
		fmt.Fprintf(os.Stderr, "no run found matching %q\n", runID)
		os.Exit(1)
	}

	resolutions, err := readResolutionsFromLogsRoot(summary.LogsRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read resolutions from %s: %v\n", summary.LogsRoot, err)
		os.Exit(1)
	}

	if asJSON {
		out := policyExplainResult{
			RunID:       summary.RunID,
			GraphName:   summary.GraphName,
			LogsRoot:    summary.LogsRoot,
			Resolutions: resolutions,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		return
	}

	fmt.Printf("run:       %s\n", summary.RunID)
	if summary.GraphName != "" {
		fmt.Printf("graph:     %s\n", summary.GraphName)
	}
	fmt.Printf("logs_root: %s\n\n", summary.LogsRoot)

	if len(resolutions) == 0 {
		fmt.Println("(no class-driven resolutions recorded — the graph may not declare class= on any agentic node, or this run predates Step 5)")
		return
	}

	for _, r := range resolutions {
		nodeID := mapStr(r, "node_id")
		fmt.Printf("node %s:\n", nodeID)
		req, _ := r["resolution"].(map[string]interface{})
		if req == nil {
			fmt.Println("  (resolution.json present but malformed — skipping)")
			continue
		}
		requested, _ := req["requested"].(map[string]interface{})
		resolved, _ := req["resolved"].(map[string]interface{})
		fmt.Printf("  requested: %s=%s\n", mapStr(requested, "type"), mapStr(requested, "value"))
		fmt.Printf("  resolved:  %s via %s", mapStr(resolved, "model_id"), mapStr(resolved, "driver"))
		if auth := mapStr(resolved, "auth_method"); auth != "" {
			if src := mapStr(resolved, "auth_source"); src != "" {
				fmt.Printf(" (%s: %s)", auth, src)
			} else {
				fmt.Printf(" (%s)", auth)
			}
		}
		fmt.Println()
		if rank, ok := req["fallback_rank"].(float64); ok {
			fmt.Printf("  rank:      %d of class chain\n", int(rank))
		}
		if pv := mapStr(req, "policy_version"); pv != "" {
			fmt.Printf("  policy:    %s", pv)
			if at := mapStr(req, "resolved_at"); at != "" {
				fmt.Printf(" (resolved at %s)", at)
			}
			fmt.Println()
		}
		if skipped, ok := req["skipped"].([]interface{}); ok && len(skipped) > 0 {
			fmt.Println("  skipped:")
			for _, s := range skipped {
				sm, _ := s.(map[string]interface{})
				if sm == nil {
					continue
				}
				rank := 0
				if rk, ok := sm["rank"].(float64); ok {
					rank = int(rk)
				}
				fmt.Printf("    rank %d: %s / %s — %s\n",
					rank,
					mapStr(sm, "model_id"),
					mapStr(sm, "driver"),
					mapStr(sm, "reason"))
			}
		}
		fmt.Println()
	}
}

func parsePolicyExplainArgs(args []string) (runID string, asJSON bool, err error) {
	for _, a := range args {
		switch a {
		case "--json":
			asJSON = true
		case "-h", "--help":
			return "", false, fmt.Errorf("usage: kilroy policy explain <run-id> [--json]")
		default:
			if runID != "" {
				return "", false, fmt.Errorf("unexpected extra argument %q", a)
			}
			runID = a
		}
	}
	if runID == "" {
		return "", false, fmt.Errorf("run id required\nusage: kilroy policy explain <run-id> [--json]")
	}
	return runID, asJSON, nil
}

// readResolutionsFromLogsRoot walks a run's logs_root one level deep and
// reads any resolution.json found inside immediate subdirectories (each
// subdirectory is a node stage). Returns the parsed records sorted by
// node_id for stable output. Missing files are not errors — runs whose
// graph has no class-attributed nodes legitimately have no resolutions.
func readResolutionsFromLogsRoot(logsRoot string) ([]map[string]interface{}, error) {
	if logsRoot == "" {
		return nil, fmt.Errorf("run has no logs_root recorded")
	}
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		return nil, err
	}
	var records []map[string]interface{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(logsRoot, e.Name(), "resolution.json")
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) || isNotADirErr(err) {
				continue
			}
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		var rec map[string]interface{}
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		return mapStr(records[i], "node_id") < mapStr(records[j], "node_id")
	})
	return records, nil
}

// mapStr extracts a string key from a generic map; missing or wrong-type
// values return "". Used for tolerant rendering of resolution.json fields
// in case shape drifts in future versions.
func mapStr(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// isNotADirErr returns true for the "not a directory" PathError variants
// that surface when a stage path is a file rather than a directory.
func isNotADirErr(err error) bool {
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		return false
	}
	return strings.Contains(pe.Error(), "not a directory")
}
