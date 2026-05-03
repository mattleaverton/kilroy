// auth_check.go: kilroy auth check diagnostic plus config-loading helpers and
// annotated-list types shared with the auth list enhancement (A8c + A8d).
// See docs/plans/2026-05-02-auth-class-resolver-integration.md §3 and §6.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/version"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config loading
// ─────────────────────────────────────────────────────────────────────────────

// loadAuthConfig loads the merged user/project auth config via the
// canonical binding.LoadConfig path (which respects XDG_CONFIG_HOME).
// projectRoot is the directory that contains .kilroy/; pass "" to
// auto-detect via findProjectRoot(). Returns *binding.ErrNoConfig when
// neither file exists.
func loadAuthConfig(projectRoot string) (*binding.Config, error) {
	if projectRoot == "" {
		projectRoot = findProjectRoot()
	}
	cfg, err := binding.LoadConfig(projectRoot)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// loadOneCfg reads a single auth.toml file. Returns nil, nil when the file is
// absent; hard-errors on parse failures.
func loadOneCfg(path string) (*binding.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg binding.Config
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Bindings == nil {
		cfg.Bindings = map[string]string{}
	}
	if cfg.Chains == nil {
		cfg.Chains = map[string]binding.Chain{}
	}
	// Populate Name from map key (Chain.Name has toml:"-"; decoder skips it).
	for name, chain := range cfg.Chains {
		chain.Name = name
		cfg.Chains[name] = chain
	}
	return &cfg, nil
}

// mergeAuthConfigs merges user and project configs; project replaces user
// entries wholesale by chain name and binding key (plan §1 rule 6).
func mergeAuthConfigs(user, project *binding.Config) *binding.Config {
	if project == nil {
		return user
	}
	merged := &binding.Config{
		Bindings: make(map[string]string, len(user.Bindings)+len(project.Bindings)),
		Chains:   make(map[string]binding.Chain, len(user.Chains)+len(project.Chains)),
	}
	for k, v := range user.Bindings {
		merged.Bindings[k] = v
	}
	for k, v := range user.Chains {
		merged.Chains[k] = v
	}
	for k, v := range project.Bindings {
		merged.Bindings[k] = v
	}
	for k, v := range project.Chains {
		merged.Chains[k] = v
	}
	return merged
}

// findProjectRoot walks up from cwd looking for a .kilroy/ directory.
// Returns "" when none is found.
func findProjectRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".kilroy")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Requirement key parsing and error helpers
// ─────────────────────────────────────────────────────────────────────────────

// parseRequirementKey converts "<provider>/<method>[/<tool>]" back into a
// Requirement (inverse of Requirement.Key()).
func parseRequirementKey(key string) (binding.Requirement, error) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 2 {
		return binding.Requirement{}, fmt.Errorf(
			"invalid binding key %q: expected provider/method[/tool]", key)
	}
	req := binding.Requirement{
		Provider: parts[0],
		Method:   binding.Method(parts[1]),
	}
	if len(parts) == 3 {
		req.Tool = parts[2]
	}
	return req, nil
}

// resolveErrorType returns the Go type name for a typed binding resolver error.
func resolveErrorType(err error) string {
	var ex *binding.ErrChainExhausted
	if errors.As(err, &ex) {
		return "ErrChainExhausted"
	}
	var noChain *binding.ErrNoChainForRequirement
	if errors.As(err, &noChain) {
		return "ErrNoChainForRequirement"
	}
	var amb *binding.ErrAmbiguousAuthChain
	if errors.As(err, &amb) {
		return "ErrAmbiguousAuthChain"
	}
	var unk *binding.ErrUnknownChain
	if errors.As(err, &unk) {
		return "ErrUnknownChain"
	}
	return "ErrUnknown"
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain check results (shared by auth check and auth list --chains)
// ─────────────────────────────────────────────────────────────────────────────

// chainCheckResult is one entry in the check / chains-view output.
type chainCheckResult struct {
	BindingKey   string                  `json:"binding_key"`
	ChainName    string                  `json:"chain_name,omitempty"`
	Status       string                  `json:"status"`
	Source       *binding.Source         `json:"source,omitempty"`
	FallbackRank int                     `json:"fallback_rank,omitempty"`
	Skipped      []binding.SkippedSource `json:"skipped"`
	ErrorType    string                  `json:"error,omitempty"`
	Message      string                  `json:"message,omitempty"`
}

// runChainChecks runs the resolver against every binding in cfg (sorted by
// key) and returns one chainCheckResult per binding.
func runChainChecks(cfg *binding.Config, detect binding.DetectionView) []chainCheckResult {
	resolver := binding.NewResolver(cfg, detect)

	keys := make([]string, 0, len(cfg.Bindings))
	for k := range cfg.Bindings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	results := make([]chainCheckResult, 0, len(keys))
	for _, key := range keys {
		req, pErr := parseRequirementKey(key)
		if pErr != nil {
			results = append(results, chainCheckResult{
				BindingKey: key,
				ChainName:  cfg.Bindings[key],
				Status:     "error",
				Skipped:    []binding.SkippedSource{},
				ErrorType:  "ErrMalformedKey",
				Message:    pErr.Error(),
			})
			continue
		}
		snap, resolveErr := resolver.Resolve(req)
		if resolveErr != nil {
			results = append(results, chainCheckResult{
				BindingKey: key,
				ChainName:  cfg.Bindings[key],
				Status:     "error",
				Skipped:    []binding.SkippedSource{},
				ErrorType:  resolveErrorType(resolveErr),
				Message:    resolveErr.Error(),
			})
		} else {
			skipped := snap.Skipped
			if skipped == nil {
				skipped = []binding.SkippedSource{}
			}
			results = append(results, chainCheckResult{
				BindingKey:   key,
				ChainName:    snap.ChainName,
				Status:       "ok",
				Source:       &snap.Source,
				FallbackRank: snap.FallbackRank,
				Skipped:      skipped,
			})
		}
	}
	return results
}

// ─────────────────────────────────────────────────────────────────────────────
// Annotated auth list types (auth list --json and --pretty with referenced_by)
// ─────────────────────────────────────────────────────────────────────────────

// annotatedEntry embeds auth.Entry and adds referenced_by / unreferenced
// fields derived from the user/project auth config.
type annotatedEntry struct {
	auth.Entry
	// ReferencedBy is the sorted list of chain names that include this entry
	// as a source. nil when no config is loaded; non-nil (possibly empty) when
	// config is available.
	ReferencedBy []string `json:"referenced_by,omitempty"`
	// Unreferenced is true when config is available but no chain references
	// this entry.
	Unreferenced bool `json:"unreferenced,omitempty"`
}

// annotatedListOutput is the JSON shape for kilroy auth list (with annotation).
type annotatedListOutput struct {
	KilroyVersion string           `json:"kilroy_version"`
	ScannedAt     string           `json:"scanned_at"`
	Platform      string           `json:"platform"`
	Entries       []annotatedEntry `json:"entries"`
	Summary       auth.Summary     `json:"summary"`
	// ConfigState is "uninitialized" when auth config is absent; omitted
	// when config loaded successfully.
	ConfigState string `json:"config_state,omitempty"`
}

// annotateListEntries annotates an auth.Entry slice with referenced_by chains
// from cfg. When cfg is nil (config absent), annotation fields are not set.
func annotateListEntries(entries []auth.Entry, cfg *binding.Config) []annotatedEntry {
	result := make([]annotatedEntry, len(entries))
	for i, e := range entries {
		ae := annotatedEntry{Entry: e}
		if cfg != nil {
			refs := findReferencingChains(e, cfg)
			if len(refs) > 0 {
				ae.ReferencedBy = refs
			} else {
				ae.Unreferenced = true
			}
		}
		result[i] = ae
	}
	return result
}

// findReferencingChains returns the sorted list of chain names in cfg that
// include the given auth.Entry as a source.
func findReferencingChains(e auth.Entry, cfg *binding.Config) []string {
	var chains []string
	for name, chain := range cfg.Chains {
		if chain.Requires.Provider != e.Provider {
			continue
		}
		if chainReferencesEntry(chain, e) {
			chains = append(chains, name)
		}
	}
	sort.Strings(chains)
	return chains
}

// chainReferencesEntry returns true when chain includes a source matching e:
//   - env_var entry → source.Kind==SourceEnvVar && source.Name==entry.Source.EnvVar
//   - cli_oauth entry → source.Kind==SourceCLISession && source.Tool==entry.Tool
func chainReferencesEntry(chain binding.Chain, e auth.Entry) bool {
	for _, src := range chain.Sources {
		switch {
		case e.Kind == auth.KindEnvVar &&
			src.Kind == binding.SourceEnvVar &&
			e.Source.EnvVar != "" &&
			src.Name == e.Source.EnvVar:
			return true
		case e.Kind == auth.KindCLIOAuth &&
			src.Kind == binding.SourceCLISession &&
			e.Tool != "" &&
			src.Tool == e.Tool:
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// Pretty printers for auth list (annotated + chains view)
// ─────────────────────────────────────────────────────────────────────────────

// printAuthListPrettyAnnotated prints the human-readable auth list with
// per-entry referenced_by annotation.
func printAuthListPrettyAnnotated(out auth.ListOutput, entries []annotatedEntry, configState string) {
	fmt.Printf("Kilroy auth scan — %s on %s\n\n", out.ScannedAt, out.Platform)

	if len(entries) == 0 {
		fmt.Println("No credentials discovered. Set provider env vars or run a CLI tool's login flow.")
	} else {
		for _, ae := range entries {
			e := ae.Entry
			marker := stateMarker(e.State)
			ident := identString(e.Identity)
			toolField := e.Tool
			if toolField == "" {
				toolField = "—"
			}
			fmt.Printf("%s  %-12s %-16s [%s]", marker, e.Provider, toolField, e.Kind)
			if ident != "" {
				fmt.Printf(" %s", ident)
			}
			fmt.Println()

			var src []string
			if e.Source.EnvVar != "" {
				src = append(src, "env="+e.Source.EnvVar)
			}
			if e.Source.File != "" {
				src = append(src, "file="+abbrevPath(e.Source.File))
			}
			if e.Source.KeychainService != "" {
				src = append(src, "keychain="+e.Source.KeychainService)
			}
			if len(src) > 0 {
				fmt.Printf("    %s\n", strings.Join(src, "  "))
			}

			if e.Expiry != nil && e.Expiry.AccessTokenExpiresAt != nil {
				fmt.Printf("    expires: %s  refreshable: %v\n",
					e.Expiry.AccessTokenExpiresAt.Format("2006-01-02T15:04Z"),
					e.Expiry.Refreshable)
			}
			if len(e.Profiles) > 1 {
				names := make([]string, 0, len(e.Profiles))
				for _, p := range e.Profiles {
					m := ""
					if p.Active {
						m = "*"
					}
					names = append(names, m+p.Name)
				}
				fmt.Printf("    profiles: %s  (* = active)\n", strings.Join(names, ", "))
			}
			if len(e.Shadows) > 0 {
				fmt.Printf("    shadows: %s (env var wins at runtime)\n", strings.Join(e.Shadows, ", "))
			}
			if len(e.ShadowedBy) > 0 {
				fmt.Printf("    shadowed by: %s\n", strings.Join(e.ShadowedBy, ", "))
			}
			for _, n := range e.Notes {
				fmt.Printf("    %s\n", n)
			}
			if e.State != auth.StateOK && e.Remediation != "" {
				fmt.Printf("    fix: %s\n", e.Remediation)
			}

			// Referenced-by annotation (only when config is loaded).
			if len(ae.ReferencedBy) > 0 {
				fmt.Printf("    referenced by: %s\n", strings.Join(ae.ReferencedBy, ", "))
			} else if ae.Unreferenced {
				fmt.Printf("    (unreferenced in config)\n")
			}
			fmt.Println()
		}
	}

	fmt.Printf("Summary: %d entries — %d ok, %d expired, %d missing, %d ambiguous\n",
		out.Summary.Total, out.Summary.OK, out.Summary.Expired,
		out.Summary.Missing, out.Summary.Ambiguous)

	if configState == "uninitialized" {
		fmt.Println("\nconfig not initialized — run `kilroy auth init` to generate")
	}
}

// printAuthListChainsView prints the chain-centric view for auth list --chains.
// When cfg is nil the output degrades to the "uninitialized" notice.
func printAuthListChainsView(cfg *binding.Config, configState string, listOut auth.ListOutput, pretty bool) {
	if cfg == nil {
		if pretty {
			fmt.Println("config not initialized — run `kilroy auth init` to generate")
		} else {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(map[string]interface{}{
				"config_state": configState,
				"chains":       []interface{}{},
				"summary":      map[string]int{"ok": 0, "error": 0, "total": 0},
			})
		}
		return
	}

	view := binding.AuthListView{List: listOut}
	checks := runChainChecks(cfg, view)

	okCount, errCount := 0, 0
	for _, c := range checks {
		if c.Status == "ok" {
			okCount++
		} else {
			errCount++
		}
	}

	if pretty {
		fmt.Println("Auth chains — current detection")
		fmt.Println()
		for _, c := range checks {
			key := `"` + c.BindingKey + `"`
			if c.Status == "ok" {
				srcID := ""
				if c.Source != nil {
					srcID = c.Source.ID()
				}
				fmt.Printf("%-36s → chain %-26s → %s (ok)\n", key, c.ChainName, srcID)
			} else {
				fmt.Printf("%-36s → chain %-26s → ERROR: %s\n", key, c.ChainName, c.Message)
			}
		}
		fmt.Println()
		fmt.Printf("Summary: %d ok, %d error\n", okCount, errCount)
	} else {
		type chainsOutput struct {
			Chains  []chainCheckResult `json:"chains"`
			Summary struct {
				OK    int `json:"ok"`
				Error int `json:"error"`
				Total int `json:"total"`
			} `json:"summary"`
		}
		out := chainsOutput{Chains: checks}
		out.Summary.OK = okCount
		out.Summary.Error = errCount
		out.Summary.Total = okCount + errCount
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if encErr := enc.Encode(out); encErr != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", encErr)
			os.Exit(1)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// kilroy auth check
// ─────────────────────────────────────────────────────────────────────────────

// authCheckUsage prints usage text for kilroy auth check.
func authCheckUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy auth check [--pretty] [--json] [--project <dir>]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  Runs the auth resolver for every configured binding and reports status.")
	fmt.Fprintln(os.Stderr, "  --project <dir>  project root containing .kilroy/ (default: nearest .kilroy/)")
	fmt.Fprintln(os.Stderr, "  Exits 1 when auth config is absent or any binding cannot be resolved.")
}

// authCheck implements kilroy auth check: loads user/project config, runs the
// resolver for every binding, and reports per-binding status.
func authCheck(args []string) {
	var jsonMode bool
	var projectDir string

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "--json":
			jsonMode = true
		case "--pretty":
			jsonMode = false
		case "--project":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--project requires a directory argument")
				os.Exit(1)
			}
			projectDir = args[i]
		case "-h", "--help":
			authCheckUsage()
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
			os.Exit(1)
		}
	}

	cfg, err := loadAuthConfig(projectDir)
	if err != nil {
		var noConfig *binding.ErrNoConfig
		if errors.As(err, &noConfig) {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			fmt.Fprintln(os.Stderr, "run `kilroy auth init` to create your auth config")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error loading auth config: %v\n", err)
		os.Exit(1)
	}

	listOut := auth.ListAll(version.Version, auth.DefaultDetectors())
	view := binding.AuthListView{List: listOut}
	checks := runChainChecks(cfg, view)

	okCount, errCount := 0, 0
	for _, c := range checks {
		if c.Status == "ok" {
			okCount++
		} else {
			errCount++
		}
	}

	if jsonMode {
		type checkOutput struct {
			Checks  []chainCheckResult `json:"checks"`
			Summary struct {
				OK    int `json:"ok"`
				Error int `json:"error"`
				Total int `json:"total"`
			} `json:"summary"`
		}
		out := checkOutput{Checks: checks}
		out.Summary.OK = okCount
		out.Summary.Error = errCount
		out.Summary.Total = okCount + errCount
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if encErr := enc.Encode(out); encErr != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", encErr)
			os.Exit(1)
		}
	} else {
		fmt.Println("Auth chains — resolution against current detection")
		fmt.Println()
		for _, c := range checks {
			if c.Status == "ok" {
				srcID := ""
				if c.Source != nil {
					srcID = c.Source.ID()
				}
				fmt.Printf("OK   %-32s → %-26s → %s\n", c.BindingKey, c.ChainName, srcID)
			} else {
				fmt.Printf("ERR  %-32s → %s\n", c.BindingKey, c.ChainName)
				fmt.Printf("     %s\n", c.Message)
			}
		}
		fmt.Println()
		fmt.Printf("Summary: %d ok, %d error\n", okCount, errCount)
	}

	if errCount > 0 {
		os.Exit(1)
	}
}
