// auth_init.go implements `kilroy auth init` (A8b).
// Implements §4 of docs/plans/2026-05-02-auth-class-resolver-integration.md:
// reads the default template, runs detection, writes a personalised auth.toml
// with detected sources active and undetected sources commented.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/auth/binding"
	"github.com/danshapiro/kilroy/internal/version"
)

// authInitCmd parses CLI flags and delegates to authInitRun.
func authInitCmd(args []string) {
	var opts authInitOpts
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--force":
			opts.force = true
		case "--rescan":
			opts.rescan = true
		case "--path":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "--path requires an argument")
				os.Exit(1)
			}
			i++
			opts.pathDir = args[i]
		case "--json":
			opts.jsonOut = true
		case "--pretty":
			opts.jsonOut = false
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: kilroy auth init [--force|--rescan] [--path <dir>] [--json|--pretty]")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  Generates ~/.config/kilroy/auth.toml from the default template.")
			fmt.Fprintln(os.Stderr, "  Active sources (detected on this machine) are emitted as live entries.")
			fmt.Fprintln(os.Stderr, "  Undetected sources are commented; uncomment after adding the credential.")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  --force        overwrite an existing auth.toml")
			fmt.Fprintln(os.Stderr, "  --rescan       update an existing auth.toml with newly detected template sources")
			fmt.Fprintln(os.Stderr, "  --path <dir>   write to <dir>/auth.toml instead of the default location")
			fmt.Fprintln(os.Stderr, "  --json         print machine-readable summary instead of human-friendly")
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", args[i])
			os.Exit(1)
		}
	}
	if err := authInitRun(opts, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// authInitOpts holds the parsed flags for auth init.
type authInitOpts struct {
	force   bool
	rescan  bool
	pathDir string // override directory; file name is always "auth.toml"
	jsonOut bool
}

// authInitRun is the testable core of kilroy auth init.
func authInitRun(opts authInitOpts, out io.Writer) error {
	if opts.rescan {
		if opts.force {
			return fmt.Errorf("--force and --rescan cannot be used together")
		}
		return authInitRescanRun(opts, out)
	}
	// 1. Load the template.
	tmpl, err := binding.LoadDefaultTemplates()
	if err != nil {
		return fmt.Errorf("load default templates: %w", err)
	}

	// 2. Run detection.
	detected := auth.ListAll(version.Version, auth.DefaultDetectors())
	view := binding.AuthListView{List: detected}

	// 3. Compute destination path.
	destDir := opts.pathDir
	if destDir == "" {
		destDir = authDefaultConfigDir()
	}
	dest := filepath.Join(destDir, "auth.toml")

	// 4. Idempotency guard.
	if !opts.force {
		if _, statErr := os.Stat(dest); statErr == nil {
			return fmt.Errorf("auth config already exists at %s; use --force to overwrite", dest)
		}
	}

	// 5. Build TOML content with active/commented sources.
	timestamp := time.Now().UTC().Format(time.RFC3339)
	content := buildAuthInitTOML(tmpl, view, timestamp)

	// 6. Write the file.
	if mkErr := os.MkdirAll(destDir, 0o755); mkErr != nil {
		return fmt.Errorf("create config dir %s: %w", destDir, mkErr)
	}
	if writeErr := os.WriteFile(dest, []byte(content), 0o600); writeErr != nil {
		return fmt.Errorf("write %s: %w", dest, writeErr)
	}

	// 7. Print summary.
	summary := buildInitSummary(dest, tmpl, view)
	if opts.jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(summary)
	}
	printInitSummaryPretty(out, summary)
	return nil
}

func authInitRescanRun(opts authInitOpts, out io.Writer) error {
	tmpl, err := binding.LoadDefaultTemplates()
	if err != nil {
		return fmt.Errorf("load default templates: %w", err)
	}
	detected := auth.ListAll(version.Version, auth.DefaultDetectors())
	view := binding.AuthListView{List: detected}

	destDir := opts.pathDir
	if destDir == "" {
		destDir = authDefaultConfigDir()
	}
	dest := filepath.Join(destDir, "auth.toml")

	cfg, exists, err := loadAuthConfigFromPath(dest)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("auth config does not exist at %s; run `kilroy auth init` first", dest)
	}
	added := mergeDetectedTemplateSources(&cfg, tmpl, view)
	if err := saveGlobalAuthConfig(dest, cfg); err != nil {
		return err
	}

	summary := buildInitSummary(dest, cfg, view)
	summary.AddedSources = added
	if opts.jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(summary)
	}
	printInitSummaryPretty(out, summary)
	return nil
}

func loadAuthConfigFromPath(path string) (binding.Config, bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return binding.Config{}, false, nil
		}
		return binding.Config{}, false, fmt.Errorf("stat %s: %w", path, err)
	}
	cfg, err := binding.LoadConfigFromPaths(path, "")
	if err != nil {
		return binding.Config{}, true, err
	}
	ensureConfigMaps(&cfg)
	return cfg, true, nil
}

func mergeDetectedTemplateSources(cfg *binding.Config, tmpl binding.Config, view binding.DetectionView) []string {
	ensureConfigMaps(cfg)
	added := []string{}
	for reqKey, chainName := range tmpl.Bindings {
		if strings.TrimSpace(cfg.Bindings[reqKey]) == "" {
			cfg.Bindings[reqKey] = chainName
		}
	}
	for _, chainName := range sortedChainNames(tmpl) {
		templateChain := tmpl.Chains[chainName]
		effectiveName := cfg.Bindings[templateChain.Requires.Key()]
		if effectiveName == "" {
			effectiveName = chainName
			cfg.Bindings[templateChain.Requires.Key()] = chainName
		}
		chain := cfg.Chains[effectiveName]
		chain.Name = effectiveName
		if chain.Requires.Provider == "" {
			chain.Requires = templateChain.Requires
		}
		for _, src := range templateChain.Sources {
			if !sourcePresent(view, src) || sourceInChain(chain.Sources, src) {
				continue
			}
			chain.Sources = append(chain.Sources, src)
			added = append(added, effectiveName+":"+src.ID())
		}
		cfg.Chains[effectiveName] = chain
	}
	sort.Strings(added)
	return added
}

// authDefaultConfigDir returns the XDG-aware config directory for kilroy.
func authDefaultConfigDir() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "kilroy")
	}
	home := os.Getenv("HOME")
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, ".config", "kilroy")
}

// ── TOML generation ────────────────────────────────────────────────────────────

// buildAuthInitTOML produces the full content of the generated auth.toml.
// Active sources are emitted as live TOML entries; undetected ones are prefixed
// with "# " so a future parser treats them as comments and a human can
// uncomment them by removing the prefix.
func buildAuthInitTOML(tmpl binding.Config, view binding.DetectionView, timestamp string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Kilroy auth config — generated by `kilroy auth init` at %s.\n", timestamp)
	fmt.Fprintf(&b, "# Active sources reflect what was detected on this machine.\n")
	fmt.Fprintf(&b, "# Commented sources show what kilroy supports but couldn't find;\n")
	fmt.Fprintf(&b, "# uncomment after adding the env var or running the relevant CLI auth.\n")
	fmt.Fprintf(&b, "\n")

	// [bindings] section — verbatim from template, sorted for determinism.
	fmt.Fprintf(&b, "[bindings]\n")
	bindKeys := sortedKeys(tmpl.Bindings)
	for _, k := range bindKeys {
		fmt.Fprintf(&b, "%q = %q\n", k, tmpl.Bindings[k])
	}
	fmt.Fprintf(&b, "\n")

	// [chains.*] sections — one per chain, sorted for determinism.
	chainNames := make([]string, 0, len(tmpl.Chains))
	for n := range tmpl.Chains {
		chainNames = append(chainNames, n)
	}
	sort.Strings(chainNames)

	for _, name := range chainNames {
		chain := tmpl.Chains[name]
		writeChainTOML(&b, name, chain, view)
	}

	return b.String()
}

// writeChainTOML appends one chain's TOML block to b.
func writeChainTOML(b *strings.Builder, name string, chain binding.Chain, view binding.DetectionView) {
	req := chain.Requires
	fmt.Fprintf(b, "[chains.%s]\n", name)
	if req.Tool != "" {
		fmt.Fprintf(b, "requires = { provider = %q, method = %q, tool = %q }\n",
			req.Provider, string(req.Method), req.Tool)
	} else {
		fmt.Fprintf(b, "requires = { provider = %q, method = %q }\n",
			req.Provider, string(req.Method))
	}
	fmt.Fprintf(b, "\n")

	for _, src := range chain.Sources {
		active := sourcePresent(view, src)
		writeSourceBlock(b, name, src, active)
	}
}

// writeSourceBlock appends one [[chains.<name>.sources]] block, either active
// or commented, followed by a blank line for readability.
func writeSourceBlock(b *strings.Builder, chainName string, src binding.Source, active bool) {
	lines := buildSourceLines(chainName, src)
	if active {
		for _, line := range lines {
			fmt.Fprintln(b, line)
		}
	} else {
		for _, line := range lines {
			fmt.Fprintf(b, "# %s\n", line)
		}
	}
	fmt.Fprintln(b)
}

// buildSourceLines returns the TOML lines for a single sources entry
// (without any "# " prefix — callers add that for commented blocks).
func buildSourceLines(chainName string, src binding.Source) []string {
	lines := []string{
		fmt.Sprintf("[[chains.%s.sources]]", chainName),
		fmt.Sprintf("kind = %q", string(src.Kind)),
	}
	switch src.Kind {
	case binding.SourceEnvVar:
		lines = append(lines, fmt.Sprintf("name = %q", src.Name))
	case binding.SourceCLISession:
		lines = append(lines, fmt.Sprintf("tool = %q", src.Tool))
	}
	return lines
}

// sourcePresent reports whether a source is available on this machine.
func sourcePresent(view binding.DetectionView, src binding.Source) bool {
	switch src.Kind {
	case binding.SourceEnvVar:
		return view.EnvVarPresent(src.Name)
	case binding.SourceCLISession:
		return view.CLISessionOK(src.Tool)
	}
	return false
}

// sortedKeys returns the keys of a map[string]string in sorted order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ── Summary ────────────────────────────────────────────────────────────────────

// initSummary is the JSON/pretty-print shape for the init summary.
type initSummary struct {
	Destination  string        `json:"destination"`
	AddedSources []string      `json:"added_sources,omitempty"`
	Chains       []chainStatus `json:"chains"`
	OK           int           `json:"ok"`
	NoUsable     int           `json:"no_usable_source"`
}

// chainStatus describes one chain's detected vs. skipped sources.
type chainStatus struct {
	Name           string   `json:"name"`
	UsableSources  []string `json:"usable_sources"`
	SkippedSources []string `json:"skipped_sources"`
}

// buildInitSummary walks the template chains and classifies each source.
func buildInitSummary(dest string, tmpl binding.Config, view binding.DetectionView) initSummary {
	s := initSummary{Destination: dest}

	chainNames := make([]string, 0, len(tmpl.Chains))
	for n := range tmpl.Chains {
		chainNames = append(chainNames, n)
	}
	sort.Strings(chainNames)

	for _, name := range chainNames {
		chain := tmpl.Chains[name]
		var usable, skipped []string
		for _, src := range chain.Sources {
			if sourcePresent(view, src) {
				usable = append(usable, src.ID())
			} else {
				skipped = append(skipped, src.ID())
			}
		}
		cs := chainStatus{
			Name:           name,
			UsableSources:  usable,
			SkippedSources: skipped,
		}
		if usable == nil {
			cs.UsableSources = []string{}
		}
		if skipped == nil {
			cs.SkippedSources = []string{}
		}
		s.Chains = append(s.Chains, cs)
		if len(usable) > 0 {
			s.OK++
		} else {
			s.NoUsable++
		}
	}
	return s
}

// printInitSummaryPretty writes a human-readable summary to w.
func printInitSummaryPretty(w io.Writer, s initSummary) {
	fmt.Fprintf(w, "Wrote: %s\n\n", s.Destination)

	if len(s.AddedSources) > 0 {
		fmt.Fprintf(w, "Added sources (%d):\n", len(s.AddedSources))
		for _, src := range s.AddedSources {
			fmt.Fprintf(w, "  • %s\n", src)
		}
		fmt.Fprintln(w)
	}

	if s.OK > 0 {
		fmt.Fprintf(w, "Chains with usable sources (%d):\n", s.OK)
		for _, c := range s.Chains {
			if len(c.UsableSources) > 0 {
				fmt.Fprintf(w, "  • %s  [%s]\n", c.Name, strings.Join(c.UsableSources, ", "))
			}
		}
		fmt.Fprintln(w)
	}

	if s.NoUsable > 0 {
		fmt.Fprintf(w, "Chains with no usable source (%d) — add a credential to enable:\n", s.NoUsable)
		for _, c := range s.Chains {
			if len(c.UsableSources) == 0 {
				fmt.Fprintf(w, "  • %s  (skipped: %s)\n", c.Name, strings.Join(c.SkippedSources, ", "))
			}
		}
	}
}
