// `kilroy auth` subcommand: discover credentials on the local machine.
// Per plan §8: kilroy's auth surface is "discovery and routing, not storage."

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/version"
)

func authCmd(args []string) {
	if len(args) == 0 {
		authUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "list":
		authList(args[1:])
	case "check":
		authCheck(args[1:])
	case "suggest-fix":
		authSuggestFix(args[1:])
	case "defaults":
		authDefaults(args[1:])
	case "init":
		authInitCmd(args[1:])
	case "set":
		authSet(args[1:])
	case "prefer":
		authPrefer(args[1:])
	case "remove-source":
		authRemoveSource(args[1:])
	case "doctor":
		authCheck(args[1:])
	case "-h", "--help", "help":
		authUsage()
		os.Exit(0)
	default:
		authUsage()
		fmt.Fprintf(os.Stderr, "unknown auth subcommand: %q\n", args[0])
		os.Exit(1)
	}
}

func authUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy auth defaults")
	fmt.Fprintln(os.Stderr, "  kilroy auth init [--force|--rescan] [--path <dir>] [--json|--pretty]")
	fmt.Fprintln(os.Stderr, "  kilroy auth set <provider> --env <ENV_VAR>")
	fmt.Fprintln(os.Stderr, "  kilroy auth prefer <provider/method[/tool]> <ENV_VAR>")
	fmt.Fprintln(os.Stderr, "  kilroy auth remove-source <provider/method[/tool]> <ENV_VAR>")
	fmt.Fprintln(os.Stderr, "  kilroy auth list [--pretty] [--json] [--chains]")
	fmt.Fprintln(os.Stderr, "  kilroy auth check [--pretty] [--json] [--project <dir>]")
	fmt.Fprintln(os.Stderr, "  kilroy auth doctor [--pretty] [--json] [--project <dir>]")
	fmt.Fprintln(os.Stderr, "  kilroy auth suggest-fix [<provider>]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  defaults prints the default_chains.toml template verbatim.")
	fmt.Fprintln(os.Stderr, "  init     generates ~/.config/kilroy/auth.toml from the template.")
	fmt.Fprintln(os.Stderr, "  set      maps a provider's api_key chain to an env var in global auth.")
	fmt.Fprintln(os.Stderr, "  prefer   moves an env source to the front of a global auth chain.")
	fmt.Fprintln(os.Stderr, "  list     outputs JSON by default; pass --pretty for human-readable.")
	fmt.Fprintln(os.Stderr, "  list --chains pivots to a chain-centric view (one row per configured binding).")
	fmt.Fprintln(os.Stderr, "  check    runs the auth resolver for every configured binding and reports status.")
}

func authList(args []string) {
	var pretty bool
	var chains bool
	for _, a := range args {
		switch a {
		case "--pretty":
			pretty = true
		case "--json":
			// JSON is the default; flag is accepted for explicitness.
		case "--chains":
			chains = true
		case "-h", "--help":
			authUsage()
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
			os.Exit(1)
		}
	}

	out := auth.ListAll(version.Version, auth.DefaultDetectors())

	// Load auth config for annotation / chains view. Graceful: no crash on
	// absent or invalid config — annotation simply degrades.
	cfg, cfgErr := loadAuthConfig("")
	configState := ""
	if cfgErr != nil {
		configState = "uninitialized"
		cfg = nil
	}

	if chains {
		printAuthListChainsView(cfg, configState, out, pretty)
		return
	}

	annotated := annotateListEntries(out.Entries, cfg)

	if pretty {
		printAuthListPrettyAnnotated(out, annotated, configState)
		return
	}

	annotatedOut := annotatedListOutput{
		KilroyVersion: out.KilroyVersion,
		ScannedAt:     out.ScannedAt,
		Platform:      out.Platform,
		Entries:       annotated,
		Summary:       out.Summary,
		ConfigState:   configState,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(annotatedOut); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
		os.Exit(1)
	}
}

// printAuthListPretty renders a human-friendly table of the auth output.
func printAuthListPretty(out auth.ListOutput) {
	fmt.Printf("Kilroy auth scan — %s on %s\n\n", out.ScannedAt, out.Platform)

	if len(out.Entries) == 0 {
		fmt.Println("No credentials discovered. Set provider env vars or run a CLI tool's login flow.")
		return
	}

	for _, e := range out.Entries {
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

		// Source detail (one line, what's where).
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

		// Expiry, profiles, shadow notes.
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
			// Shadow note: in default tool invocation, env var wins. For
			// kilroy class-routed CLI runs, the binder scrubs the env var
			// so the CLI uses its OAuth session — see `kilroy auth check`
			// for the resolver-decided source per chain.
			fmt.Printf("    shadows: %s (env var wins for default invocations; class-routed CLI runs scrub env vars per binder)\n", strings.Join(e.Shadows, ", "))
		}
		if len(e.ShadowedBy) > 0 {
			fmt.Printf("    shadowed by: %s\n", strings.Join(e.ShadowedBy, ", "))
		}

		// Notes.
		for _, n := range e.Notes {
			fmt.Printf("    %s\n", n)
		}

		// Remediation for non-ok states.
		if e.State != auth.StateOK && e.Remediation != "" {
			fmt.Printf("    fix: %s\n", e.Remediation)
		}
		fmt.Println()
	}

	fmt.Printf("Summary: %d entries — %d ok, %d expired, %d missing, %d ambiguous\n",
		out.Summary.Total, out.Summary.OK, out.Summary.Expired,
		out.Summary.Missing, out.Summary.Ambiguous)
}

func stateMarker(s auth.State) string {
	switch s {
	case auth.StateOK:
		return "OK "
	case auth.StateExpired:
		return "EXP"
	case auth.StateMissing:
		return "---"
	case auth.StateAmbiguous:
		return "??"
	}
	return "  "
}

func identString(i auth.Identity) string {
	switch {
	case i.Email != "":
		return i.Email
	case i.User != "":
		return "@" + i.User
	case i.Org != "":
		return "org=" + i.Org
	case i.AccountID != "":
		return "acct=" + i.AccountID
	}
	return ""
}

func abbrevPath(p string) string {
	home := os.Getenv("HOME")
	if home != "" && strings.HasPrefix(p, home) {
		return "~" + p[len(home):]
	}
	return p
}

// authSuggestFix prints remediation for any non-ok entries (filtered by
// optional provider arg). Output is human-readable; agents should parse the
// `auth list --json` output's `remediation` field instead.
func authSuggestFix(args []string) {
	provider := ""
	if len(args) > 0 {
		provider = args[0]
	}
	out := auth.ListAll(version.Version, auth.DefaultDetectors())

	any := false
	for _, e := range out.Entries {
		if e.State == auth.StateOK {
			continue
		}
		if provider != "" && !strings.EqualFold(e.Provider, provider) {
			continue
		}
		any = true
		fmt.Printf("[%s] %s/%s: %s\n", e.State, e.Provider, e.Tool, e.Remediation)
		if e.Remediation == "" {
			fmt.Println("  (no remediation hint available)")
		}
	}
	if !any {
		if provider != "" {
			fmt.Printf("No fixable issues found for provider %q. (Run `kilroy auth list` to see all entries.)\n", provider)
		} else {
			fmt.Println("All discovered credentials are healthy. Nothing to fix.")
		}
	}
}
