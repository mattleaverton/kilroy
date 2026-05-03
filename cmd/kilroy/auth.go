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
	case "suggest-fix":
		authSuggestFix(args[1:])
	case "defaults":
		authDefaults(args[1:])
	case "init":
		authInitCmd(args[1:])
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
	fmt.Fprintln(os.Stderr, "  kilroy auth init [--force] [--path <dir>] [--json|--pretty]")
	fmt.Fprintln(os.Stderr, "  kilroy auth list [--pretty]")
	fmt.Fprintln(os.Stderr, "  kilroy auth suggest-fix [<provider>]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  defaults prints the default_chains.toml template verbatim.")
	fmt.Fprintln(os.Stderr, "  init     generates ~/.config/kilroy/auth.toml from the template.")
	fmt.Fprintln(os.Stderr, "  list     outputs JSON by default; pass --pretty for human-readable.")
}

func authList(args []string) {
	var pretty bool
	for _, a := range args {
		switch a {
		case "--pretty":
			pretty = true
		case "--json":
			// JSON is the default; flag is accepted for explicitness.
		case "-h", "--help":
			authUsage()
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
			os.Exit(1)
		}
	}

	out := auth.ListAll(version.Version, auth.DefaultDetectors())

	if pretty {
		printAuthListPretty(out)
		return
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
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
			fmt.Printf("    shadows: %s (env var wins at runtime)\n", strings.Join(e.Shadows, ", "))
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
