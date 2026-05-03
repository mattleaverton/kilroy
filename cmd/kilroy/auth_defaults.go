// auth_defaults.go implements `kilroy auth defaults` (A8a).
// Prints the embedded default_chains.toml verbatim to stdout. No flags.

package main

import (
	"fmt"
	"os"

	"github.com/danshapiro/kilroy/internal/auth/binding"
)

func authDefaults(args []string) {
	for _, a := range args {
		switch a {
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: kilroy auth defaults")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  Prints the embedded default_chains.toml template verbatim to stdout.")
			fmt.Fprintln(os.Stderr, "  Use `kilroy auth init` to generate a personalised auth.toml from this template.")
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %q\n", a)
			os.Exit(1)
		}
	}
	if _, err := os.Stdout.Write(binding.DefaultChainsTOML()); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
}
