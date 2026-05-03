package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/modeldb"
)

func attractorModelDB(args []string) {
	if len(args) == 0 {
		modeldbUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "-h", "--help", "help":
		modeldbUsage()
		os.Exit(0)
	case "suggest":
		attractorModelDBSuggest(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown modeldb subcommand: %s\n", args[0])
		modeldbUsage()
		os.Exit(1)
	}
}

func modeldbUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  kilroy modeldb suggest [--refresh] [--ttl <duration>] [--provider <name>]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  --refresh           force a fresh fetch (ignore cache)")
	fmt.Fprintln(os.Stderr, "  --ttl <duration>    cache lifetime (e.g. 1h, 24h)")
	fmt.Fprintln(os.Stderr, "  --provider <name>   limit to one provider (repeatable)")
}

func attractorModelDBSuggest(args []string) {
	opts := modeldb.SuggestOptions{}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			modeldbUsage()
			os.Exit(0)
		case "--refresh":
			opts.ForceRefresh = true
		case "--ttl":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--ttl requires a duration value (e.g. 1h, 24h)")
				os.Exit(1)
			}
			d, err := time.ParseDuration(args[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "--ttl: invalid duration %q: %v\n", args[i], err)
				os.Exit(1)
			}
			opts.TTL = d
		case "--provider":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--provider requires a value")
				os.Exit(1)
			}
			opts.Providers = append(opts.Providers, strings.ToLower(args[i]))
		default:
			fmt.Fprintf(os.Stderr, "unknown arg: %s\n", args[i])
			os.Exit(1)
		}
	}

	ctx, cleanup := signalCancelContext()
	defer cleanup()

	out, err := modeldb.Suggest(ctx, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(out)
}
