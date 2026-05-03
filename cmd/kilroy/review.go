package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/review"
)

func attractorReview(args []string) {
	var graphPath string
	var outputPath string
	var jsonOutput bool
	var maxTurns int

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			fmt.Fprintln(os.Stderr, "usage:")
			fmt.Fprintln(os.Stderr, "  kilroy review --graph <file.dot> [--output <file> | -o <file>] [--json] [--max-turns <n>]")
			os.Exit(0)
		case "--graph":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--graph requires a value")
				os.Exit(1)
			}
			graphPath = args[i]
		case "--output", "-o":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--output requires a value")
				os.Exit(1)
			}
			outputPath = args[i]
		case "--json":
			jsonOutput = true
		case "--max-turns":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--max-turns requires a value")
				os.Exit(1)
			}
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 1 {
				fmt.Fprintln(os.Stderr, "--max-turns must be a positive integer")
				os.Exit(1)
			}
			maxTurns = n
		default:
			fmt.Fprintf(os.Stderr, "unknown arg: %s\n", args[i])
			os.Exit(1)
		}
	}

	if graphPath == "" {
		usage()
		os.Exit(1)
	}

	repoPath, _ := os.Getwd()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rep, err := review.Run(ctx, review.Options{
		GraphPath: graphPath,
		RepoPath:  repoPath,
		MaxTurns:  maxTurns,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var output string
	if jsonOutput {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, "json encode:", err)
			os.Exit(1)
		}
		output = string(b) + "\n"
	} else {
		output = rep.Markdown()
	}

	if outputPath != "" {
		if err := os.WriteFile(outputPath, []byte(output), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", outputPath)
	} else {
		fmt.Print(output)
	}
}
