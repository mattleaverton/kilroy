package main

import (
	"fmt"
	"os"

	"github.com/danshapiro/kilroy/internal/server"
)

func attractorServe(args []string) {
	addr := "127.0.0.1:8080"

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			fmt.Fprintln(os.Stderr, "usage:")
			fmt.Fprintln(os.Stderr, "  kilroy serve [--addr <host:port>]    (default 127.0.0.1:8080)")
			os.Exit(0)
		case "--addr":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--addr requires a value")
				os.Exit(1)
			}
			addr = args[i]
		default:
			fmt.Fprintf(os.Stderr, "unknown arg: %s\n", args[i])
			os.Exit(1)
		}
	}

	srv := server.New(server.Config{
		Addr: addr,
	})

	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
