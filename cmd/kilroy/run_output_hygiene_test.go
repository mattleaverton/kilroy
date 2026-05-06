package main

import (
	"os"
	"testing"
)

func TestNoGeneratedRunArtifactsInCmdPackage(t *testing.T) {
	for _, name := range []string{"result.md", "E2E_MARKER.md"} {
		if _, err := os.Stat(name); err == nil {
			t.Fatalf("generated run artifact %q is present in cmd/kilroy", name)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %q: %v", name, err)
		}
	}
}
