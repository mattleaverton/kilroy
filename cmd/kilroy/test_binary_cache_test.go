package main

import (
	"os"
	"testing"
)

func TestBuildKilroyBinary_CachesDefaultBuild(t *testing.T) {
	first := buildKilroyBinary(t)
	second := buildKilroyBinary(t)

	if first != second {
		t.Fatalf("default test binary path should be cached: first=%q second=%q", first, second)
	}
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("cached test binary should exist: %v", err)
	}
}
