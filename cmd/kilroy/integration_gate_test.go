package main

import (
	"testing"

	"github.com/danshapiro/kilroy/internal/testutil"
)

func requireIntegration(t testing.TB) {
	t.Helper()
	testutil.RequireIntegration(t)
}
