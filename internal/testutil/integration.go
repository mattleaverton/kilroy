package testutil

import (
	"os"
	"strings"
	"testing"
)

const IntegrationEnv = "KILROY_INTEGRATION"

func IntegrationEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(IntegrationEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func RequireIntegration(t testing.TB) {
	t.Helper()
	if !IntegrationEnabled() {
		t.Skipf("integration test skipped; set %s=1 to run", IntegrationEnv)
	}
}
