// Acceptance tests for the unified Dispatcher. Verifies that DOT/policy
// intent — not a CLI flag — determines which path runs. Resolver-level
// behavior (agent_tool/llm_provider/agent_class) is covered in
// internal/attractor/engine/agent_route_test.go; the tests here lock in
// the dispatcher's path-mapping table and the deterministic-failure
// surface for unsupported drivers.
package agents

import (
	"testing"
)

// dispatchPathForDriver maps the canonical CLI drivers to dispatchCLI
// and the canonical SDK drivers to dispatchAPI. Anything else is
// dispatchUnknown (deterministic failure at Execute time).
func TestDispatchPathForDriver_Mapping(t *testing.T) {
	cliDrivers := []string{"claude_cli", "codex_cli", "gemini_cli", "opencode"}
	for _, d := range cliDrivers {
		t.Run("cli/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchCLI {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchCLI(%d)", d, got, dispatchCLI)
			}
		})
	}

	sdkDrivers := []string{"anthropic_sdk", "openai_sdk", "google_sdk"}
	for _, d := range sdkDrivers {
		t.Run("api/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchAPI {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchAPI(%d)", d, got, dispatchAPI)
			}
		})
	}

	unknown := []string{"", "claude", "openai", "unknown_driver", "claude_sdk"}
	for _, d := range unknown {
		t.Run("unknown/"+d, func(t *testing.T) {
			if got := dispatchPathForDriver(d); got != dispatchUnknown {
				t.Fatalf("driver %q: got dispatch path %d, want dispatchUnknown(%d)", d, got, dispatchUnknown)
			}
		})
	}
}
