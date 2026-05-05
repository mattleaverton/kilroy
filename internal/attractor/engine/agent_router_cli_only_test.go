package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/model"
)

func TestCLIOnlyModelOverride_SwitchesBackendAndWarns(t *testing.T) {
	orig := cliOnlyModelIDs
	cliOnlyModelIDs = map[string]bool{
		"test-cli-only-model": true,
	}
	t.Cleanup(func() {
		cliOnlyModelIDs = orig
	})

	// Set up router with openai configured as API backend.
	runtimes := map[string]ProviderRuntime{
		"openai": {Key: "openai", Backend: BackendAPI},
	}
	router := NewAgentRouterWithRuntimes(nil, runtimes)

	// Confirm baseline: openai backend is API.
	if got := router.backendForProvider("openai"); got != BackendAPI {
		t.Fatalf("backendForProvider(openai) = %q, want %q", got, BackendAPI)
	}

	// Create a node using the CLI-only model.
	node := model.NewNode("spark-test")
	node.Attrs["llm_provider"] = "openai"
	node.Attrs["llm_model"] = "test-cli-only-model"
	node.Attrs["shape"] = "box"

	// Create an execution with temp dirs to isolate artifacts and an Engine
	// to capture warnings.
	eng := &Engine{}
	exec := &Execution{
		Engine:      eng,
		LogsRoot:    t.TempDir(),
		WorktreeDir: t.TempDir(),
	}

	// Short timeout: we only need the override to fire (before runCLI tries
	// to invoke a real binary). Prevents stalls if codex is in PATH.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run will likely fail (no real CLI binary), but the override should fire
	// first. We don't check the error — runCLI may return a failure outcome
	// instead of an error.
	route, err := ResolveAgentRoute(node, exec, PolicyDeps{})
	if err != nil {
		t.Fatalf("ResolveAgentRoute: %v", err)
	}
	_, _, _ = router.Run(ctx, exec, node, "test prompt", route)

	// Verify the CLI-only override warning was emitted.
	found := false
	for _, w := range eng.Warnings {
		if strings.Contains(w, "cli-only model override") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected warning containing 'cli-only model override', got warnings: %v", eng.Warnings)
	}
}

func TestCLIOnlyModelOverride_RegularModelNoOverride(t *testing.T) {
	requireIntegration(t)
	// Set up router with openai configured as API backend.
	runtimes := map[string]ProviderRuntime{
		"openai": {Key: "openai", Backend: BackendAPI},
	}
	router := NewAgentRouterWithRuntimes(nil, runtimes)

	// Create a node using a regular (non-CLI-only) model.
	node := model.NewNode("regular-test")
	node.Attrs["llm_provider"] = "openai"
	node.Attrs["llm_model"] = "gpt-5.4"
	node.Attrs["shape"] = "box"

	eng := &Engine{}
	exec := &Execution{
		Engine:      eng,
		LogsRoot:    t.TempDir(),
		WorktreeDir: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run will fail (no API client), but no CLI-only override should fire.
	route, err := ResolveAgentRoute(node, exec, PolicyDeps{})
	if err != nil {
		t.Fatalf("ResolveAgentRoute: %v", err)
	}
	_, _, _ = router.Run(ctx, exec, node, "test prompt", route)

	for _, w := range eng.Warnings {
		if strings.Contains(w, "cli-only model override") {
			t.Errorf("unexpected CLI-only override warning for regular model: %s", w)
		}
	}
}
