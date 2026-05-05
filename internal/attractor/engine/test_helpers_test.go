// Shared test helpers used by integration tests in this package.
// Salvaged from provider_preflight_test.go when the legacy preflight
// machinery was deleted; the helpers themselves describe a fake CLI tool
// + minimal config + catalog, none of which is preflight-specific.

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// singleProviderDot generates a tiny DOT graph with a single agentic node
// using the given provider/model. Used by integration tests that exercise
// the engine end-to-end against a fake CLI.
func singleProviderDot(provider, modelID string) []byte {
	return []byte(fmt.Sprintf(`
digraph G {
  graph [goal="test"]
  start [shape=Mdiamond]
  a [shape=box, llm_provider="%s", llm_model="%s", prompt="x"]
  exit [shape=Msquare]
  start -> a
  a -> exit [condition="outcome=success"]
}
`, provider, modelID))
}

// testPreflightConfigForProviders builds a minimal RunConfigFile for the
// given (provider, backend) pairs using the test_shim CLI profile and
// the supplied pinned catalog path.
func testPreflightConfigForProviders(repo string, catalog string, providers map[string]BackendKind) *RunConfigFile {
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = "127.0.0.1:1"
	cfg.CXDB.HTTPBaseURL = "http://127.0.0.1:1"
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{}
	for provider, backend := range providers {
		cfg.LLM.Providers[provider] = ProviderConfig{Backend: backend}
	}
	cfg.Git.RunBranchPrefix = "attractor/run"
	return cfg
}

// writeCatalogForPreflight writes a JSON catalog blob to a temp file and
// returns its path. Name is historical; it's just a catalog file these
// days, no preflight involvement.
func writeCatalogForPreflight(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	return p
}

// writeFakeCLI writes a small bash script that responds to --help with
// the given output and exit code. Returns the path to the script.
func writeFakeCLI(t *testing.T, name string, helpOutput string, helpExit int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "exec" && "${2:-}" == "--help" ]]; then
cat <<'EOF'
%s
EOF
exit %d
fi
echo "ok"
`, helpOutput, helpExit)
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cli: %v", err)
	}
	return p
}
