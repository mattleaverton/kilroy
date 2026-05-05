//go:build !windows

package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAnthropicCLIContract_InvocationArtifactIncludesStreamJSONAndVerbose(t *testing.T) {
	repo := initTestRepo(t)
	logsRoot := t.TempDir()
	cxdbSrv := newCXDBTestServer(t)

	cli := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(cli, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
cat <<'EOF'
Usage: claude -p --dangerously-skip-permissions --output-format stream-json --verbose --model MODEL
EOF
exit 0
fi
cat > status.json <<'JSON'
{"status":"success","notes":"ok"}
JSON
echo '{"type":"done","text":"ok"}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &RunConfigFile{Version: 1}
	cfg.Repo.Path = repo
	cfg.CXDB.BinaryAddr = cxdbSrv.BinaryAddr()
	cfg.CXDB.HTTPBaseURL = cxdbSrv.URL()
	cfg.LLM.CLIProfile = "test_shim"
	cfg.LLM.Providers = map[string]ProviderConfig{
		"anthropic": {Backend: BackendCLI, Executable: cli},
	}
	cfg.Git.RunBranchPrefix = "attractor/run"

	dot := singleProviderDot("anthropic", "claude-sonnet-4-20250514")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := RunWithConfig(ctx, dot, cfg, RunOptions{RunID: "anthropic-contract-ok", LogsRoot: logsRoot, AllowTestShim: true})
	if err != nil {
		t.Fatalf("RunWithConfig: %v", err)
	}

	invPath := filepath.Join(res.LogsRoot, "a", "cli_invocation.json")
	b, err := os.ReadFile(invPath)
	if err != nil {
		t.Fatalf("read %s: %v", invPath, err)
	}
	var inv map[string]any
	if err := json.Unmarshal(b, &inv); err != nil {
		t.Fatalf("decode %s: %v", invPath, err)
	}
	if strings.TrimSpace(anyToString(inv["provider"])) != "anthropic" {
		t.Fatalf("provider: got %q want %q", anyToString(inv["provider"]), "anthropic")
	}
	argvAny, ok := inv["argv"].([]any)
	if !ok {
		t.Fatalf("argv missing/invalid in invocation: %#v", inv["argv"])
	}
	argv := make([]string, 0, len(argvAny))
	for _, v := range argvAny {
		argv = append(argv, strings.TrimSpace(anyToString(v)))
	}
	if !hasArg(argv, "--output-format") || !hasArg(argv, "stream-json") {
		t.Fatalf("expected stream-json contract flags in argv, got %v", argv)
	}
	if !hasArg(argv, "--verbose") {
		t.Fatalf("expected --verbose for anthropic stream-json contract, got %v", argv)
	}
}

// (Removed) TestAnthropicCLIContract_PreflightFailsWhenVerboseCapabilityMissing
// asserted the legacy preflight's per-provider capability-token gate
// (rejecting `claude` if `claude --help` didn't list `--verbose`). That
// gate was deleted along with the rest of the legacy preflight; the
// replacement is the broader CLI capability probe in
// internal/attractor/engine/prelaunch.go (binary on PATH + responds 0
// to --help) plus runtime error classification in
// provider_error_classification.go (`"unknown option"` in stderr maps
// to providerCLIErrorKindCapabilityMissing).
