# Attractor Run Preflight Mode Implementation Plan

> **For Claude:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--preflight` mode (alias `--test-run`) to `kilroy attractor run` that executes all pre-run validations/preflights and exits without starting pipeline execution.

**Architecture:** Reuse the exact `RunWithConfig` pre-run pipeline by extracting a shared pre-execution bootstrap and a run-only execution bootstrap boundary. The shared path must include config defaulting/override merge (`applyDefaults`), parse/validate, provider/model policy checks, provider preflight, and CXDB readiness probing. The run-only path must contain execution-only side effects (registry bundle publish, context creation, sink wiring, engine allocation, traversal). Surface preflight-only mode through CLI flags that keep existing safety gates (`--confirm-stale-build`, CLI headless warning, `--allow-test-shim` policy, provider preflight, model catalog checks, CXDB readiness) while explicitly preventing run startup side effects (no run branch/worktree traversal, no node execution, no final status). Keep behavior deterministic by funneling both normal runs and preflight-only runs through one shared bootstrap with an explicit execution boundary.

**Tech Stack:** Go (`cmd/kilroy`, `internal/attractor/engine`), existing test harnesses (`cmd/kilroy/main_exit_codes_test.go`, `internal/attractor/engine/run_with_config*_test.go`), Markdown docs (`README.md`, `docs/strongdm/attractor/README.md`).

---

## Scope Check

This is one subsystem change: **run startup contract and CLI ergonomics for preflight-only execution**. It touches CLI parsing, engine bootstrap sequencing, and documentation, but all edits support one user-visible feature and one runtime contract.

## File Structure Map

### CLI Surface
- Modify: `cmd/kilroy/main.go`
  - Responsibility: parse `--preflight` and `--test-run`, enforce incompatible flag rules, and emit preflight-only output.
- Modify: `cmd/kilroy/main_exit_codes_test.go`
  - Responsibility: regression tests for flag parsing, usage text, and no-run side effects in preflight-only mode.

### Engine Bootstrap / Execution Boundary
- Modify: `internal/attractor/engine/run_with_config.go`
  - Responsibility: centralize shared pre-run checks and short-circuit before `eng.run()` when preflight-only mode is enabled.
- Create: `internal/attractor/engine/preflight_with_config.go`
  - Responsibility: expose explicit engine API (`PreflightWithConfig`) so CLI does not overload `RunWithConfig` semantics directly.
- Modify: `internal/attractor/engine/run_with_config_test.go`
  - Responsibility: unit tests for preflight-only short-circuit semantics and artifact expectations.
- Modify: `internal/attractor/engine/run_with_config_integration_test.go`
  - Responsibility: integration coverage proving preflight-only still runs real preflight checks (provider policy/CXDB readiness) while skipping run execution.

### User Docs / Runbook
- Modify: `README.md`
  - Responsibility: add preflight-only command examples and explain exactly what is validated vs what is intentionally not started.
- Modify: `docs/strongdm/attractor/README.md`
  - Responsibility: operational runbook note for preflight-only mode and expected artifacts (`preflight_report.json`, no run progression artifacts).

## Chunk 1: Engine Preflight-Only Contract

### Task 1: Add Engine-Level Preflight-Only API and Shared Bootstrap Path

**Files:**
- Create: `internal/attractor/engine/preflight_with_config.go`
- Modify: `internal/attractor/engine/run_with_config.go`
- Modify: `internal/attractor/engine/run_with_config_test.go`

- [ ] **Step 1: Write failing unit tests for preflight-only behavior**

```go
func TestPreflightWithConfig_SkipsRunExecutionArtifacts(t *testing.T) {
    // Arrange a minimal start->exit graph and valid run config.
    // Call PreflightWithConfig(...).
    // Assert: preflight_report.json exists.
    // Assert absent: final.json, checkpoint.json, manifest.json, run.pid.
    // Assert absent: logsRoot/worktree execution directory.
    // Assert git branch attractor/run/<run_id> was not created.
}

func TestPreflightWithConfig_ReturnsRunAndReportMetadata(t *testing.T) {
    // Assert returned metadata includes run_id/logs_root and preflight report path.
}

func TestPreflightWithConfig_StillEnforcesRunPolicyGates(t *testing.T) {
    // Example: test_shim profile without AllowTestShim still fails with policy error.
}
```

- [ ] **Step 2: Run targeted engine tests to confirm they fail first**

Run: `go test ./internal/attractor/engine -run 'TestPreflightWithConfig_' -count=1 -v`
Expected: FAIL (API/mode not implemented yet).

- [ ] **Step 3: Implement preflight-only mode using a shared bootstrap path**

```go
// preflight_with_config.go
type PreflightResult struct {
    RunID               string
    LogsRoot            string
    PreflightReportPath string
    Warnings            []string
    CXDBUIURL           string
}

// run_with_config.go
type runBootstrap struct {
    Graph    *model.Graph
    Dot      []byte
    Config   *RunConfigFile
    Options  RunOptions // applyDefaults already called; RunID/LogsRoot/WorktreeDir finalized
    Registry *HandlerRegistry
    Catalog  *modeldb.Catalog
    Runtimes map[string]ProviderRuntime
    CXDBClient *cxdb.Client
    CXDBBin    *cxdb.BinaryClient
    Startup  *CXDBStartupInfo
    Warnings []string
}

func bootstrapRunWithConfig(ctx context.Context, dotSource []byte, cfg *RunConfigFile, overrides RunOptions) (*runBootstrap, error) {
    // one source of truth for:
    // - applyConfigDefaults + override merge + opts.applyDefaults()
    // - parse/validate + provider/model policy + provider preflight report
    // - CXDB readiness (ensureCXDBReady) when DisableCXDB=false
    // this function must NOT publish registry bundle, create CXDB context/sink,
    // allocate Engine, or start traversal.
}

func RunWithConfig(ctx context.Context, dotSource []byte, cfg *RunConfigFile, overrides RunOptions) (*Result, error) {
    boot, err := bootstrapRunWithConfig(ctx, dotSource, cfg, overrides)
    if err != nil {
        return nil, err
    }
    // defer CXDB bin close + startup shutdown for all returns when readiness started processes.
    // run-only bootstrap: publish registry bundle, create context, build sink.
    // normal execution path: newBaseEngine(...), eng.run(...)
}

func PreflightWithConfig(ctx context.Context, dotSource []byte, cfg *RunConfigFile, overrides RunOptions) (*PreflightResult, error) {
    boot, err := bootstrapRunWithConfig(ctx, dotSource, cfg, overrides)
    if err != nil {
        return nil, err
    }
    // defer CXDB bin close + startup shutdown for all returns when readiness started processes.
    cxdbUI := ""
    if boot.Startup != nil {
        cxdbUI = strings.TrimSpace(boot.Startup.UIURL)
    }
    return &PreflightResult{
        RunID:               boot.Options.RunID,
        LogsRoot:            boot.Options.LogsRoot,
        PreflightReportPath: filepath.Join(boot.Options.LogsRoot, "preflight_report.json"),
        Warnings:            append([]string{}, boot.Warnings...),
        CXDBUIURL:           cxdbUI,
    }, nil
}
```

- [ ] **Step 4: Re-run targeted unit tests and verify pass**

Run: `go test ./internal/attractor/engine -run 'TestPreflightWithConfig_' -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit task changes**

```bash
git add internal/attractor/engine/preflight_with_config.go internal/attractor/engine/run_with_config.go internal/attractor/engine/run_with_config_test.go
git commit -m "feat(engine): add preflight-only RunWithConfig bootstrap path"
```

### Task 2: Prove Preflight-Only Runs Full Preflight Stack but Never Starts Pipeline

**Files:**
- Modify: `internal/attractor/engine/run_with_config_integration_test.go`
- Modify: `internal/attractor/engine/run_with_config_test.go`

- [ ] **Step 1: Add failing integration tests for full-preflight/no-execution contract**

```go
func TestPreflightWithConfig_RunsProviderChecksAndWritesReport(t *testing.T) {
    // Use a config that reaches provider preflight.
    // Assert preflight_report.json summary has pass/warn/fail counts populated.
}

func TestPreflightWithConfig_InitializesAndShutsDownCXDBWithoutRunStart(t *testing.T) {
    // Use cxdb test server.
    // Assert no final.json and no stage directories; only preflight artifacts exist.
}
```

- [ ] **Step 2: Run targeted integration tests and confirm fail-first**

Run: `go test ./internal/attractor/engine -run 'TestPreflightWithConfig_RunsProviderChecksAndWritesReport|TestPreflightWithConfig_InitializesAndShutsDownCXDBWithoutRunStart' -count=1 -v`
Expected: FAIL before implementation is complete.

- [ ] **Step 3: Implement explicit preflight-only short-circuit and side-effect guard assertions**

```go
// In bootstrapRunWithConfig / PreflightWithConfig path:
// - do NOT call newBaseEngine(...)
// - do NOT call eng.run(...)
// - ensure branch/worktree creation methods are unreachable in preflight-only mode.
// - shared bootstrap includes opts.applyDefaults() and CXDB readiness checks.
// - preflight path always closes CXDB binary client and shuts down managed startup/UI processes.
//
// In RunWithConfig-only path:
// - publish registry bundle, create context, and construct CXDB sink after shared bootstrap succeeds.
//
// In integration tests, assert matrix explicitly:
// present: preflight_report.json
// absent: final.json, checkpoint.json, manifest.json, run.pid, logsRoot/worktree
// absent: run branch name under refs/heads/<runBranchPrefix>/<runID>
//
// Keep defer-based cleanup for cxdb startup managed processes on all returns.
```

- [ ] **Step 4: Re-run targeted integration tests and ensure pass**

Run: `go test ./internal/attractor/engine -run 'TestPreflightWithConfig_RunsProviderChecksAndWritesReport|TestPreflightWithConfig_InitializesAndShutsDownCXDBWithoutRunStart' -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit task changes**

```bash
git add internal/attractor/engine/run_with_config_integration_test.go internal/attractor/engine/run_with_config_test.go internal/attractor/engine/run_with_config.go
git commit -m "test(engine): cover preflight-only full-preflight and no-execution behavior"
```

## Chunk 2: CLI Flags, UX, and Documentation

### Task 3: Add `--preflight` and `--test-run` CLI Mode for `attractor run`

**Files:**
- Modify: `cmd/kilroy/main.go`
- Modify: `cmd/kilroy/main_exit_codes_test.go`

- [ ] **Step 1: Add failing CLI tests for new flags and incompatible flag handling**

```go
func TestAttractorRun_PreflightFlag_Accepted(t *testing.T) {}
func TestAttractorRun_TestRunAlias_Accepted(t *testing.T) {}
func TestAttractorRun_PreflightRejectsDetach(t *testing.T) {}
func TestAttractorRun_PreflightOutput_IsPreflightOnlyMetadata(t *testing.T) {}
func TestAttractorRun_PreflightStillEnforcesTestShimGate(t *testing.T) {}
func TestAttractorRun_PreflightStillEnforcesStaleBuildGate(t *testing.T) {}
func TestUsage_IncludesPreflightFlags(t *testing.T) {}
```

- [ ] **Step 2: Run CLI tests to confirm expected failures**

Run: `go test ./cmd/kilroy -run 'TestAttractorRun_PreflightFlag_Accepted|TestAttractorRun_TestRunAlias_Accepted|TestAttractorRun_PreflightRejectsDetach|TestAttractorRun_PreflightOutput_IsPreflightOnlyMetadata|TestAttractorRun_PreflightStillEnforcesTestShimGate|TestAttractorRun_PreflightStillEnforcesStaleBuildGate|TestUsage_IncludesPreflightFlags' -count=1 -v`
Expected: FAIL (flags not recognized / usage not updated yet).

- [ ] **Step 3: Implement CLI mode parsing, routing, and output**

```go
// main.go attractorRun arg parsing:
case "--preflight":
    preflightOnly = true
case "--test-run":
    preflightOnly = true // alias

// Preserve existing safety gates before preflight execution:
// - stale-build guard (--confirm-stale-build)
// - CLI headless warning prompt and opt-out behavior
// - test_shim allow gate and provider executable policy
if preflightOnly && detach {
    fmt.Fprintln(os.Stderr, "--preflight/--test-run cannot be combined with --detach")
    os.Exit(1)
}

if preflightOnly {
    pf, err := engine.PreflightWithConfig(ctx, dotSource, cfg, engine.RunOptions{...})
    // print deterministic metadata for operators
    fmt.Printf("preflight=true\n")
    fmt.Printf("run_id=%s\n", pf.RunID)
    fmt.Printf("logs_root=%s\n", pf.LogsRoot)
    fmt.Printf("preflight_report=%s\n", pf.PreflightReportPath)
    if pf.CXDBUIURL != "" { fmt.Printf("cxdb_ui=%s\n", pf.CXDBUIURL) }
    // emit warnings to stderr like run path
    os.Exit(0)
}
```

- [ ] **Step 4: Re-run targeted CLI tests and verify pass**

Run: `go test ./cmd/kilroy -run 'TestAttractorRun_PreflightFlag_Accepted|TestAttractorRun_TestRunAlias_Accepted|TestAttractorRun_PreflightRejectsDetach|TestAttractorRun_PreflightOutput_IsPreflightOnlyMetadata|TestAttractorRun_PreflightStillEnforcesTestShimGate|TestAttractorRun_PreflightStillEnforcesStaleBuildGate|TestUsage_IncludesPreflightFlags' -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit task changes**

```bash
git add cmd/kilroy/main.go cmd/kilroy/main_exit_codes_test.go
git commit -m "feat(cli): add attractor run --preflight mode with --test-run alias"
```

### Task 4: Document Preflight-Only Mode and Operational Expectations

**Files:**
- Modify: `README.md`
- Modify: `docs/strongdm/attractor/README.md`

- [ ] **Step 1: Update docs with explicit preflight-only examples and contract**

```md
./kilroy attractor run --graph pipeline.dot --config run.yaml --preflight
./kilroy attractor run --graph pipeline.dot --config run.yaml --test-run

# Behavior:
# - --test-run is an alias of --preflight only (it does NOT bypass --allow-test-shim policy)
# - runs parse/validate + run-config validation + model-catalog/provider preflight + CXDB readiness checks
# - writes preflight_report.json
# - does not start traversal/node execution
# - absent by design: final.json, checkpoint.json, manifest.json, run.pid, run worktree, run branch traversal
```

- [ ] **Step 2: Run focused CLI usage/doc-adjacent tests**

Run: `go test ./cmd/kilroy -run 'TestUsage_IncludesPreflightFlags|TestAttractorRun_PreflightFlag_Accepted|TestAttractorRun_TestRunAlias_Accepted' -count=1 -v`
Expected: PASS.

- [ ] **Step 3: Commit doc updates**

```bash
git add README.md docs/strongdm/attractor/README.md
git commit -m "docs(runbook): document preflight-only run mode and alias"
```

## Chunk 3: Full Validation and Merge Readiness

### Task 5: End-to-End Verification and CI-Equivalent Checks

**Files:**
- Test: `cmd/kilroy/main_exit_codes_test.go`
- Test: `internal/attractor/engine/run_with_config_test.go`
- Test: `internal/attractor/engine/run_with_config_integration_test.go`

- [ ] **Step 1: Run targeted feature suites**

Run: `go test ./cmd/kilroy -run 'TestAttractorRun_Preflight|TestUsage_IncludesPreflightFlags' -count=1 -v`
Run: `go test ./internal/attractor/engine -run 'TestPreflightWithConfig_' -count=1 -v`
Expected: PASS.

- [ ] **Step 2: Run full repository quality gate**

Run: `gofmt -l . | grep -v '^\./\.claude/' | grep -v '^\.claude/'`
Expected: no output.

Run: `go vet ./...`
Expected: PASS.

Run: `go build ./cmd/kilroy/`
Expected: PASS.

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 3: Validate demo graphs with fresh binary**

Run:

```bash
go build -o ./kilroy ./cmd/kilroy
while IFS= read -r f; do
  echo "Validating $f"
  ./kilroy attractor validate --graph "$f"
done < <(find demo -type f -name '*.dot' | sort)
```

Expected: every validation command exits `0` and prints `ok: <file>` (any non-zero exit fails this step).

- [ ] **Step 4: Manual smoke for operator UX (@using-kilroy)**

Run:

```bash
set -euo pipefail
TMP_DIR="$(mktemp -d)"
cat > "$TMP_DIR/fake-codex" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "exec" && "${2:-}" == "--help" ]]; then
  echo "Usage: codex exec --json --sandbox workspace-write"
  exit 0
fi
echo '{"type":"done","text":"ok"}'
SH
chmod +x "$TMP_DIR/fake-codex"

cat > "$TMP_DIR/pipeline.dot" <<'DOT'
digraph G {
  graph [goal="preflight smoke"]
  start [shape=Mdiamond]
  exit  [shape=Msquare]
  a [shape=box, llm_provider=openai, llm_model=gpt-5.2-codex, prompt="hi"]
  start -> a -> exit
}
DOT

cat > "$TMP_DIR/run.yaml" <<YAML
version: 1
repo:
  path: $(pwd)
cxdb:
  binary_addr: 127.0.0.1:9
  http_base_url: http://127.0.0.1:9
llm:
  cli_profile: test_shim
  providers:
    openai:
      backend: cli
      executable: $TMP_DIR/fake-codex
modeldb:
  openrouter_model_info_path: $(pwd)/internal/attractor/modeldb/pinned/openrouter_models.json
  openrouter_model_info_update_policy: pinned
YAML

OUT1="$TMP_DIR/preflight.out"
OUT2="$TMP_DIR/test-run.out"
./kilroy attractor run --graph "$TMP_DIR/pipeline.dot" --config "$TMP_DIR/run.yaml" --allow-test-shim --no-cxdb --preflight >"$OUT1"
./kilroy attractor run --graph "$TMP_DIR/pipeline.dot" --config "$TMP_DIR/run.yaml" --allow-test-shim --no-cxdb --test-run >"$OUT2"
grep -q '^preflight=true$' "$OUT1"
grep -q '^preflight=true$' "$OUT2"
! grep -q '^worktree=' "$OUT1"
! grep -q '^run_branch=' "$OUT1"
! grep -q '^final_commit=' "$OUT1"
REPORT_PATH="$(awk -F= '/^preflight_report=/{print $2}' "$OUT1")"
jq -e . "$REPORT_PATH" >/dev/null
```

Expected:
- command exits before pipeline execution starts,
- stdout includes `preflight=true`, `run_id`, `logs_root`, `preflight_report`,
- stdout excludes run outputs `worktree`, `run_branch`, `final_commit`,
- emitted `preflight_report` path parses via `jq -e`,
- smoke uses `--no-cxdb` for deterministic local execution (CXDB preflight path is covered by engine tests above).

- [ ] **Step 5: Final commit**

```bash
git add \
  internal/attractor/engine/preflight_with_config.go \
  internal/attractor/engine/run_with_config.go \
  internal/attractor/engine/run_with_config_test.go \
  internal/attractor/engine/run_with_config_integration_test.go \
  cmd/kilroy/main.go \
  cmd/kilroy/main_exit_codes_test.go \
  README.md \
  docs/strongdm/attractor/README.md
git commit -m "test/ci: validate preflight-only run mode end-to-end"
```

## Risks and Guardrails

- Keep one bootstrap path for both run and preflight-only modes to prevent drift in what gets validated.
- Keep bootstrap boundaries explicit: shared bootstrap validates/preflights, run-only bootstrap performs execution side effects.
- Avoid partial duplicate logic in CLI and engine; CLI should route to engine API only.
- Do not weaken existing production/test-shim safety policy checks; preflight mode must enforce the same policy.
- Keep `--detach` rejected in preflight mode to avoid ambiguous “detached no-op run” behavior.

## Notes for Execution

- Assume alias means `--test-run` (flag form) for consistency with existing CLI options.
- If implementation discovers existing automation depending on `RunWithConfig` side effects before `eng.run`, preserve those side effects in normal mode and gate only the execution boundary for preflight-only mode.
- Reuse existing cxdb test server helpers to avoid flaky external dependencies.
