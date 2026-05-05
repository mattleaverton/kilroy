# Alpha — Foundation Honesty

Date: 2026-05-05

## Why this plan exists

The kilroy codebase has three structural truthfulness gaps:

1. A parallel agent-backend abstraction was built and tested but never made load-bearing. The live dispatch path uses a backdoor method that bypasses the abstraction. Two interfaces describe the same surface; only one is exercised at runtime.
2. Layered configuration support is partially shipped. Whether the precedence stack, project-root resolution, and CLI flag wiring are all complete and consistent is unverified.
3. Sub-run bookkeeping commits (verify, summary, status, done) extend the agent's working branch, so the branch tip is no longer the work commit. Pushing the branch directly publishes housekeeping noise.

This plan closes all three. After it lands, the code matches what the docs say it is, and every downstream feature is built on a foundation that doesn't shift under it.

The plan is one coherent push. Until α is closed, no other work starts that touches dispatch, config loading, or the run integration surface — those features would otherwise be built twice.

---

## Workstream 1 — Agent dispatch consolidation

### Goal

The live agent dispatch path uses a single interface (`AgentBackend.StartTurn`) and a single orchestration loop (`RunTurn`). The credential-resolution concern at the agent layer is mediated by `AuthResolver` for every transport. The legacy backdoor method `NativeExecuteAgent` is deleted, and every agent backend implementation is reachable from `kilroy run` via a routing decision.

### Current state

- `AgentBackend` interface lives at `internal/attractor/agentbackend/agentbackend.go`.
- `SDKBackend`, `TmuxBackend`, `OllamaBackend`, `CLIBackend` all implement `AgentBackend`. Each also exposes `NativeExecuteAgent`, which delegates directly to the underlying handler.
- `internal/attractor/agents/dispatcher.go` calls `NativeExecuteAgent` (the live path).
- `internal/attractor/agents/loop/control.go` provides `RunTurn` (~412 LOC). Zero non-test callers.
- `internal/attractor/agents/auth.AuthResolver` and `BindingAuthResolver` exist. Only `OllamaBackend` uses them. `SDKBackend` and `TmuxBackend` still call binding helpers directly.
- `OllamaBackend` and `CLIBackend` are not reachable from `kilroy run` — they are exercised only by their own tests.
- Engine call sites (`escalation_test.go`, `parallel_handlers.go`, `resume.go`) reference an engine-package internal `AgentBackend` type that is distinct from the new abstraction.

### Steps

1. **AuthResolver into transports.** Replace direct binding-helper calls in `SDKBackend` and `TmuxBackend` with `AuthResolver.ResolveCredential`. Construct each backend with its resolver injected. Confirm the route-frozen-by-prelaunch-snapshot invariant holds through the abstraction (the snapshot remains authoritative; the resolver re-reads source values at execution but never re-resolves identity).
2. **RunTurn into engine.** Replace the inlined turn orchestration in `engine.go` with `RunTurn(ctx, backend, msg, opts, handler, cfg)`. Carry observability hooks through the `TurnEvent` stream so existing CXDB emission and stage logging continue.
3. **Dispatcher cutover.** Change `dispatcher.go` so it (a) selects an `AgentBackend` implementation by resolved driver/transport (SDK / Tmux / CLI / Ollama), (b) constructs that backend with the prelaunch-snapshot route and an `AuthResolver`, and (c) hands the turn to `RunTurn`. Remove `NativeExecuteAgent` from the call sites.
4. **Engine internal call-site migration.** `escalation_test.go`, `parallel_handlers.go`, `resume.go` move onto the new seam. The engine-package internal `AgentBackend` either becomes an alias for the new interface or is deleted in favor of it.
5. **Make Ollama reachable.** Add provider/driver routing so a workflow can target Ollama via class or explicit declaration. Verify `kilroy run` end-to-end against a small Ollama model.
6. **Make CLIBackend reachable.** Same shape: routing that picks `CLIBackend` for cli driver paths. Replace the wrapped tmux handler invocation for cli driver routes.
7. **Delete the backdoor.** Remove `NativeExecuteAgent` from the `AgentBackend` interface and from all adapters. Remove dead code in the engine.
8. **Test cascade.** Engine package tests green. Every shipped workflow runs end-to-end. The full `kilroy run review` / `kilroy run implement` cycle exercises the new path against real CLI and SDK backends.

### Verification

- `grep -rn "NativeExecuteAgent" --include="*.go"` returns zero hits outside historical comments.
- `RunTurn` has at least one non-test caller in `internal/attractor/engine/`.
- `AuthResolver` is referenced from the live `SDKBackend` and `TmuxBackend` paths.
- A real `kilroy run` against an Ollama model succeeds end-to-end.
- A real `kilroy run` against a claude_cli class succeeds and runs through `CLIBackend`, not the legacy tmux handler wrapper.
- The engine package test suite passes; all shipped workflow integration tests pass.

---

## Workstream 2 — Layered configuration

### Goal

Configuration loads from a clean precedence stack — built-in defaults < user config < project config < environment variable < CLI flag — with documented project-root resolution. The CLI flag surface is consistent with the layered model: any flag duplicating a config key wins at the highest precedence and the resolved source is logged at preflight.

### Current state

Configuration loading exists. The shape and completeness of the layering, project-root resolution, and CLI wiring are unverified. Initial code searches for layered-config keywords (`LayeredConfig`, `ConfigLayer`, `LoadLayered`) returned zero hits in non-test code, suggesting either it is wired under different names or it is incomplete.

This workstream begins with an audit to establish what exists and what is missing.

### Steps

1. **Audit.** Read the existing config-loading path end-to-end. Document the actual precedence order, where each layer reads from, and what keys each layer can set. Output: a one-page status describing what exists, what is missing, and what is wired under unexpected names. Update this plan with the audit findings before continuing.
2. **Project-root marker and upward search.** Confirm or implement: the loader walks upward from the working directory and stops at the first directory containing a `.kilroy/` marker, or at the repository root if no marker exists. The chosen project root is exposed at preflight.
3. **Environment override.** Confirm or implement: `KILROY_PROJECT_ROOT` is authoritative when set. If invalid (path doesn't exist, isn't a directory, isn't readable), the CLI fails loudly with a useful error rather than silently falling back.
4. **CLI wiring consistency.** Each config-bearing CLI flag overrides the corresponding config layer. The resolved value and its source are reported in preflight output (when foreground) and in the run manifest.
5. **Pitfall coverage.** Add tests for: symlinked working directories, project root inside a git submodule, project root above a workspace, malformed `.kilroy/` directory, conflicting keys across layers, missing files at each layer, and empty-string vs unset env vars.

### Verification

- A documented precedence order from defaults through CLI flag.
- Tests exercising each layer's contribution and override behavior.
- Tests covering project-root resolution edge cases.
- A failing config (bad path, malformed file, conflicting keys) produces an error message that names the offending source.
- Preflight output shows the resolved project root and the source of each load-bearing config value.

---

## Workstream 3 — Run isolation and housekeeping commits

### Goal

A run's bookkeeping commits do not extend the agent's work branch. The agent's actual work commit is identifiable by a stable reference. Pushing the work-branch tip publishes the work commit only, with no housekeeping noise.

### Current state

When an agent worker creates a branch and commits its work, subsequent housekeeping nodes in the workflow (verify, summary, status, done) commit on top of that same branch. The branch tip becomes the last housekeeping commit. The work commit is identifiable only by its hash, which a human extracts manually before publishing.

### Steps

1. **Pin the design.** Two viable shapes:
   - (a) Housekeeping commits go to a dedicated reference per run (e.g. `attractor/housekeeping/<run-id>`) that is separate from the work branch. The work branch ends at the agent's last work commit.
   - (b) Housekeeping nodes do not commit at all. They write their artifacts (verify output, summary, status JSON) to disk under the run's logs directory. The run output exposes both the work commit hash and the artifacts.

   Both work. (b) is simpler and aligns with how stage outputs already work; (a) preserves git-archeology of housekeeping. Pick (b) unless there is a strong reason to retain housekeeping commits.

2. **Implement the chosen shape.** Update the engine's stage runner so housekeeping nodes follow the new contract. Update the `kilroy-write-result.sh` and equivalent helpers in shipped workflows to write artifacts to disk rather than committing.

3. **Update shipped workflows.** Each housekeeping stage in `implement`, `fix`, `review`, `coding-relay`, `coding-loop`, `investigate`, `multi-tool-exercise`, `build-test` switches to the new mechanism. No more "checkout branch, commit, exit" idiom in housekeeping nodes.

4. **Surface the work commit.** `kilroy runs show <id>` reports the work commit hash explicitly, distinct from any housekeeping references and artifacts.

5. **Director-side cleanup.** The PR-triage and integration playbook updates so cherry-picking the work-branch tip is the standard "land this run's work" step. No more hand-extracting work commits by hash.

### Verification

- A run of any shipped workflow leaves the work-branch tip equal to the agent's actual work commit.
- `kilroy runs show <id>` reports the work commit hash explicitly.
- Cherry-picking the work-branch tip onto the integration branch produces a clean integration with no housekeeping artifacts in the diff.
- Existing director / integration paths continue to work (with a small simplification once the manual hash-extraction step is no longer needed).

---

## Cross-workstream concerns

**Test surface.** All three workstreams change load-bearing code. The full engine-package test suite must remain runnable throughout the work. Workstream 1 in particular touches the dispatch path heavily, and intermediate states will produce broken tests; sequence the cutover so each landing point leaves the suite green.

**Ordering inside α.** Workstream 2 is mostly orthogonal and can run in parallel with Workstream 1 as a verification pass. Workstream 1 and Workstream 3 interact at the run integration surface — Workstream 3's housekeeping change should land after Workstream 1's dispatcher cutover, since a clean dispatch path simplifies reasoning about where housekeeping commits originate.

**Documentation.** `AGENTS.md` and `README.md` describe the agent-backend abstraction, layered config, and run integration model. After this plan lands, audit those docs against reality and correct any drift.

**Status hygiene.** The codebase has accumulated tasks and notes that claim work is complete when it is scaffolding-only. As each step in this plan lands, update any tracker entries that referred to the underlying work — accuracy in the status surface is part of the deliverable.

---

## Definition of done

- The live dispatch path runs through `AgentBackend.StartTurn` and `RunTurn`. The `NativeExecuteAgent` backdoor is deleted.
- `OllamaBackend` and `CLIBackend` are reachable from `kilroy run` and exercised by integration tests.
- `AuthResolver` is the credential-resolution path for every agent transport.
- Configuration precedence is documented, tested, and consistent across all layers.
- A run's work-branch tip is the agent's actual work commit, with housekeeping artifacts living separately.
- The engine package test suite is green.
- A real-world mileage run (multiple parallel workers reviewing real PRs) succeeds end-to-end on the new foundation.
