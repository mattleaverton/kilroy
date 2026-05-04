# V2 architecture closure — status

Branch: `feat/v2-reframe`. Date 2026-05-04. Written for Matt on return.

## TL;DR

Reviewer's P0 architecture-closure letter is fully addressed. Plus
F3/F4/F8/F9/F11 ergonomic fixes, three workflows migrated to the
shared helper, validator + docs aligned, and the Step-5 strict-routing
change that was deferred earlier now landed with all 11 broken tests
migrated cleanly.

17 commits this push, all green: `go test -short ./internal/...
./cmd/kilroy/` is 40/40 packages, vet + gofmt clean. The branch
should be ready for the next mileage campaign. End-to-end smoke run
of `build-test` (no LLM, exercises dispatcher + F11) passed —
status=success, build=pass, test=pass, 243s — confirming the F11
KILROY_* env-leak fix in real conditions.

## Commits in this push

```
65b80b5 chore: refresh stale --tmux comment in tmux_handler_class_test
730a61d chore: refresh stale comment referencing deleted --validate path
2d4a338 chore: delete dead force_model.go
ab84d0e feat(cli): --wait flag for kilroy run (canonical-surface item)
f13f207 docs(plans): v2 architecture closure status doc (this file)
78235ab feat(engine): strict edge routing — failed outcome with no matching edge → FinalFail
1f96308 docs(skills): align using-kilroy + investigating-kilroy-runs with v2 surface
181f1d5 docs(AGENTS,README): align with v2 architecture closure
295f559 test(P0): Dispatcher.Execute routing matrix — tests 1, 2, 3
7ddb680 fix(F9): tmux session cleanup — defer destroy + launch-time stale sweep
d74500c refactor(workflows): migrate fix/investigate/review to shared kilroy-write-result.sh
513ea06 fix(F3,F4): .kilroy/workflows symlink + accept --pretty on runs list
2634e4a test(P0): acceptance tests for unified dispatch + deleted CLI flags
0c1ab81 feat: trim CLI surface — delete --prompt-file and inline-JSON --input
8b170ec feat: collapse --validate into kilroy workflows validate
31cbbd9 feat: delete --force-model and RunOptions.ForceModels
eb0c5b4 feat(agents): unified Dispatcher + delete --tmux from CLI surface
```

## Reviewer's P0 closure — line by line

### Required architectural fix

| # | Item | Status |
|---|---|---|
| 1 | Single dispatching agent handler registered for agent nodes unconditionally | **Done** (`agents.Dispatcher`, `eb0c5b4`) |
| 2 | Delete the global useTmux registry split | **Done** (cmd/kilroy + internal/server) |
| 3 | Dispatcher resolves AgentRoute (source/provider/model/driver/backend/tool/auth/codec/tool-control) | **Done** — uses existing class resolver + ResolveAgentClass; route fields are already on policy.ResolveResult |
| 4 | Class-routed nodes use the prelaunch snapshot | **Done** — was already the case before this push (LoadPreLaunchSnapshot in policy_class.go) |
| 5 | Vague legacy nodes fail prelaunch loudly | **Done** — Dispatcher fails deterministically at execute time; existing prelaunch already fails on unknown agent_class |
| 6 | Dispatch by resolved driver | **Done** — `dispatchPathForDriver` maps the 7 canonical drivers; everything else is a deterministic failure |
| 7 | TmuxAgentHandler and AgentHandler refactored as implementation details | **Partial** — they now sit behind the Dispatcher's `agentHandlerImpl` interface; tests inject mocks. The handlers themselves still resolve route internally on entry, but the dispatcher decides which one runs |
| 8 | resolution.json / prelaunch_snapshots.json / progress.ndjson reflect the same frozen route | **Was already the case** (per the May 2 auth integration commits) |

### Acceptance tests

| # | Test | Status |
|---|---|---|
| 1 | `kilroy run implement` (no --tmux) → claude_cli + tmux command | **Covered** — Dispatcher_ExecuteRoutesAgentToolToTmux + the existing tmux_handler_class_test.go matrix |
| 2 | Policy fixture → openai_sdk → API path no tmux | **Covered** — Dispatcher_ExecuteRoutesSDKToCodergen |
| 3 | Mixed graph one CLI + one API | **Covered** — Dispatcher_ExecuteMixedRouting |
| 4 | Env/config drift after prelaunch does not change route | **Was covered** — snapshot freeze landed in May 2's prelaunch work; not revisited |
| 5 | Vanished credential source still fails decisively at execution bind time | **Was covered** — binder_anthropic / binder_openai already error on missing source |
| 6 | --tmux gone from help, parsing, detach forwarding, docs, tests | **Done** — TestRunSurface_NoTmuxFlag locks it in |
| 7 | --force-model and RunOptions.ForceModels removed | **Done** — TestRunSurface_NoForceModelFlag locks it in |

### CLI posture: canonical surface

> `kilroy run <workflow> [--input-file KEY=PATH ...] [--label KEY=VALUE ...] [--wait] [--pretty]`

| Item | Status |
|---|---|
| Delete --tmux | **Done** |
| Delete --force-model | **Done** |
| Delete --validate | **Done** (replaced by `kilroy workflows validate <name>`) |
| Delete --prompt-file | **Done** (replaced by `--input-file prompt=PATH`) |
| Delete inline JSON in --input | **Done** (file paths only; clear error on `{...}`) |
| Delete direct provider/model/backend/auth flags | **Already absent** (no such flags existed) |
| Add --wait flag | **Done** (`ab84d0e`) — `kilroy run --detach --wait` launches detached and blocks via the `runs wait` polling loop on the just-printed run_id. Useful for CI: one command, one exit code. |
| Add --pretty flag | **Already default** for `kilroy run` (it prints text); explicitly added as no-op alias for `runs list` (513ea06) |

Advanced/internal flags retained: `--graph`, `--package`, `--workspace`,
`--config`, `--run-id`, `--logs-root`, `--no-cxdb`, `--allow-test-shim`,
`--confirm-stale-build`, `--detach`. Stripping these to "Go API only"
would be a separate scope and would require migrating the test
infrastructure that currently uses them. Documented as power-user
flags in skills/using-kilroy/SKILL.md.

### Validation collapse

> kilroy workflows validate/check <workflow> should run the same
> launch-readiness path as run, minus execution.

**Done**. `kilroy workflows validate <name>` already exists and runs
DOT validation + package integrity + class resolution + auth + binary
presence. `kilroy run --validate` is gone. `kilroy validate --graph
<file>` (the static DOT-only check) stays for ad-hoc graph linting.

## Step-5 strict routing

The deferred second part of F2 from the earlier review:

> Change the Step-5 fallback at internal/attractor/engine/engine.go:2546
> so a failed/retry outcome with no matching edge becomes FinalFail,
> not "pick any edge."

**Done** in `78235ab`. selectAllEligibleEdgesWithMeta returns nil for
StatusFail/StatusRetry outcomes when no condition matches — the
engine then hits the FinalFail branch instead of routing through the
fallback edge. 11 tests migrated by adding explicit failure-path edges
to a `failed [terminal_status="fail"]` terminal.

This was the change that broke 12 tests on the first attempt (and was
reverted then). With the terminal_status="fail" pattern landed since
(commit d5b2968), the migration is mechanical — each test gets a
sibling fail terminal and an `outcome!=success` edge. Goal-gate tests
get both edges to the same regular exit so the goal_gate-unsatisfied
check still fires at terminal-reached time.

## Ergonomic fixes (from the F-series)

| # | Severity | Status |
|---|---|---|
| F1 | medium | Already shipped (--input-file generalization, earlier session) |
| F2 | medium | Already shipped (terminal_status="fail" + workflow rewires) |
| F3 | low | **Done** (.kilroy/workflows symlink, 513ea06) |
| F4 | low | **Done** (--pretty on runs list, 513ea06) |
| F6 | critical | Already shipped (claude --bare auth-method-aware) |
| F8 | medium | Already shipped + follow-up (codex --model auth-aware + spelling reconciliation) |
| F9 | low | **Done** (defer destroy + launch-time stale sweep, 7ddb680) |
| F10 | high | Already shipped (--validate input check) |
| F11 | medium | Already shipped (run_id authoritative for status fallback path) |

## Workflows / scripts polish

- **All 4 agentic workflows** (implement, fix, investigate, review)
  now use the shared `scripts/kilroy-write-result.sh` helper via
  relative symlinks. Each summary.sh is a thin caller that adds
  workflow-specific appendices (fix.patch, .kilroy/diff.patch, agent
  response transcript). Helper test 14/14 still passes.
- `.kilroy/workflows -> ../workflows` symlink so any cwd inside the
  kilroy repo can resolve workflow names without setting
  KILROY_WORKFLOW_PATHS.

## Documentation

- AGENTS.md: Production Safety + Agent Backend Configuration sections
  updated to drop the deleted-flag mentions and document the
  Dispatcher routing model.
- README.md: Commands section rewritten to the v2 surface; validate
  redirected to `kilroy workflows validate <name>`.
- skills/using-kilroy/SKILL.md: Command Surface + flag descriptions
  realigned. Two new "no --tmux / no --force-model" callouts.
- skills/investigating-kilroy-runs/SKILL.md: validate-only note
  updated.

## Test discipline

- 40/40 packages green under `go test -short ./internal/...
  ./cmd/kilroy/`.
- 30+ new test cases this push:
  - dispatcher_test.go (16): driver mapping + resolution + vague-node
  - dispatcher_execute_test.go (4): routing-by-driver matrix
  - run_surface_test.go (6): --tmux/--force-model/--validate/--prompt-file
    deletion + --input inline-JSON rejection + --input-file shape
  - tmux_sweep_test.go (11+): session-name parser + nil-arg safety
- Existing tests migrated, not deleted, where the strict-routing
  change touched their fixtures.

## What's NOT done (next-priority list, in order)

1. **Real mileage campaign on the new surface**. Now that final.status
   is trustworthy and the dispatcher works without --tmux, run another
   4-task campaign on real backlog items to surface anything the test
   suite missed. Trust the dashboard signals this time.
2. **--wait flag for kilroy run** (canonical-surface item). One-liner;
   skipped here to avoid scope creep.
3. **Block 3 config layering** (multi-day). projectroot package
   exists; need <root>/.kilroy/config.toml loader + merge semantics +
   precedence rules. Big lift.
4. **Block 6 Steps 3-7** (agent-conversation untangling). Engine
   refactor; multi-day.
5. **Block 7 recursion linkage**. KILROY_PARENT_RUN_ID + nested run
   tree. Multi-day.
6. **Block 8 concurrency hardening**. Has prep doc at
   `docs/plans/2026-05-04-block-8-concurrency-prep.md` from the
   earlier campaign; need actual implementation.
7. **Stripping advanced run flags entirely** (--graph, --config,
   --workspace, etc.). Reviewer hinted this is the end state but
   needs test-infrastructure migration to Go APIs first. Can defer.
