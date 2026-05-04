# Block 8 Concurrency Hardening — what should the stress test look like?

## TL;DR

The Block 8 stress test must exercise **per-run isolation under concurrent siblings** sharing five
classes of mutable surface: (1) the global SQLite run DB at `~/.local/state/kilroy/runs.db`, (2) the
parent git repo's `.git` (branch creation + worktree add), (3) the single shared tmux server socket
named `kilroy`, (4) the per-machine CLI tool sessions (`~/.claude/`, `~/.codex/`, `~/.config/...`)
when class resolution lands on `cli_session` sources, and (5) the optionally-shared CXDB
binary+HTTP service. Workspace allocation, log paths, run-IDs, run-ownership locks, the
per-run model-catalog snapshot, and the per-stage codex isolated home are all already keyed on
the run-ID/stageDir and do not need direct hardening — only verification. A defensible Block 8 test
launches 12 ULID-distinct sibling runs from one parent against the **same parent repo** and asserts
DB-row uniqueness, branch/worktree non-overlap, log-tree non-overlap, no tmux-session-name clashes
on the shared socket, and the ability of the rundb's WAL+busy_timeout=5s configuration to absorb the
12-writer write storm without `SQLITE_BUSY` losses.

## 1. Inventory — shared mutable surfaces (file:line)

Grouped by failure mode. Citations are file:line (paths relative to repo root).

### 1.a Global SQLite run database (file-locking / write contention)
- `internal/attractor/rundb/rundb.go:27-34` — `DefaultPath()` returns `${XDG_STATE_HOME:-$HOME/.local/state}/kilroy/runs.db`. **One file shared by every run on the machine** (parent + children + siblings + unrelated runs).
- `internal/attractor/rundb/rundb.go:39-50` — `Open()` enables `journal_mode=wal`, `busy_timeout=5000ms`, `synchronous=normal`, `foreign_keys=on`, and `db.SetMaxOpenConns(1)`.
- `cmd/kilroy/main.go:184` and `cmd/kilroy/run_detach_db.go:25` — every `kilroy` invocation calls `rundb.Open(rundb.DefaultPath())` independently; each process opens its own connection to the same file.
- Write hot-spots all funnel through `internal/attractor/rundb/write.go`:
  - `InsertRun` (line 65) — once per run start, INSERT OR REPLACE into `runs`.
  - `CompleteRun` (line 81) — once per run end, UPDATE `runs`.
  - `InsertNodeStart` (line 110) / `CompleteNode` (line 123) — fired per node attempt.
  - `InsertEdgeDecision` (line 136) — per edge selection.
  - `InsertProviderSelection` (line 146) — per agent node attempt.
  - `InsertNodeArtifact` (line 166) — per captured stage file (artifactCaptureList in `engine/run_db_hooks.go:87-105` is 14 files), and again for any tool-script reference (`engine/run_db_hooks.go:153-200`). With 12 runs × N nodes × ~14 artifacts each, this is the dominant write surface.
  - `RecordNodeDiff` (line 189) — per node attempt with a diff.

### 1.b Parent git repository `.git/` (file-locking)
- `internal/attractor/workflows/git_hook.go:40-47` — `SetupRunWorkspace` runs `git -C <repoPath> branch --force <runBranch> <baseSHA>` then `git worktree add <worktreeDir> <runBranch>` against the **shared parent repo**.
- `internal/attractor/gitutil/git.go:79-88` — `CreateBranchAt` and `AddWorktree` do not hold any kilroy-level lock; concurrency relies entirely on git's own `.git/index.lock` / `.git/worktrees/<name>.lock`.
- `internal/attractor/engine/engine.go:525-534` — engine calls `SetupRunWorkspace` early in `eng.run()`. With 12 siblings, 12 concurrent `git branch` + `git worktree add` calls hit the same parent `.git` simultaneously.
- `internal/attractor/workflows/git_hook.go:84-86` (`RepairWorktree`) and `engine/parallel_handlers.go:490` — also touch the shared `.git/worktrees/` registry during execution.
- Branch names are derived from `attractor/run/<runID>` (`engine/branch_names.go:8-18`) and ULID-distinct, so name collision is impossible. The race is purely on `.git/index.lock`.

### 1.c tmux server (process-shared resource)
- `internal/attractor/agents/tmux_handler.go:22` — `const kilroySocket = "kilroy"`. **One named socket** for every tmux invocation on the machine.
- `internal/attractor/agents/tmux_handler.go:36-42` — `NewTmuxAgentHandler` builds `tmux.NewManager(kilroySocket)`; every node execution shares it.
- `internal/attractor/agents/tmux/manager.go:13-23` — `Manager.run` always passes `-L kilroy`, so sessions live on a single tmux server process.
- Session-name uniqueness: `engine/agents/tmux_handler.go:518-535` — `buildSessionName` = `"kilroy-" + runID + "-" + nodeID`, sanitized, truncated to 128 chars. ULID run-IDs make per-run uniqueness intrinsic; collisions only possible across re-runs of the **same** runID (not the 12-sibling case).

### 1.d Per-machine CLI tool sessions (silent shared mutable state)
- `internal/attractor/engine/binder_anthropic.go:33-57` (`BindClaudeCLI`) — for the `cli_session` source, the binder *only* scrubs `ANTHROPIC_API_KEY`; it does **not** override `HOME`. The Claude CLI therefore reads/writes `~/.claude/` (auth tokens, history, MCP state). 12 concurrent Claude CLI invocations all share `~/.claude/`.
- `internal/attractor/engine/binder_openai.go:48-58` (`BindCodexCLI`, `SourceCLISession` branch) — symmetrically scrubs `OPENAI_API_KEY` only; the Codex CLI uses `~/.codex/` shared across runs.
- `internal/attractor/engine/agent_router.go:1909-2035` (`buildCodexIsolatedEnvWithName`) — for the API/probe code path **only**, sets `HOME=<codexHome>` under `XDG_STATE_HOME/kilroy/attractor/codex-state/codex-home-<sha>`. The base directory `codex-state/` is shared across runs, but each stageDir hashes to its own subdirectory. Note this is the env path; the tmux path (the production execution path for class-routed runs) does **not** apply this isolation (see binder above).
- The tmux template for codex (`internal/attractor/agents/templates/codex.go:27-33`) explicitly returns an empty env map and defers to the binder, so the shared `~/.codex/` story stands for `cli_session` runs.

### 1.e CXDB binary + HTTP service (process-shared, network-bound)
- `internal/attractor/engine/cxdb_bootstrap.go:109-217` (`ensureCXDBReady`) — connects to `cfg.CXDB.BinaryAddr` and `cfg.CXDB.HTTPBaseURL`. **One CXDB instance shared by every concurrent run** that opts in.
- The bootstrap will autostart CXDB if not reachable (lines 136-156) — **only the first sibling** to lose the race wins the autostart; the rest will see the autostart already up.
- `internal/attractor/engine/run_with_config.go:50-63` — `PublishRegistryBundle` is content-addressed by SHA-256 (per CLAUDE.md/MEMORY.md note). Bundle hash is deterministic, so 12 simultaneous publishes of the same bundle are an idempotent race.
- `internal/cxdb/binary_client.go:79` — binary client dials TCP per-call; no per-run port reservation.

### 1.f Per-run resources that are *isolated by construction* (need verification, not hardening)
- **Run ID:** `internal/attractor/engine/runid.go:10-18` — ULID with `crypto/rand` monotonic entropy. Collision-free by construction. Collision probability for 12 IDs: negligible.
- **Logs root:** `internal/attractor/engine/engine.go:2282-2310` (`defaultLogsRoot`) — `${XDG_STATE_HOME}/kilroy/attractor/runs/<runID>`. Unique per run because runID is unique.
- **Worktree directory:** `internal/attractor/engine/engine.go:156-158` (`applyDefaults`) — defaults to `<logsRoot>/worktree`. Unique by transitivity.
- **Run-ownership lock:** `internal/attractor/engine/run_ownership_lock.go:63-135` — `O_CREAT|O_EXCL` create on `<logsRoot>/run.lock.json`, with PID + PID-start-time fingerprint and 20×25ms retry. Per-logsRoot, so it does **not** prevent two runs against different logsRoots — that is the desired behavior for siblings.
- **Run branch:** `internal/attractor/engine/branch_names.go:8-18` — `attractor/run/<runID>`. Unique by transitivity.
- **Tmux session names:** see 1.c above.
- **Per-run model catalog snapshot:** `internal/attractor/modeldb/catalog_resolve.go:39-43` — written under `<logsRoot>/modeldb/openrouter_models.json`. Per-run.
- **Per-run progress files:** `internal/attractor/engine/progress.go:78-84` — `<logsRoot>/progress.ndjson` + `<logsRoot>/live.json`, opened/closed per event. Mutex-serialized intra-engine; per-run by location.
- **Per-run RunLog:** `internal/attractor/engine/runlog.go:15-67` — `<logsRoot>/run.log` with mutex. Per-run.
- **Per-run artifact store:** `internal/attractor/engine/artifact_store.go:59-77` — under `<logsRoot>/artifacts/`. Per-run.
- **Per-stage atomic writes:** `internal/attractor/runtime/atomic_write.go:11-43` — `os.CreateTemp(dir, ".tmp-*.json")` then `os.Rename`. Each stage dir is per-run.

## 2. Surfaces already safe under concurrent access

| Surface | Protection | Read/write pairing |
|---|---|---|
| `runs.db` writes | SQLite WAL journal + `busy_timeout=5000ms` + `db.SetMaxOpenConns(1)` per process. (`rundb.go:39-50`) | All writes serialize through the per-process single connection; cross-process writes serialize via the WAL writer lock with a 5s wait. **Caveat:** `SetMaxOpenConns(1)` only serializes within a single process; with 12 sibling processes, twelve writers contend on the WAL lock. The 5s busy_timeout has not been measured under that load. |
| `runs.db` reads | WAL mode lets readers proceed without blocking writers. | Reads from `kilroy runs list` etc. concurrent with run writes are non-blocking. |
| `<logsRoot>/run.log` (`runlog.go:62-67`) | `sync.Mutex` around the `*os.File.Write`. | Per-run only — different runs write to different files. Within a run, every Emit/Info/Warn/Error grabs the mutex. |
| `<logsRoot>/progress.ndjson` (`progress.go:69-81`) | `engine.progressMu` mutex; file is opened+closed per event so partial writes flush before the lock releases. | Per-run only. |
| `runtime.Context` (`runtime/context.go:11-126`) | `sync.RWMutex`; `Set`/`Get`/`AppendLog`/`SnapshotValues`/`SnapshotLogs`/`Clone`/`ApplyUpdates` all take it. | Per-run; parallel branches use `Clone()` (line 76-85) for a deep-copied snapshot. |
| `ArtifactStore` (`engine/artifact_store.go:59-217`) | `sync.RWMutex`. `Store` takes `Lock`; `Retrieve`/`Has`/`List`/`Info` take `RLock`. | Per-run; the `baseDir` is `<logsRoot>/artifacts/` so cross-run paths cannot collide. |
| `tmux.Manager` per-session input | `Manager.locks` (`sync.Map` of per-session channels, `manager.go:103-113`). | Per-session; uses `acquireInputLock`/`releaseInputLock` channel-of-1 pattern. |
| Run-ownership lock | POSIX `O_CREAT|O_EXCL` create+rename, plus PID+start-time liveness fingerprint with 20×25ms retry. (`run_ownership_lock.go:85-135`) | Per-logsRoot; rejects concurrent same-logsRoot runs but allows different-logsRoot siblings, which is what we want. |
| Atomic file writes (`runtime/atomic_write.go:11-43`) | Same-directory tmp file then `os.Rename`. | Last-writer-wins on the destination, but tmp names from `os.CreateTemp` are collision-free. Use sites: checkpoint.json, status.json, final.json — all per-run. |
| `<logsRoot>/run.pid` (`engine.go:519`) | Single writer per run (the engine), written once at startup. | No concurrent writers. |
| Per-run model catalog snapshot (`modeldb/catalog_resolve.go`) | One writer per run; destination is `<logsRoot>/modeldb/`. | No cross-run sharing. |
| Per-stage codex isolated home (`agent_router.go:2001-2013`) | SHA-keyed under `XDG_STATE_HOME/kilroy/attractor/codex-state/codex-home-<sha>` where the SHA hashes `absStageDir|homeDirName`. | Different stage dirs (different runs) get different subdirs. |
| CXDB registry bundle publish (`run_with_config.go:50-57`) | Content-addressed by SHA — duplicate publish is idempotent. | Multiple sibling publishes of the same bundle ID are harmless. |
| Anthropic SDK / OpenAI SDK env scrubs (`binder_anthropic.go:50-56`, `binder_openai.go:48-58`) | Scrub-list applied via `env -u VAR` wrapper in `tmux_handler.go:206-215`, so child processes literally do not see the env key. | Per-stage; no cross-run interaction. |

## 3. Surfaces that may NOT be safe under concurrent access

For each, I name a concrete failure scenario the code admits. I flag where I cannot tell from the code.

### 3.a Parent repo `.git/index.lock` / `.git/worktrees/` race
**Where:** `internal/attractor/workflows/git_hook.go:40-47` (`SetupRunWorkspace`) and `gitutil/git.go:85-101` (`AddWorktree`/`RemoveWorktree`/`RepairWorktree`).
**Scenario:** 12 siblings simultaneously call `git -C <repoPath> branch --force …` and `git worktree add …`. Git serializes via `.git/index.lock`. If a sibling holds it longer than another sibling's wait window (git's default lock retry is short), the loser exits with `fatal: Unable to create '.git/index.lock': File exists.` causing the run to fail at startup with no checkpoint having been taken.
**Severity:** High under 12-way contention; we have no kilroy-level retry around these calls.
**What the test must catch:** any sibling that exits with `.git/index.lock` or `worktree.lock` in stderr.

### 3.b Concurrent rundb writes hitting WAL contention
**Where:** `rundb/rundb.go:39-50` + every `*.go` in `engine/run_db_hooks.go`.
**Scenario:** 12 sibling engines + the parent + zero-or-more `runs list`/`status` callers all open the same DB. Each engine fires bursts of artifact-capture writes (~14 artifacts × N node attempts). The 5-second `busy_timeout` is long but finite. If a writer holds the WAL writer lock for >5s (large artifact blob INSERT), a peer will see `SQLITE_BUSY` returned to the engine, which then logs a warning via `e.Warn(...)` — the run continues but **the DB record is incomplete** (missing artifact / node / edge row). `runs show` then under-reports.
**I don't know** how long an INSERT of a 10MB artifact blob (the cap in `engine/run_db_hooks.go:109`) actually holds the WAL writer lock on a typical macOS APFS / Linux ext4 box. Could be milliseconds, could be tens of seconds under fsync pressure with `synchronous=normal`. Worth measuring.
**What the test must catch:** any "rundb: …" warnings in `<logsRoot>/run.log`; final node-execution count in the DB matches the engine's view; final progress.ndjson event count matches the engine's emit count.

### 3.c CLI tool sessions in `~/.claude/`, `~/.codex/`
**Where:** `binder_anthropic.go:50-57` (no `HOME` override for `cli_session`), `binder_openai.go:48-58` (same for codex `cli_session`), `tmux_handler.go:121-153` (binder result merged into env, no isolated `HOME`).
**Scenario:** 12 sibling claude_cli sessions all read/write `~/.claude/` simultaneously — config files, OAuth refresh tokens, history, MCP server state. The Claude CLI is **not documented** as concurrency-safe for the same `$HOME`. Failure shapes I would expect:
- OAuth refresh-token rewrite race (one session refreshes, the others see a stale token).
- Configuration / MCP state file truncation.
- History file interleaving (cosmetic).
**I don't know** whether Claude CLI uses file locking on its state files. The kilroy code does not protect the directory in any way.
**What the test must catch:** any node attempt that fails with auth-related errors after the first one succeeds; any `~/.claude/.credentials.json`-style corruption (compare hash before/after).

### 3.d Tmux server start race (cosmetic but observable)
**Where:** `tmux/manager.go:29-45`, `agents/tmux_handler.go:22`.
**Scenario:** No kilroy run currently has a tmux server on `-L kilroy`. 12 siblings call `tmux -L kilroy new-session ...` simultaneously. The first invocation forks a tmux server and binds the socket; the rest race for the socket. tmux normally handles this gracefully — losers reconnect to the winner's server — but there is a brief window where a loser may see "no server running on /tmp/tmux-<uid>/kilroy" and fail.
**I don't know** the exact tmux behavior under this race; the kilroy code has no retry.

### 3.e CXDB autostart race
**Where:** `cxdb_bootstrap.go:131-217`.
**Scenario:** if siblings collectively launch with autostart enabled and CXDB is not yet up, all 12 will call `startBackgroundCommand(cfg.CXDB.Autostart.Command, ...)` (line 146). If the autostart command starts CXDB (e.g. `docker run -d --name kilroy-cxdb …`), the **second through twelfth** instances will fail because the container name is already in use. Each will return an error from `startBackgroundCommand`, but the subsequent `connect()` retry loop should eventually succeed once the first sibling's autostart finishes.
**Worth confirming** that the engine treats the `startBackgroundCommand` error as recoverable when a peer wins the race.

### 3.f Codex `~/.codex/` mutation under cli_session
Same shape as 3.c for Codex; same unknowns.

### 3.g Default browser-open / system tray / etc.
None observed. `browsergate` is a string classifier (`internal/attractor/browsergate/match.go`), not a runtime gate.

### 3.h Test-environment-only surfaces (not runtime)
`internal/attractor/engine/cxdb_test_server_test.go:349` listens on `127.0.0.1:0` for tests; not part of production runtime.

## 4. Proposed Block 8 stress-test design

### 4.a What the parent run does

The parent is a `kilroy run` of a small driver workflow whose `agent` stage's job is to spawn 12 sibling runs. Two shapes work:

- **Bash-driver shape (preferred for Block 8):** the parent stage is a `tool_command bash <path>` stage. The bash script launches 12 detached `kilroy run --detach <name> --label stress-block-8 --label parent_run_id=$KILROY_RUN_ID …` invocations, then waits on them with `kilroy runs wait --label parent_run_id=$KILROY_RUN_ID --all --timeout 30m`. This avoids LLM cost, runs in CI, and gives the test deterministic semantics.
- **Agent shape (also valid, costlier):** the parent's agent stage is given `KILROY_PARENT_RUN_ID` via §11.2's contract (Block 7 prerequisite — likely not landed yet) and launches the children via shelled-out `kilroy run`.

**Children should be a mix of distinct workflows** to stress different code paths concurrently — per the plan's "12+ sibling runs of distinct workflows":
- 4 × `investigate` (Block 9 trio; cheap, read-only).
- 4 × `fix` against tiny synthetic broken-Go repos (exercises the git checkpointing path).
- 4 × `build-test` against tiny Go modules (exercises the verify path).

All children share the same `--config` (so they share the same CXDB target if enabled). They run against the **same** parent repo (not 12 separate clones) — that is the load-bearing concurrency check.

### 4.b Test file location and shape

Suggested path: `internal/attractor/engine/concurrency_stress_test.go`. Build-tagged `//go:build stress` so `go test ./...` doesn't run it by default; CI invokes it via `go test -tags=stress ./internal/attractor/engine/`.

Skeleton:

```go
//go:build stress

func TestBlock8_TwelveSiblingRunsAreIsolated(t *testing.T) {
    parentRepo := initSyntheticRepo(t)        // single repo all 12 share
    logsRootBase := t.TempDir()
    cfg := loadStressConfig(t)                // CXDB optional; toggled by env

    childIDs := launchSiblings(t, ctx, 12,    // returns ULID run IDs
        []workflowSpec{
            {name: "investigate", count: 4},
            {name: "fix",         count: 4},
            {name: "build-test",  count: 4},
        },
        parentRepo, logsRootBase, cfg,
    )

    // Wait for all to terminate (success or fail).
    finals := waitAll(t, ctx, childIDs, 20*time.Minute)

    // === Assertions, grouped by failure surface ===
    assertAllReachedTerminal(t, finals)                          // §4.c.1
    assertNoLogsRootCollisions(t, finals)                        // §4.c.2
    assertDBRowExistsExactlyOncePerRun(t, finals)                // §4.c.3
    assertNoSiblingDBRowMissingNodeExecutions(t, finals)         // §4.c.4
    assertNoTmuxSessionNameCollisions(t, finals)                 // §4.c.5
    assertNoGitLockErrorsInRunLogs(t, finals)                    // §4.c.6
    assertParentRepoBranchesAllCreated(t, parentRepo, finals)    // §4.c.7
    assertWorktreeDirsAllRegistered(t, parentRepo, finals)       // §4.c.8
    assertNoRundbWarnings(t, finals)                             // §4.c.9
    assertCheckpointShasMonotonicPerRun(t, finals)               // §4.c.10
    assertCLISessionStateUnchangedOrIntact(t, ~/.claude, ~/.codex) // §4.c.11
    assertCXDBContextsDistinct(t, finals)                        // §4.c.12
}
```

### 4.c Concrete assertion list

1. **All 12 children reach a terminal state within timeout.** `final.json` exists for each, with `status ∈ {success, fail, canceled}`. Hangs are a Block 8 failure regardless of cause.
2. **Logs roots do not overlap.** For every pair of run IDs `(a,b)`, `logsRoot(a)` is not a prefix of `logsRoot(b)` and vice versa, and the directory contents are disjoint.
3. **DB has exactly one `runs` row per child run, no orphan or duplicate IDs.** `SELECT run_id, COUNT(*) FROM runs WHERE run_id IN (…) GROUP BY run_id` — every count must equal 1; row count must equal 12.
4. **Per-run `node_executions` count matches what the engine recorded in `progress.ndjson`.** For each child, `count(*) FROM node_executions WHERE run_id = ?` equals the count of `event in {"node_started"/"node_completed"}` events in the per-run progress.ndjson. Any mismatch implicates rundb write loss (§3.b).
5. **No two tmux sessions on the shared `kilroy` socket share a name during overlapping windows.** Approximate via `progress.ndjson` `tmux_session_start`/`tmux_session_complete` event timestamps from all runs — assert no two runs emit the same `session` value with overlapping windows.
6. **No run.log line contains `index.lock` or `worktree.lock` or `Unable to create`.** Direct probe for §3.a.
7. **`git -C <parentRepo> branch --list 'attractor/run/*'` lists at least 12 unique branches** matching the children's runIDs.
8. **`git -C <parentRepo> worktree list` shows the children's worktree paths still registered** (or cleanly removed by the engine on teardown — assert one or the other consistently).
9. **No `e.Warn("rundb: …")` lines in any run.log.** Direct probe for §3.b.
10. **Per-run checkpoint commit SHAs form a chain on the run's branch.** `git log <runBranch>` should show a strictly forward history; any rebase / forced reset implies cross-run interference.
11. **`~/.claude/` and `~/.codex/` directory hashes (excluding history files we know mutate normally) match the pre-run snapshot** — or, more practically, **are at least valid JSON** (catch corruption from §3.c). Skip this assertion when running under the `cli_session` source on a machine the developer is also using interactively — this is a CI-only check.
12. **CXDB context IDs are distinct per child.** Read each child's `final.json.cxdb_context_id`; assert the set has 12 distinct values.

### 4.d Test variants for matrix coverage

Run the test three times in CI:

- **Variant A: api backend, no CXDB.** Cleanest — exercises rundb + git + logs + workspace surfaces only.
- **Variant B: cli backend (claude or codex), no CXDB.** Adds the tmux + `~/.claude/` / `~/.codex/` surfaces. Requires CI to have the tool installed and authenticated (or use a stub binary via `KILROY_CLAUDE_PATH`).
- **Variant C: api backend, CXDB enabled with autostart.** Stresses §3.e.

### 4.e What the test deliberately does NOT cover

- It does not assert that any specific child's *content* is correct (that's the workflow's own test).
- It does not test resume from checkpoint of a sibling (orthogonal — Block 8 is launch-time and run-time isolation).
- It does not stress 100+ siblings — the plan's bar is 12, and the failure modes do not look like they need O(100) to surface.

## 5. Open questions

- **rundb write-lock latency under load.** §3.b — I cannot tell from the code how long a 10MB artifact-blob INSERT holds the WAL writer lock with `synchronous=normal`. Worth a microbenchmark (one writer, varying blob sizes) before declaring the 5s `busy_timeout` adequate. If it is not adequate, options are (i) increase the busy_timeout, (ii) shrink `maxCapturedArtifactBytes`, (iii) move large-artifact storage out of SQLite into the on-disk artifact store.
- **Claude / Codex CLI concurrency-safety on `~/.claude/` / `~/.codex/`.** §3.c, §3.f — kilroy code makes no claim about it. Either (a) the CLIs are safe and we can document it, or (b) we need an isolated `HOME=<stageDir>/.claude-home` for the cli_session path the way the API path already does for codex (§1.d). The current asymmetry between the binder paths (envvar → isolated home; cli_session → shared home) looks like a hidden assumption, not a deliberate design.
- **Parent-repo `.git/index.lock` retry policy.** §3.a — should kilroy add an outer retry loop around `SetupRunWorkspace` (e.g. 5 × 200ms) to absorb the burst, or rely on git's own behavior? The right call probably depends on what the stress test actually shows.
- **Block 7 dependency.** The plan calls Block 7 (recursion linkage / `KILROY_PARENT_RUN_ID`) a sibling of Block 8 but not a hard prereq. Practically, the stress test wants the `parent_run_id` linkage to identify the cohort cleanly. If Block 7 is not in by the time Block 8 lands, fall back to the `--label parent_run_id=…` shape proposed in §4.a.
- **CXDB registry bundle publish under a registry version skew.** The MEMORY note records that changing a type definition changes the bundle hash and conflicts with cached bundles. The 12-sibling test won't surface this on its own, but Block 8 should call out that mid-run schema migrations are out of scope.
- **Where the stress test executable lives in CI.** A `//go:build stress` test that reaches out and `exec`s `kilroy run --detach …` is structurally an end-to-end test, not a unit test. It probably belongs alongside `scripts/e2e.sh` rather than in the engine package — but consolidating the assertion helpers in `internal/attractor/engine/` is also defensible. Worth a Block-8-author judgment call.
- **AGENTS.md "load-bearing-property guarantees" wording.** Block 8's third bullet asks to document the guarantees; I have not drafted the AGENTS.md text — that is implementation-time work once the stress test pins down what is actually true.

## Methodology

I read, in this worktree (`/Users/matt/.local/state/kilroy/attractor/runs/01KQR6NA26969WZ6W14V98DGDY/worktree/`):
the Block 8 plan section and §11.3 (`docs/plans/2026-05-01-kilroy-v2-final-plan.md`); the run lifecycle
(`internal/attractor/engine/run_with_config.go`, `engine.go` initialization at lines 490-600,
`run_ownership_lock.go`, `runid.go`, `branch_names.go`, `run_db_hooks.go`); the rundb concurrency
contract (`internal/attractor/rundb/rundb.go`, `write.go`); the runtime artifact writers
(`internal/attractor/runtime/atomic_write.go`, `checkpoint.go`, `context.go`, `status.go`, `final.go`);
the tmux server abstraction (`internal/attractor/agents/tmux/manager.go`, `session.go`,
`agents/tmux_handler.go`); CXDB bootstrap (`engine/cxdb_bootstrap.go`); per-driver credential binders
(`engine/binder_anthropic.go`, `binder_openai.go`, `credential_binder.go`); the model catalog
snapshot (`modeldb/catalog_resolve.go`); the artifact store (`engine/artifact_store.go`); the
RunLog and progress files (`engine/runlog.go`, `progress.go`); workspace allocation
(`engine/workspace_test.go`, `engine.go applyDefaults`); the GitOps interface
(`engine/git_ops.go`, `workflows/git_hook.go`, `gitutil/git.go`); and `cmd/kilroy/main.go` +
`cmd/kilroy/run_detach_db.go` to confirm the per-process rundb open pattern.

I did **not** read: Block 6 agent-backend code (only relevant if its concurrency story differs from the
existing tmux/api split, which §11.3 implies it does not); CXDB internals beyond `binary_client.go`
(I trust the bundled-by-hash idempotence note from MEMORY.md); the `internal/server/` UI surface
(out of scope — UI does not read/write run state, it polls the DB); test files in detail (used them
as confirmation rather than primary sources). I also did not run the stress test or any benchmark —
this is read-only research, and the open questions in §5 explicitly call out where measurements
would change my answers.

Time-cap: not reached; converged in roughly one focused pass.
