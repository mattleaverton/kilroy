# 2026-05-01 — V2 reframe dogfood lab notes

Captured while running 6 parallel quick-launch investigations to inform the v2 plan synthesis. Goal: gather *evidence* for the friction the v2 reframe is supposed to fix, by hitting it directly.

## Set-up

- 7 empty temp dirs under `/tmp/kilroy-v2-investigations/inv-{pilot,1-manifest,2-auth,3-cwd,4-policy,5-tuple,6-builtins}/`.
- Each is a freshly-`git init`'d empty repo with one empty commit.
- Each holds a single `prompt.md` describing the investigation; passed via `--prompt-file`.
- Pilot ran first to validate ergonomics; the six investigations launched in parallel after pilot succeeded.
- Labels: `task=v2-inv<N>-<name>` for retrieval.

## Findings — friction observed

### 1. Stale-build detection trips when invoked from arbitrary CWD

`/Users/matt/.local/bin/kilroy` is a symlink into the dev repo (`/Users/matt/sw/personal/kilroy/kilroy`). When invoked from `/tmp/...`, kilroy compared the binary's embedded build SHA against the dev repo's HEAD (because the binary lives there) and refused to launch with "WARNING: STALE KILROY BUILD DETECTED".

**Why this matters for v2.** The agent-primary pitch says "kilroy stands where you stand." If `kilroy run <workflow>` from a non-kilroy CWD checks the binary-source repo for staleness, every cross-repo invocation can hit this. Three options:
- (a) Stale-build check should only fire when CWD == binary-source repo (i.e. clearly dev mode).
- (b) Distinguish "released binary" (fingerprinted in the binary itself, no in-repo SHA comparison) from "dev binary" (compared to repo head).
- (c) Remove stale-build entirely and rely on `--version` printout discipline.

Option (b) is probably right. Released binaries don't need stale checks; dev binaries do. The check should respect that.

### 2. Failed validation creates a "zombie" run record

The shipped `~/.local/share/kilroy/workflows/quick-launch/graph.dot` had `agent -> done` without a `condition=` attribute. The recently-added validator (commit `95637c9`, "validator-time guard on inbound-to-terminal edges") rejects this. But the failure mode was nasty:

- `attractor run --detach` returned `exit 0`, printed `detached=true`, `logs_root=...`, `pid_file=...`.
- The logs root was created.
- A `run.pid` file was written, but the worker process exited immediately.
- `runs show` reported `status: running` indefinitely (no `live.json`, no `progress.ndjson`, no `final.json` ever created, only `run.out` containing the validation error).
- `runs wait --timeout 8m` hung the full timeout, then exited 0 with "timeout waiting" (which itself is confusing — exit 0 on timeout?).

This is **exactly** the pre-execution-failure case Plan B's "failed launch is a first-class persisted run outcome" is supposed to fix. The current behavior:
- silently misleads agent callers into believing the run is in flight,
- contaminates the run-list with a permanent "running" zombie,
- offers no signal that anything is wrong unless you cat `run.out` directly.

**For v2:** validation must persist a final.json with `status: fail` and `failure_reason: validation_failed` *before* the worker exits. The existing run-record machinery is already 90% of the way there — just needs the validation path to take it.

Bonus: `runs wait`'s exit code on timeout (saw 0) deserves an audit. Per the skill doc it should be 2; it returned 0. Maybe a flag interaction with `--latest --label`. Worth a separate look.

### 3. Quick-launch shipped graph was already broken

The user-installed quick-launch package (`~/.local/share/kilroy/workflows/quick-launch/graph.dot`, `graph.codex.dot`, `graph.gemini.dot`) was broken against the current validator. The graphs ship with `agent -> done` unconditioned, which the new validator rejects. Nobody noticed because:
- shipped graphs aren't tested in the kilroy CI matrix (or are tested against an older build),
- the failure mode (zombie running record) doesn't surface to anyone watching,
- the graphs were authored before the validator change tightened things.

**For v2:** built-in workflows (which Plan B wants to bake into the binary) MUST be validated in CI against the same validator the runtime uses. This is a `go test ./...` regression test, not an add-on. If the validator rejects a baked-in workflow, the build should fail.

I patched the three shipped graphs with `condition="outcome=success"` on `agent -> done` and re-validated. All three now pass `kilroy attractor validate`. This was a real bug; fix is local to the user-level workflow files.

### 4. CLI shape: still attractor-prefixed

Every command in this session was `kilroy attractor run …`, `kilroy attractor runs show …`, etc. The skill doc and AGENTS.md both wrap this through, but the friction is real — the verbosity stacks against the quick-launch goal of "fire and forget many calls." Plan B's "drop the `attractor` namespace; move subcommands to top level" is well motivated by direct observation.

### 5. Auto-detection of providers worked silently and well

`auto-detected provider openai (backend=cli)` and `auto-detected provider anthropic (backend=cli)` were printed on every launch with no run.yaml supplied. This is a v2 strength worth preserving — workflow authors don't hardcode providers, kilroy figures out what's reachable. The class-resolver will formalize this behavior.

### 6. The pilot-run worktree pattern is genuinely clean

Pilot agent's `pwd` was `<logs_root>/worktree`, not the temp dir I created. This is good isolation:
- temp dir serves as the "git repo we detected" — nothing more.
- worktree is the agent's actual workspace, kilroy-managed, isolated per run.
- six sibling runs cannot collide on each other's worktrees.

This is one of the v1 strengths to preserve in v2. The 5-layer architecture ("Execution Core handles workspace, isolation, retries, events") gets this right.

### 7. Output retrieval is fine

`kilroy attractor runs show --latest --label task=<slug> --print result.md` worked first try, machine-readable, no ceremony. Plus `runs show --json` gave structured run metadata. The agent-primary contract is mostly already there for outputs; the remaining work is mostly about making *launch* and *failure* match the same agent-primary bar.

## Findings — what worked well, no friction

- Auto-detection of installed CLIs (no run.yaml needed for the common case).
- Tagging with `--label` and retrieving with `--latest --label`.
- `--prompt-file` for long prompts. Skill doc was right that this is much better than inline JSON.
- Detached + background-tmux model. The agent ran with no shell-blocking ceremony; my conversation kept moving while six investigations ran behind it.
- `runs show --json` machine-readable output. Already structured; v2's "JSON by default" mostly formalizes what's already present.

## Open follow-ups discovered during the session

- [ ] Stale-build symlink behavior (finding 1) — pre-v2 cleanup.
- [ ] Validation-failure persistence (finding 2) — v2 launch contract.
- [ ] CI test for built-in workflow graphs (finding 3) — quality gate.
- [ ] `runs wait` exit code semantics (finding 2 bonus) — audit.
- [ ] First zombie pilot run (`01KQJ5PDMTPPERR8FT87QSQ1SS`, label `v2-pilot`) is still showing `running` — DB cleanup or "garbage collect zombie" command is worth designing.

## Cross-reference

- Plan A: `docs/plans/2026-05-01-kilroy-v2-workflow-platform-shift.md`
- Plan B: `docs/plans/2026-05-01-v2-workflow-platform-reframe.md`
- This dogfood log feeds into the synthesis at `docs/plans/2026-05-01-kilroy-v2-final-plan.md` (TBD).
