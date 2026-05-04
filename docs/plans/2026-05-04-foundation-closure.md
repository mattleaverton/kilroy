---
title: Foundation Closure Plan
date: 2026-05-04
branch: feat/v2-reframe
status: draft
---

# Foundation Closure Plan

The intended last big push before kilroy is a stable platform. Synthesizes
findings from three sources:

- **A** = self-audit conducted 2026-05-04 (21 items)
- **R** = external reviewer pass (P0/P1/P2 items)
- **M** = Matt's net-new asks

Each work item is tagged with its source. Items are grouped by phase, with
phase 1 being the bug-fix beachhead and later phases progressing from
contract correctness → symmetry → deferred decisions → net-new exploration.

**Operational model:** dispatch parallel kilroy workers per phase using
the existing self-hosted loop. Use a smarter overseer (claude-opus or
sonnet) to scope each task tightly for a kimi-k2/opencode worker. Reviewer
overseer validates on exit. We chewed through ~10 probes and 3 fixes that
way today; this plan is sized for the same throughput.

---

## Phase 1 — Critical bugs / contract violations

These are silent failures that contradict documented invariants. Ship
first. Independent file scopes — dispatchable in parallel.

### 1.1 Auth route identity is not actually frozen [R-P0]

**File:** `internal/attractor/agents/tmux_handler.go:567`,
`internal/attractor/engine/agent_router.go:305`.

Prelaunch resolves auth, writes a snapshot, and promises the route is
frozen. But both the tmux path and the SDK path re-open auth resolution
from the run worktree at execution time. This means a prelaunch can pass
on user-layer config, then execution can fail when the worktree lacks
the same project auth config — or worse, silently use a different
binding than what was snapshotted.

**Outcome:** at execution, the binder reads the prelaunch snapshot and
applies it directly. No re-resolution from the worktree.

### 1.2 SDK class routing falls back to canonical env [R-P0]

**File:** `internal/attractor/engine/agent_router.go:324`.

`clientForRoute` binds the selected credential, then calls
`ensureAPIClient()` BEFORE installing the credential-aware adapter. A
user with only `*_API_KEY_KILROY` configured can satisfy auth intent
but fail before that value is actually used.

**Outcome:** `ensureAPIClient` either runs after the credential-aware
adapter is in place, or refuses to run when a class-routed credential
will be the source of truth. No silent canonical-env dependency.

### 1.3 Non-class API routes bypass `_KILROY` precedence [A-15]

**Files:** `internal/llm/providers/{anthropic,openai,google}/adapter.go`
all read `os.Getenv("X_API_KEY")` directly.

Class-routed runs go through the auth chain and observe `_KILROY`
suffix first. Non-class runs ignore the convention. Documented
invariant violated. Likely the same fix shape as 1.2.

**Outcome:** all three adapters consult the auth chain or at least the
`_KILROY`-first env precedence. One implementation path, three call
sites.

### 1.4 opencode missing from prelaunch CLI driver list [R-P1]

**File:** `internal/attractor/engine/prelaunch.go:395` — `isCLIDriver`
excludes opencode even though `agent_route.go:170` resolves it as
`BackendCLI`. Workflows can validate while missing the actual binary.

**Outcome:** opencode is in the CLI driver set; prelaunch probes the
opencode binary and reports missing-binary as a fail.

### 1.5 CI is red — gofmt fail [R]

**File:** `cmd/kilroy/attractor_runs.go:839`. `gofmt -l .` reports it.
Ship a single-line fix.

### 1.6 Top-level help advertises `--follow` (removed) [A-1, R-P2]

**File:** `cmd/kilroy/main.go:223`. `--follow` was deleted in `3d34471`;
the actual subcommand uses `--watch` (`attractor_status.go:21`).
Top-level help string still says `--follow`. Trivial.

### 1.7 README leads with pre-v2 surface [R-P2]

**File:** `README.md:5`. Still leads with ingest/DOT/run.yaml instead of
workflow-first usage. Agents treat docs as API; this is not cosmetic.

### 1.8 Stale `attractor` namespace references in comments/help [A-4, A-5]

**Files:** `cmd/kilroy/main.go:78` (env-file usage example),
`cmd/kilroy/run.go:48` (pre-v2 attractor run comment),
`cmd/kilroy/main.go:118-125` (5-line removal stub — collapse to one).

**Phase 1 acceptance:**
- `gofmt -l .` clean
- `go test ./... -short` green
- Auth: a class-routed run with prelaunch snapshot from cwd works the
  same when launched from the worktree with the project auth.toml absent
  (snapshot is the source of truth)
- Non-class API run honors `OPENAI_API_KEY_KILROY` over `OPENAI_API_KEY`

---

## Phase 2 — Product contract / agent-primary surface

### 2.1 Workflow loop cap mismatch [R-P1]

**Files:** `workflows/implement/graph.dot:37`,
`workflows/fix/graph.dot:39` say "retry once". Default loop visit cap
disabled at `internal/attractor/engine/loop_restart_policy.go:19`.
Deterministic failure signature limit is 3, not "once", and changed
failure text can evade it. Product contract mismatch.

**Outcome:** either workflows enforce a real visit cap of 1 (matching
their label), or labels are corrected to reflect real semantics. Pick
the former unless there's a strong reason — agent-primary product
should mean what it says.

### 2.2 Chatty preflight on foreground runs [A-2]

**File:** `cmd/kilroy/main.go:734`. `auto-detected provider X` lines
emitted on every foreground launch. JSON-by-default + agent-primary
contract says: pristine machine output. Foreground should match the
detached path's quiet behavior (or send the lines to stderr at info
level only).

### 2.3 `kilroy run --no-cxdb` falls through to top-level help [A-3]

**File:** `cmd/kilroy/main.go:345`. With no graph/workflow argument it
calls top-level `usage()` instead of `runUsage()`. Cuts agents who
mistype.

### 2.4 Test the kilroy cleanup command [M-5]

Investigative pass. What does it cleanup? Disk only? DB rows? CXDB
chain? What is NOT captured in the DB that gets lost on cleanup?
Document semantics. Decide whether cleanup needs a `--dry-run` and a
JSON manifest of what would be removed. Required input for the UI work
(M-2) — UI has to know what survives cleanup.

**Phase 2 acceptance:**
- implement/fix workflows enforce their advertised retry semantics, or
  labels match reality
- Foreground `kilroy run` JSON output is the first byte (preflight is
  silent or stderr-only)
- `kilroy run --no-cxdb` with no positional argument shows run-specific
  usage
- Cleanup semantics documented + at least one test exercises it

---

## Phase 3 — Symmetry / completeness

### 3.1 Gemini CLI shipped class entries [A-16, M-4]

**File:** `internal/policy/data/policy.toml`. Today gemini_cli only
exists in `frontend_aesthetic` rank 3. Add gemini_cli class entries
mirroring the new `coding_codex_subscription` / `coding_codex_apikey`
shape. Users with Gemini subscriptions need a class to hit.

### 3.2 `policy show` and `policy list` project-aware [A-20]

**File:** `cmd/kilroy/policy.go`. F3 fixed `resolve` only. Bring
`show` and `list` to feature parity — same `--project` flag, same
cwd-discovery default. Each is a single-line fix on top of F3's
infrastructure.

### 3.3 opencode full auth binder integration [A-19, R-P1 partial]

**File:** `internal/attractor/agents/templates/opencode.go:61` (PARTIAL
marker). `_KILROY` precedence works. Full env-scrub + isolated config
like claude/codex doesn't. Ship the missing pieces or remove the
template until it's done — no half-paths.

### 3.4 `prelaunch_validation.json` schema versioning [A-21]

Add a `schema_version` field at top level. Document the contract. F1
worker flagged this as quiet liability — automation will start reading
it without a versioning discipline.

### 3.5 Catalog/policy mutual lint [R-P2]

`policy.toml` names `claude-opus-4-7` while OR catalog has 4.5/4.6.
We've established the dot/dash thing is real; also need a build-time
lint that policy IDs exist in the catalog, so policy can't drift past
data. Probably a single test in `internal/policy/`.

**Phase 3 acceptance:**
- gemini_cli class entries shipped + verified end-to-end with a real
  Gemini account
- `kilroy policy {show,list,resolve}` all observe project-layer
  auth.toml
- opencode template either fully integrated or removed
- `prelaunch_validation.json` carries `schema_version`
- Build fails if `policy.toml` names a model the catalog doesn't recognize

---

## Phase 4 — Deferred decisions

These need an explicit decision from Matt before scoping. Not
shippable as parallel worker tasks — they're conversations.

### 4.1 Async-default flip [A-14]

v2 §14: `--detach` becomes default, `--sync` the explicit-block escape.
Currently sync is default. Flip changes the default UX dramatically —
agents and humans both see "command exits immediately, ID printed". Need
your call before scoping.

### 4.2 Block 3 — config layering [A-11]

`<project-root>/.kilroy/config.toml` discovery + merge semantics. Auth
already does this. Config doesn't. Multi-day; needs scope conversation.

### 4.3 Block 8 — concurrency 12-sibling stress [A-12, R-P1]

Final plan calls 12+ siblings an acceptance gate
(`docs/plans/2026-05-01-kilroy-v2-final-plan.md:576,783`). We've
demoed 4. Need either the test infrastructure or a documented
"acceptable scope" reduction.

### 4.4 Block 7 — recursion linkage [A-13]

`KILROY_PARENT_RUN_ID` for nested run trees. Today's P5–P11 each
launched a sub-run; none are linked in the run DB. UI work (M-2) will
expose this gap immediately.

### 4.5 Skills environment policy [M-3]

Quick design decision: does kilroy care about the skills environment
of CLI workers, or is that the system's purview? Document and move on.
Probably "system's purview" but should be written down.

---

## Phase 5 — Net-new exploration

These are larger investments worth doing only after the foundation is
clean. Each is a multi-day prototype on its own branch.

### 5.1 Minimal UI for run visualization [M-2]

> "The UI is not the primary concern, but I want to get one up and
> running so I can visually see if it has degraded or is still working
> and it will allow me to see if we're gathering all the appropriate
> context of runs"

Read-only first cut. Consumes `kilroy serve` (which lets us answer A-17
in passing — either it's used now and we keep it, or it's not and we
remove it). Probably:
- Run list (status, age, workflow)
- Run detail (stages, provider_selections, prelaunch snapshot)
- CXDB chain viewer
- No write surface

Depends on Phase 4.4 (parent linkage) for the run-tree visualization to
make sense.

### 5.2 Tmux-interactive Claude with API key + Haiku polling [M-1]

> "An earlier iteration had a tmux-driven interactive session running on
> api key — pretty isolated and a bit janky… perhaps we could have a
> haiku api call to periodically check what the tmux read of the screen
> says and decide if we need to insert or interact"

Alternative path, NOT a replacement for the existing `claude -p`
non-interactive cli_oauth path. Architectural sketch:
- New driver flavor: `claude_cli_interactive` (api_key only)
- Tmux session held open, screen read on a clock
- Haiku-class model decides: continue / inject / consider failed
- Provides a way to swap between subscription and api-key paths per
  workflow

This is exploratory. Set a time-box (1 week?) and treat the result as
"keep / shelve / generalize."

### 5.3 Cursor CLI driver [M-4]

Net new driver, same shape as codex_cli. Builds on Phase 1 auth fixes
landing first (don't ship a new driver onto a broken base).

### 5.4 `kilroy serve` decision [A-17]

Either we document what consumes it (the UI in 5.1 is the obvious
candidate) and keep it, or remove it. Don't leave it as a top-level
command that does an unclear thing.

### 5.5 `modeldb suggest` decision [A-18]

Either link it from validator errors (so users find it when they need
it) or remove it. Currently it duplicates info already in catalog.

---

## Phase 6 — Surface polish (lowest priority)

Phony / borderline-legacy CLI args [A-6 through A-10]:

- `--confirm-stale-build` — friction trap. Smarter timestamp comparison
  or `KILROY_DEV=1` env. Real friction observed during today's worker
  runs.
- `--allow-test-shim` — flag in help as test-only.
- `--no-cxdb` — silent auto-default, remove from user-facing list, or
  document the auto-default behavior in `--help`.
- `--workspace`, `--run-id`, `--logs-root` — move to hidden
  `--advanced-help` block.
- `--graph`/`--package` direct mode — keep for tests; mark as advanced.

Ship as one focused pass. None of this affects correctness.

---

## What moved forward from M

Of Matt's 5 net-new asks:

- **M-3 (skills environment)** moved to Phase 4 (deferred decisions) —
  it's a quick design decision, not exploration. Should be answered
  before any worker dispatches collide with it.
- **M-4 (gemini CLI)** moved to Phase 3 (symmetry) — already exists as
  A-16, just needs shipped class entries.
- **M-5 (cleanup investigation)** moved to Phase 2 — UI work depends on
  knowing what cleanup destroys.
- **M-1 (tmux interactive Claude)** stays in Phase 5 — net-new
  exploration, hold until foundation is solid.
- **M-2 (UI)** stays in Phase 5 — depends on parent linkage (P4.4) and
  cleanup semantics (P2 M-5) being clear first.
- **M-4 (cursor CLI)** stays in Phase 5 — new driver work.

---

## Operational notes

- Each phase 1/2/3 item is independently dispatchable as a kilroy
  worker task. Match the F1/F2/F3 prompt template (TL;DR / files /
  diff outline / verification / out-of-scope / report).
- Use a smarter overseer (claude-opus or sonnet) to scope each task
  tightly. The kimi-k2/opencode workers we proved out today handle
  the implementation just fine when the prompt is sharp.
- Reviewer overseer validates on exit (lint, tests, manual probe).
- Do NOT run the full test suite per iteration — narrow `-run` matchers
  per the practice we landed on today. Full suite at phase boundaries.
- Don't mix phases in a single worker. Phase 1 has
  contract-violation-fixing semantics; phase 2 has product-shape
  semantics. Different review criteria.

## Open questions for Matt

1. Phase 4.1 (async-default flip): yes/no/defer?
2. Phase 4.2 (Block 3 config layering): scope this push, next push, or
   deprioritize?
3. Phase 4.3 (12-sibling stress): real gate or accept 4-sibling
   demonstration as sufficient for v2?
4. Phase 5.2 (tmux interactive Claude): time-box length?
5. Should this plan replace the v2 final plan as the active reference,
   or layer on top of it?
