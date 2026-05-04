## Status & Next Steps — 2026-05-03

Snapshot of where the v2-reframe branch is and what to pick up next.
Branch: `feat/v2-reframe`. Reference plans: `2026-05-01-v2-workflow-platform-reframe.md`,
`2026-05-02-auth-class-resolver-integration.md`.

---

### Where we are

**Auth ↔ class-resolver integration (A1–A8)**
Landed and reviewer-feedback rounds (R1–R10) addressed. Claude_cli matrix
verified end-to-end. Plan doc has the full round log.

**Block 6 — codec extraction**
- Step 1 (TurnEvent unified types) ✅
- Step 2 (Anthropic SSE, OpenAI SSE, codex CLI JSONL) ✅ — landed via
  parallel campaign (commit `9f2abbc`).
- Step 2 add-on (claude-CLI JSONL codec on TurnEvent) ✅ — commit `0c4fe0f`.
- Step 3 (TmuxAgentHandler consumes codec for response extraction) ✅ —
  commit `2927c90`.

**CLI surface cleanup (today)** — commit `1b24957`
- `kilroy attractor ...` is gone. Top-level surface only: `run`, `runs`,
  `validate`, `status`, `resume`, `stop`, `ingest`, `serve`, `review`,
  `modeldb`. Stub on the old subcommand prints an error and exits 2.
- `kilroy run` now accepts both shapes: workflow-name (`kilroy run
  implement`) and direct mode (`kilroy run --graph X.dot` /
  `kilroy run --package <dir>`). Direct mode preserves the engine's
  full flag surface (`--detach`, `--config`, …).
- `--preflight` and `--test-run` retired; `--validate` is the only name.
- README, AGENTS, demos, skills, helper code all swept to the new surface.

**Drift / parser bugs fixed today**
- `extractFuncBody` in `cmd/kilroy/help_usage_drift_test.go` was counting
  `{` inside string literals like `"{"`, overshooting into adjacent
  functions. Now tracks lexical state (string / rune / raw-string /
  line-comment / block-comment) before counting braces.
- `parseDuration` in `internal/attractor/engine/handlers.go` accepted
  "1ms" as 1 second because `fmt.Sscanf("%d", "1ms")` returns `(1, true)`.
  New `parseAllDigits` helper requires every rune to be a digit before
  the bare-seconds path. Commit `b22374f`.

**Test status**
- `./cmd/kilroy/...` green.
- `./internal/attractor/...`, `./internal/cli/...`, `./internal/llm/...`
  green when given enough timeout (engine pkg needs ≥ 230 s with
  `-short`; the 180 s default times out).

---

### What's next

Per Matt's last message: **"then we'll do work loop runs"** — the
parallel-worker work loop is the next major task.

Concrete options:

1. **Work-loop runs** (Matt's stated next step). Use `kilroy run
   <workflow> --detach` to spin up parallel workers across
   whichever in-flight items we want to grind on. Quick-launch workflow
   already exists; main thing is picking the queue.

2. **Stabilization follow-ups** (lower priority, don't block work loop):
   - Pre-existing failures still red on main:
     `TestRunWithConfig_ForceModel_BypassesCatalogGate`,
     `TestRunWithConfig_AllowsKimiAndZai_WhenCatalogUsesOpenRouterPrefixes`.
   - Engine pkg test wall time: 229 s under `-short`. Worth a look at
     whether any heavy fixtures can be skipped or parallelized.

3. **Block 6 Step 4+ (if/when planned)** — codec consumers beyond
   tmux. Not on the immediate path; revisit after work-loop runs.

---

### Pickup checklist for the next session

- [ ] Read this file.
- [ ] Confirm with Matt which queue to grind on for work-loop runs.
- [ ] Check `git status` — there are some untracked files (`.opencode/`,
      `inspiration/`, `result.md`, `E2E_MARKER.md`) that may or may not
      be intentional. Ask before adding.
- [ ] If running tests: use `-timeout 300s` for the engine pkg.
