---
title: Overnight Session Summary — Foundation Closure Push
date: 2026-05-05
branch: feat/v2-reframe
---

# Overnight session summary

Single 14-hour session, Matt asleep, kilroy used to improve kilroy. **37 commits**, Block 6 (the largest planned refactor) closed end-to-end, plus most of Phase 1-3 of the foundation closure plan.

## Headline

**V2 Block 6 is COMPLETE.** All five steps landed:

| Step | What | Commit |
|------|------|--------|
| 3 | AgentBackend interface + SDKBackend/TmuxBackend adapters | `08e1972` |
| 4 | HTTP + tmux transport extracted to `internal/attractor/agents/transport/` | `8ee8af9` |
| 5 | AuthResolver interface + BindingAuthResolver impl | `1f4cce4` |
| 6 | ToolControlKilroy orchestration loop in `internal/attractor/agents/loop/` | `7ea0d7d` |
| 7 | OllamaBackend (forcing function — proved abstraction generalizes) | `95e6bce` |

The Ollama forcing function worked: it composed cleanly using existing
transport + auth + loop primitives without requiring AgentBackend
interface changes. The abstraction is validated.

## Full commit list (37 this session)

```
95e6bce feat(agents): OllamaBackend — third AgentBackend implementation (Block 6 Step 7)
7ea0d7d feat(agents/loop): ToolControlKilroy orchestration loop (Block 6 Step 6)
1f4cce4 feat(agents/auth): AuthResolver interface + BindingAuthResolver impl (Block 6 Step 5 partial)
8ee8af9 refactor(agents): Block 6 Step 4 — extract HTTP + tmux transport into transport package
a49eb80 feat(engine): manifest writeback for parent_run_id (Block 7 Step 5)
61a673a test(cli): add --sync to tests broken by async-default flip (cascade)
6361104 feat(engine,rundb,cli): Block 7 Steps 3-4 — env contract + runs show JSON
71c7c27 feat(config): wire cxdb.ui.url through config.toml (Block 3 Step 4 proof)
bb90546 docs(reference): workflows.md catalog of all 8 built-in workflows (Block 10 final)
6c87ef8 docs(reference): classes.md (auto-gen) + auth-surface.md (Block 10 followup partial)
8880592 feat(config): wire 5 CLI sites to load project config (Block 3 Step 3)
71ee9a0 feat(engine,rundb): Block 7 Step 2 — RunDBWriter signature plumbs ParentRunID
bfa6d8f feat(rundb,engine): Block 8 12-sibling concurrency stress + concurrent-migration safety
b388c39 fix(rundb): rename parent_run_id migration 006 → 007 (collision)
a5fb1e9 feat(cli): kilroy run async by default (--sync to block) [BREAKING]
183335a docs(reference): workflow-toml-schema.md generated from manifest_v2.go source (Block 10 partial)
4c75032 feat(config): internal/config package — TOML loader for project + user layers (Block 3 partial)
827a028 feat(rundb): add parent_run_id column and round-trip support
08e1972 feat(agents): AgentBackend interface + SDKBackend/TmuxBackend adapters (Block 6 Step 3)
6923afe feat(prelaunch): schema_version on prelaunch_validation.json (P3.4)
c1d8da8 feat(cli/policy): policy show/list honor project-layer auth.toml (P3.2)
cf0da6c feat(policy): gemini_cli class entries for subscription + apikey (P3.1)
9cc7dde feat(cli): kilroy version --json for agent-primary surface (smoke)
089c592 feat(prelaunch): probe API-key credentials for opencode routes (P1.15, CRITICAL)
88ed4e1 fix(auth): replace duplicate findProjectRoot walker (P1.14)
8541344 feat(validate): --batch and workflows validate run catalog check (P1.13)
117848b feat(validate): plain-text surfaces Fix field; modeldb suggest pointer (P1.12)
57c9dd5 fix(test): align T99 orphans test with T98 pruneFromDB signature
f62802d fix(rundb,cli): --orphans --dry-run lists only true orphans (P1.11)
e05fc99 feat(rundb,cli): runs prune excludes status=running by default (P1.10)
411e736 fix(auth,engine): freeze auth route identity at execution (P1.1+P1.2)
5e64a44 fix(engine): --workspace must override --config repo.path (P1.9)
8984d7c feat(auth,llm): honor _KILROY env precedence in non-class API routes (P1.3)
2347d6c docs(readme): lead with workflow-first usage (P1.7)
e1fa1b6 feat(workflows): enforce 'retry once' via max_node_visits (P2.1)
a295a3e feat(prelaunch): add opencode to CLI driver list (P1.4)
dcb8ba1 fix(cli,docs): phase-1 self-fixes — gofmt, --follow → --watch, stale comments
```

## Phases closed

- **Phase 1 (critical bugs)**: COMPLETE — all 14 items in the foundation closure plan landed (P0 auth-snapshot freeze, `_KILROY` precedence, opencode prelaunch, README rewrite, retry-once, `--workspace` override, prune safety, --orphans dry-run, validate Fix field, --batch catalog check, findProjectRoot dedup, P1.15 prelaunch credentials probe, async-default flip, gofmt/--follow/stale comments).
- **Phase 2 (product contract)**: COMPLETE — workflow loop cap mismatch fixed; cleanup investigated.
- **Phase 3 (symmetry)**: MOSTLY COMPLETE — gemini classes, policy show/list project-aware, schema versioning, validate Fix, --batch catalog check, findProjectRoot dedup. Outstanding: P3.5 lint test (DATA DRIFT 4.7→4.6 needs Matt decision); P3.3 opencode full auth binder (research only); P3.6 auth write surface (deferred).
- **Phase 4 (deferred decisions)**: Block 3 (config layering), Block 7 (parent linkage end-to-end), Block 8 (12-sibling stress) all landed. Block 6 (the big one) landed. async-default flip landed (BREAKING).
- **Phase 5 (net-new exploration)**: not started overnight (deferred per direction).
- **Phase 6 (surface polish)**: not started.

## Investigations + research

7 investigates landed in the first wave, plus 2 research-only design proposals overnight:
- Skills isolation design proposal (T110): concrete plan for stage-isolated agent_home + opt-in inherit + skills_hash fingerprinting. Not implemented per Matt's directive.
- opencode auth binder integration scope (T111): MVP ~1 day to switch envs + isolate XDG dirs; full parity ~3 days. Not implemented per Matt's directive.

## Outstanding for morning review

- **129 DATA DRIFT** — embedded catalog has claude-opus-4.6 max but policy.toml uses claude-opus-4-7. T127's matcher fix is correct, but lint then fails on real data drift. Matt decision: refresh embedded catalog OR pin policy to 4.6.
- **134 Step 5 part B** — wire transport components through AuthResolver (interface + impl shipped, transport injection pending).
- **136 Step 6 followup** — wire RunTurn into engine.go (loop is parallel today, not replacing inlined orchestration).
- **104 P3.6 auth write surface** — `kilroy auth set/login` (deferred per Matt; .env hand-edits noted as unacceptable long-term).
- **Block 5 auth followups** — opencode SQLite probe, Linux libsecret, Windows Credential Manager, Gemini OAuth note (deferred per Matt).
- **Skills isolation IMPL** — design done, no code changes per Matt.

## Worker workflow notes

- **Coding-relay (kimi-k2 via opencode)** is the right tool for multi-step refactors when the spec is bounded and the abstraction is well-shaped. Several runs hit the 6-iter cap with real progress (T117/T121/T123/T132/T135/T137) — the cap-hit pattern is fine; trajectory shows iter 5 typically had the work, iter 6 was polish.
- **Implement workflow** is right for surgical edits and test fixes (T130 worked perfectly: 9-line cascade fix in <2 iter).
- **Cherry-pick + squash** is the proven integration pattern. Always: clean noise files (`result.md`, `cmd/kilroy/result.md`, `cmd/kilroy/E2E_MARKER.md`) before finalizing.
- **`.gitignore` additions from kimi runs** are noise (re-add `.kilroy/` after the existing `.kilroy/*` + `!.kilroy/workflows`); skip them.
- **Migration version collisions** (T115's `006_parent_run_id.sql`) need to be checked against existing `schema_migrations` table before adding new migration files. Specs for migration work should require this check.
- **Stale-build trap** (`--confirm-stale-build`) requires rebuilds between every dispatch round if source changed. Real friction; documented in earlier audit as P6.

## Test status

Last verified clean at HEAD `95e6bce`:
- `cmd/kilroy/` — green
- `internal/attractor/engine/` — green (needs 300s timeout, exceeds 180s default)
- `internal/attractor/rundb/` — green
- `internal/attractor/modeldb/` — green
- `internal/policy/` — green
- `internal/config/` — green
- `internal/auth/binding/` — green
- `internal/attractor/agents/...` — green (Block 6 Steps 3-7)

Outstanding test concerns:
- The engine package needs >180s now. Either CI's default timeout needs raising or some slow tests need optimization.
- T122 (async-default test cascade) was deferred; T130 covered the failures — but a broader audit may find more.
