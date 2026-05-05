# AGENTS.md

Instructions for AI agents working on Kilroy itself.

## Prime directive

If you can read this, **you are improving Kilroy, not using it on a downstream project**. When Kilroy fails on a workflow, fix Kilroy so it works for *every* project — not the workflow, not the target repo, not the test fixture. The only exception is an explicit user instruction telling you otherwise.

## Director pattern

You are a **director**. Your job is to plan work, dispatch parallel runs of Kilroy itself against task specs, oversee execution, and integrate results back into the working branch.

### Loop

1. **Plan.** Read the user request. Decompose into discrete tasks. For each task, decide size: small (single file, ≤30 min), medium (one package, ≤2 hr), large (multi-package refactor, ≤1 day).
2. **Spec.** Write each task as a markdown file at `/tmp/kilroy-self-runs/<wave>/T-<id>-<slug>.md`. Required sections:
   - **Context** — current state, prior work, what's already landed
   - **What we want** — concrete deliverables
   - **Constraints** — what must not change, freeze invariants, scope limits
   - **Verification** — exact commands the agent must run green before claiming done
   - **Out of scope** — explicit non-goals
   - **Report** — what to put in `result.md`
3. **Dispatch.** Pick the workflow that fits the task (see below). Launch with `kilroy run <workflow> --input-file prompt=/path/to/T-XXX.md --label …`. Run multiple in parallel when independent.
4. **Oversee.** `kilroy runs list` to see status. `kilroy runs show <id>` for details. Use `kilroy status --latest --watch` to follow a single run live.
5. **Integrate.** When a run finishes:
   - Find the agent's commit: `git log --pretty='format:%h %s' main..HEAD | grep -vE 'attractor\(|critic:'`
   - Cherry-pick onto the working branch
   - Drop noise files (`result.md`, `cmd/kilroy/result.md`, `cmd/kilroy/E2E_MARKER.md`, `.gitignore` re-additions)
   - Build + test locally
   - Squash with a clean commit message

## Workflow selection

| Workflow | When | Iterations |
|---|---|---|
| `implement` | Surgical edit, well-scoped change with build+test verification (e.g. fix this test, add this flag, rename this function) | Single-shot + verify + retry-once |
| `fix` | Bug fix with reproduction. Small scope, focused root-cause work | Single-shot + verify + retry-once |
| `investigate` | Research/code-spelunking question. No code changes. Returns a research artifact | Read-only, deep_investigation class |
| `coding-relay` | Multi-step refactor with iterative planner→coder→critic→status loop. Right when scope is bounded but spans multiple commits | 6 iterations max |
| `review` | Review a change. Stages diff, reads context, returns a structured review | Single-shot |

`coding-relay` uses kimi-k2 via opencode for the coder stage, anthropic SDK for planner, codex for critic. Hitting the 6-iter cap is a normal trajectory, not a failure — iter 5 typically has the work, iter 6 is polish.

## Public CLI surface

Use only this shape in scripts, prompts, and automation:

```bash
kilroy run <workflow> [--input-file KEY=PATH ...] [--label K=V ...] [--sync] [--pretty]
kilroy workflows list | describe <name> | validate <name>
kilroy runs list | show <id> | wait <id>
kilroy status [--logs-root <dir> | --latest] [--watch]
kilroy auth defaults | init | list | check | suggest-fix
kilroy policy list | show <class> | resolve <class> | explain <run-id>
```

`kilroy run` is **async by default** — returns a run handle immediately. Pass `--sync` to block. Older docs may show `--detach` as default; that flipped — `--detach` is a no-op now.

`--graph`, `--package`, `--config`, `--run-id`, `--logs-root` are direct-mode escape hatches for ad-hoc work and tests, not the public path.

## Pre-commit checklist

Run all of these green before committing. They mirror CI:

```bash
gofmt -l . | grep -v '^\./\.claude/' | grep -v '^\.claude/'   # must be empty
go vet ./...
go build ./cmd/kilroy/
go test -timeout=300s ./...
```

The engine package now needs `>180s` (running ~220s); `300s` is the safe default.

## Auth

Setup on a new machine: `kilroy auth init` discovers env vars + CLI sessions, generates `~/.config/kilroy/auth.toml`. `kilroy auth check` verifies every chain has a usable source. `kilroy auth list --pretty` shows active resolution + shadowing.

Per-tool budget convention: each api_key chain has `<PROVIDER>_API_KEY_KILROY` listed before the canonical name. Setting `ANTHROPIC_API_KEY_KILROY` scopes Kilroy spend without affecting your daily Claude CLI use.

Snapshot semantics: prelaunch resolves each agentic node's class once and writes `prelaunch_validation.json`. Execution reads from that snapshot — env or config drift between prelaunch and execution can't silently change the route. The source value is re-read at execution (so a vanished env var fails decisively) but the source identity is frozen.

CLI driver materialization (load-bearing): `claude_cli`, `codex_cli`, `gemini_cli` on the cli_oauth route get `EnvScrub` — a list of canonical env vars that must be unset in the child process via `env -u`. Without this, a stray `ANTHROPIC_API_KEY` in shell would silently make the CLI use the env key instead of the logged-in subscription (silent wrong-billing).

## Known gotchas

- **Stale-build trap.** `--confirm-stale-build` is required if you don't rebuild between source changes. Always rebuild between dispatch rounds when source moved.
- **Async-default flip.** Tests asserting on artifacts immediately after `kilroy run` returns need `--sync`. If a test is flaky, check first.
- **kimi auth silent fail (was).** Before P1.15 (commit `089c592`), missing `KIMI_API_KEY` made coding-relay spin 6 iterations producing nothing. Now prelaunch dies in <1s. If you see this regress, file it as critical.
- **Shell snapshot inheritance.** Agent sessions may not see `.zshrc` exports added after the session started. If `kilroy auth check` shows missing keys you know are set, restart the session or write a repo-local `.env` (gitignored) with the missing values.
- **Migration version collisions.** New rundb migration files must check `schema_migrations` MAX(version) before assigning a number.

## Production safety

Never start a production run except as the user explicitly approved.

- Production runs (`llm.cli_profile=real`) are expensive. Routing decisions (provider, model, reasoning depth, API vs CLI) are cost-bearing.
- For any approved production command, execute exactly what the user asked. Do not change flags, env, config, paths, or `--run-id` unless explicitly approved.
- Per-run model overrides via CLI flag are gone. If the chosen model is wrong, fix the workflow or policy, not a flag.

## Repo layout

- `cmd/kilroy/` — CLI entrypoint and subcommands
- `internal/attractor/` — engine, runtime, validate, agents, policy
- `internal/agent/`, `internal/cxdb/`, `internal/llmclient/` — agent loop, CXDB integration, provider env wiring
- `internal/auth/binding/` — auth chain resolver
- `workflows/` — shipped workflow packages
- `scripts/` — operational helpers (e2e, cxdb start, benchmarks)

## Coding style

Idiomatic Go. `gofmt` before every commit. Lowercase package names, `CamelCase` exports, colocated `*_test.go`. Prefer explicit config over implicit behavior. Keep packages domain-focused (`engine`, `validate`, `policy`) — avoid cross-package leakage.

## Commit / PR style

`area: summary` or `type(scope): summary`. Examples: `engine/runtime: ...`, `feat(policy): ...`, `docs(plan): ...`. Keep commits narrow. PRs include intent, key files, validation commands, runtime impact.
