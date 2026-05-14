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
3. **Dispatch.** Pick the workflow that fits the task (see below). Launch with `kilroy run <workflow> --input-file KEY=/path/to/T-XXX.md --label …`. Run multiple in parallel when independent.
4. **Oversee.** `kilroy runs list --label wave=N` to see your wave's status. `kilroy runs show <id> --json` for details. `kilroy status --latest --watch` to follow a single run live. `kilroy runs wait <id> --timeout 1h` to block on a single run.
5. **Integrate.** When a run finishes:
   - Find the agent's code commit: `git log --pretty='format:%h %s' feat/v2-reframe..attractor/run/<run-id> | grep -vE 'attractor\(|critic:'` — the per-stage `attractor(<run-id>): agent` commit captures the change
   - Cherry-pick onto the working branch (`git cherry-pick --no-commit <hash>` so you can clean noise before committing)
   - Drop noise files (`result.md`, `cmd/kilroy/result.md`, `cmd/kilroy/E2E_MARKER.md`, `.gitignore` re-additions)
   - Build + test locally (use `go vet` between iterations; full suite only at the end)
   - Commit with a clean message — squash multi-commit work into one logical commit if needed

### Tagging conventions

Always tag dispatched runs so you can filter and audit them later. Standard labels:

- `scope=<short-slug>` — what work area this run targets
- `session=<slug>` — shared id for related plan/implement/validate runs
- `phase=plan|implement|validate|station` — where this run sits in the line
- `wave=<N>` — which dispatch wave this is part of (helps when multiple are in flight)
- `task=<id>` — the task ID from your spec file (e.g. `task=A`, `task=137`)
- `workflow=<name>` — auto-set by `kilroy run` from the workflow name; you can override

`kilroy runs list --label scope=foo --pretty` and `kilroy runs list --label wave=3 --status running` are how you survey state across many parallel runs.

## Workflow selection

| Workflow | When | Iterations | Default class |
|---|---|---|---|
| `plan` | Raw goal, unclear seed, or need for task/testing/validation packet | Single fast intake node | `quick_easy` |
| `implement` | Public coding loop from task packet + testing/validation plan | 20 iterations max | mixed direct routing + `quick_easy` |
| `validate` | Evidence collection against task packet and validation plan | Single script node | none |
| `implement-oneshot` | Internal surgical edit when you already have a tight prompt | Single-shot + verify + retry-once | `hard_coding` |
| `fix` | Internal bug-fix station with reproduction and patch output | Single-shot + verify + retry-once | `hard_coding` |
| `investigate` | Internal read-only research/code-spelunking station | Read-only | `deep_investigation` |
| `review` | Internal review of a diff/branch/patch | Single-shot | `hard_coding` |
| `build-test` | Internal no-LLM build/test detector and reporter | Single-shot | none |

### Picking the right one

- **Unclear or product-shaped request** → `plan` first. Use the generated task packet before coding.
- **Main demo path** → `plan` → `implement` → `validate`.
- **One file or one focused Kilroy self-change** → `implement-oneshot` is still available with `--all`.
- **A bug with a known repro** → `fix` remains an internal station when that framing is useful.
- **"How does X work?" or "Where is Y?"** → `investigate` remains the internal read-only station.
- **Need a structured opinion on a diff** → `review`.

Default `kilroy list` shows only `plan`, `implement`, and `validate`. Use
`kilroy list --all` to see internal station workflows.

## Local install / demo smoke

For a local checkout install, run:

```bash
./scripts/install.sh
```

The installer builds `kilroy`, copies it to `~/.local/bin/kilroy`, refreshes
the global built-in workflow directory, and installs first-party Kilroy skills
into all supported user-level agent skill roots:

- `~/.claude/skills`
- `~/.codex/skills`
- `~/.agents/skills`
- `~/.config/opencode/skills`

After reinstall, smoke from a non-Kilroy repo:

```bash
kilroy list --pretty
kilroy describe implement --pretty
kilroy check implement --pretty
kilroy auth init --rescan
kilroy auth list --pretty
kilroy auth list --chains --pretty
kilroy auth check --pretty
kilroy policy resolve hard_coding
kilroy policy resolve coding_codex_apikey
```

## Public CLI surface

Use only this shape in scripts, prompts, and automation:

```bash
kilroy list | describe <name> | check <name>
kilroy run <workflow> [--input-file KEY=PATH ...] [--label K=V ...] [--sync] [--in-place] [--pretty]
kilroy runs list | show <id> | wait <id>
kilroy status [--logs-root <dir> | --latest] [--watch]
kilroy auth defaults | init | list | check | set | prefer | remove-source | suggest-fix
kilroy policy list | show <class> | resolve <class> | prefer | pin | clear | overrides | explain <run-id>
```

`kilroy run` is **async by default** — validates before detach and returns a run
handle with `prelaunch` details. Pass `--sync` to block. Older docs may show
`--detach` as default; that flipped — `--detach` is a no-op now. Use
`--in-place` only for read-only workflows that must inspect the current working
tree instead of an isolated worktree. In-place workflows still write declared
outputs and Kilroy metadata in that tree.

`--graph`, `--package`, `--config`, `--run-id`, `--logs-root` are direct-mode escape hatches for ad-hoc work and tests, not the public path.

## Pre-commit checklist

Run all of these green before committing. They mirror CI:

```bash
gofmt -l . | grep -v '^\./\.claude/' | grep -v '^\.claude/'   # must be empty
go vet ./...
go build ./cmd/kilroy/
go test -timeout=300s ./...
```

## Test suite split

`go test ./...` is the default unit-test pass. It is the right command for
normal local iteration, CI, and quick verification unless the task explicitly
needs subprocess/workflow/tmux/server/corpus coverage. Integration tests skip by
default and only run when `KILROY_INTEGRATION=1` is set.

Run the full integration-enabled suite only when explicitly choosing to pay that
cost:

```bash
KILROY_INTEGRATION=1 go test -timeout=300s ./...
```

For targeted integration verification, keep the package and `-run` pattern
specific:

```bash
KILROY_INTEGRATION=1 go test -count=1 -timeout=300s ./internal/attractor/engine -run '^TestRunWithConfig_HeartbeatEmitsDuringAgent$' -v
```

`./scripts/e2e.sh` is the deterministic E2E/contract script. It opts into
integration tests internally before validating shipped DOT files:

```bash
./scripts/e2e.sh
```

## Authoring workflows

A workflow package is `workflows/<name>/` with at minimum `workflow.toml` + `graph.dot`. Optional: `prompts/` (prompt fragments referenced from nodes) and `scripts/` (shell helpers). Use a shipped workflow as a template — `workflows/implement-oneshot/` is the simplest coding representative, while `workflows/validate/` is the simplest script-only representative.

`workflow.toml` skeleton:

```toml
[workflow]
name              = "myflow"
version           = "1"
description       = "What this workflow does."
default_class     = "hard_coding"
graph             = "graph.dot"

[inputs.prompt]
type     = "string"
required = true

[nodes.agent]
class = "hard_coding"
```

`graph.dot` rules (enforced by validate):
- `model_stylesheet` may declare `agent_class:` only — `llm_model:` and `llm_provider:` are rejected
- Each agent node sets `agent_class="<known-class>"` — unknown classes fail validate with `unknown_agent_class`
- Class names come from `kilroy policy list`. If you need a class that doesn't exist, **add it to `internal/policy/data/policy.toml` rather than naming a raw model**

Use `kilroy describe <name> --pretty` to see how the loader interprets your manifest, and `kilroy check <name> --pretty` to run the full prelaunch validation.

## Providers and classes

Built-in providers:
- API + CLI: `openai`, `anthropic`, `google`
- API only: `kimi`, `zai`, `cerebras`, `minimax`, `inception`
- Aliases: `gemini`/`google_ai_studio` → `google`, `moonshot` → `kimi`, `z-ai` → `zai`

Built-in classes (run `kilroy policy list` for current snapshot):
- `hard_coding` — multi-file coding, max reasoning depth (default for `implement`/`fix`/`review`)
- `quick_easy` — small/simple tasks, faster cheaper model
- `deep_investigation` — research-only, large context window (default for `investigate`)
- `architectural_critique` — design review, opinionated trade-off analysis
- `frontend_aesthetic` — UI-shaped tasks
- `coding_codex_subscription` / `coding_codex_apikey` — codex CLI specifically
- `coding_gemini_subscription` / `coding_gemini_apikey` — gemini CLI specifically

Each class declares an ordered fallback chain in `policy.toml`. The first chain entry whose `requires` (provider + auth method, optionally tool) is satisfied on the host wins. `kilroy policy resolve <class>` shows which one.

## Auth

Setup on a new machine: `kilroy auth init` discovers env vars + CLI sessions, generates `~/.config/kilroy/auth.toml`. `kilroy auth check` verifies every chain has a usable source. `kilroy auth list --pretty` shows active resolution + shadowing.

Per-tool budget convention: each api_key chain has `<PROVIDER>_API_KEY_KILROY` listed before the canonical name. Setting `ANTHROPIC_API_KEY_KILROY` scopes Kilroy spend without affecting your daily Claude CLI use.

Snapshot semantics: prelaunch resolves each agentic node's class once and writes `prelaunch_validation.json`. Execution reads from that snapshot — env or config drift between prelaunch and execution can't silently change the route. The source value is re-read at execution (so a vanished env var fails decisively) but the source identity is frozen.

CLI driver materialization (load-bearing): `claude_cli`, `codex_cli`, `gemini_cli` on the cli_oauth route get `EnvScrub` — a list of canonical env vars that must be unset in the child process via `env -u`. Without this, a stray `ANTHROPIC_API_KEY` in shell would silently make the CLI use the env key instead of the logged-in subscription (silent wrong-billing).

## Known gotchas

- **Stale-build trap.** `--confirm-stale-build` is required if you don't rebuild between source changes. Always rebuild between dispatch rounds when source moved.
- **Async-default flip.** Tests asserting on artifacts immediately after `kilroy run` returns need `--sync`. If a test is flaky, check first.
- **kimi auth silent fail (was).** Before P1.15 (commit `089c592`), missing `KIMI_API_KEY` made coding-relay spin producing nothing. Now prelaunch dies in <1s. If you see this regress, file it as critical.
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
- `ui/server/` and `ui/web/` — peripheral local dashboard; build with `go build -o ./kilroy-ui ./ui/server` or run `go run ./ui/server --addr 127.0.0.1:8080`
- `workflows/` — shipped workflow packages
- `scripts/` — operational helpers (e2e, cxdb start, benchmarks)

The UI reads the default Kilroy run database directly and is for local run
inspection, policy/auth visibility, artifact browsing, and explicit maintenance
actions; keep it separate from the `kilroy` CLI serving surface.

## Coding style

Idiomatic Go. `gofmt` before every commit. Lowercase package names, `CamelCase` exports, colocated `*_test.go`. Prefer explicit config over implicit behavior. Keep packages domain-focused (`engine`, `validate`, `policy`) — avoid cross-package leakage.

## Commit / PR style

`area: summary` or `type(scope): summary`. Examples: `engine/runtime: ...`, `feat(policy): ...`, `docs(plan): ...`. Keep commits narrow. PRs include intent, key files, validation commands, runtime impact.
