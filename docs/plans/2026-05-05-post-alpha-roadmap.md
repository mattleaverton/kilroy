# Post-Alpha Roadmap

Date: 2026-05-05

## Why this plan exists

This is the work that comes after the foundation closure (agent dispatch consolidation, layered configuration, run isolation). It is split into two priorities:

- **Higher priority** — features that make the product usable by someone other than its author, complete the per-run flexibility surface, and expand horizontal capability with new drivers and a visualization layer.
- **Lower priority** — items that polish edges, harden production claims at scale, expand cross-platform coverage, or clean up vestigial surfaces.

Each item is described to stand alone — what it is, why it matters, what "done" looks like. No dependency assumptions across items unless explicitly stated.

---

# Part 1 — Higher priority

## Auth as a real product surface

Today auth is a read-only surface. Users edit `~/.config/kilroy/auth.toml` directly or set shell environment variables. There is no imperative way to add, change, or remove credentials. This is the largest onboarding scar after workflow discoverability.

### Auth write surface

Add an imperative interface for managing credentials. Two shapes are viable and both may be useful:

- `kilroy auth set <provider> --api-key <value>` — flag-driven imperative, suits scripted setup.
- `kilroy auth login --provider <name>` — interactive oauth dance for providers that support it (anthropic, google, openai), suits human first-run setup.
- `kilroy auth remove <provider>` — explicit removal.

Decide which shapes ship together. The interactive `login` flow is the larger UX shift; the imperative `set` is the smaller code change. Reasonable to ship `set` and `remove` first, then add `login` when oauth flow design is settled.

Done when: a new user can configure all supported providers without editing `auth.toml` by hand. `kilroy auth check` passes after a guided setup that uses only the new commands.

### opencode auth binder integration

The opencode tool today is not auto-detected by the auth resolver. Workflows that target opencode require explicit `--config` invocation. Wire opencode into the binder so its sessions are discovered, surfaced in `kilroy auth list`, and validated by `kilroy auth check`.

Done when: opencode appears in `kilroy auth list` with its session source, `kilroy auth check` validates an opencode session without manual config plumbing, and a workflow targeting opencode launches without `--config`.

### `kilroy auth init --rescan`

Today `kilroy auth init` is one-shot: it generates a config from detected sources at first run. Add an incremental rescan that picks up new env vars, new CLI sessions, and new keychain entries without overwriting existing configuration. Useful for the common "I added a new credential, refresh the config" path.

Done when: `kilroy auth init --rescan` adds newly-detected sources to the config in place, preserves existing entries, and reports what was added.

---

## Per-run flexibility

### `--class <name>` per-run override flag

Override the workflow's `default_class` for a single invocation. Per-node `class=` declarations on the graph still win. Emit a stderr warning that the override is for ad-hoc and A/B use, not for stable production runs ("for stable runs, prefer a workflow with the right default class").

Useful for swapping the same workflow between a fast-cheap and a slow-thorough class without forking the workflow package.

Done when: `kilroy run implement --class quick_easy ...` runs the same shipped workflow on the alternate class, prelaunch resolution shows the override active, and a stderr warning is emitted.

### `kilroy runs follow <id>`

Blocking-with-progress polling. Same effect as `kilroy runs wait <id>` but emits node-status changes to stderr as they occur. Default tick 5s. Replaces hand-rolled polling loops in director scripts.

Done when: `kilroy runs follow <id>` blocks until the run terminates, prints stage transitions and current-node status as they happen, and exits with the run's terminal exit code.

### Review workflow stale-base fix

The shipped review workflow's stage-context script computes `git merge-base` against the local working-tree HEAD. If local main is stale relative to the remote, the diff handed to the reviewer doesn't reflect the real conflict surface, and the reviewer can produce a "merge clean" verdict on a PR that would actually conflict.

Fix: fetch `<remote>/<base>` before computing merge-base. Default remote: `upstream` if it exists, otherwise `origin`. Default base branch: the upstream-tracking branch of HEAD if set, otherwise `main`. Add `KILROY_REVIEW_BASE_REF` env override (`upstream/develop` or any explicit ref). On fetch failure, warn to stderr, fall back to local merge-base, and prepend a stale-base risk note to the diff header so the reviewer sees the caveat.

Done when: a review run on a stale-local-main checkout produces the correct diff against the remote tip, or visibly flags itself as stale-base when it cannot fetch.

### Chatty preflight on foreground stderr

When a run launches in foreground (the default async-default behavior), emit one line per agentic node before launch:

```
[preflight] <node>: class=<C> -> <provider>/<driver> (<model>)
```

Silent on JSON-output paths. Helps users see the resolution that prelaunch picked without rummaging through `prelaunch_validation.json`.

Done when: a foreground `kilroy run` shows the resolution table at start, JSON-output paths remain quiet, and the format is stable across CLI versions.

---

## Onboarding and packaging

### Embedded shipped workflows

Today the kilroy binary does not embed the shipped workflow packages. It reads them from disk via a search-path stack: `KILROY_WORKFLOW_PATHS` environment variable, then `<project>/.kilroy/workflows/`, then `~/.config/kilroy/workflows/`. From outside the source repository, users must symlink workflows into the user config directory or set the env var manually. This is the single largest install friction.

Fix: embed the shipped workflows in the binary using Go's embed package. The binary always has a built-in fallback. The existing search paths still take precedence so:

- A user with a customized workflow in `~/.config/kilroy/workflows/myname/` overrides the embedded copy.
- A developer with `KILROY_WORKFLOW_PATHS` set to the in-tree `workflows/` sees their edits live.
- A fresh install without any configuration finds shipped workflows automatically.

Done when: a fresh `go install github.com/danshapiro/kilroy/cmd/kilroy@latest` (or equivalent install path) followed by `kilroy run implement` works without any further configuration.

### Friendly workflow-not-found error

When `kilroy run <name>` or `kilroy workflows validate <name>` finds nothing, the error names the searched paths and includes a one-line recommendation. Today's behavior is a terse "workflow not found." Better:

```
no workflow named "myflow"
searched: KILROY_WORKFLOW_PATHS, /path/.kilroy/workflows, ~/.config/kilroy/workflows
suggestion: list available workflows with `kilroy workflows list`,
            or set KILROY_WORKFLOW_PATHS to a directory containing the workflow
```

Once embedded shipped workflows ship, the suggestion adjusts: shipped names are always present, so a not-found means a typo or a missing user-defined workflow.

Done when: a user who fat-fingers a workflow name gets an actionable error that names the searched paths and suggests a recovery step.

### Workflows list discoverability footer

The default pretty list view filters workflows marked experimental in their manifest. Today the filter is silent — the only hint is in `--help`. Print a footer to stderr after the table when the filter is active:

```
(N workflows hidden — pass --all to see them)
```

No filter behavior change.

Done when: `kilroy workflows list --pretty` prints the footer when items are filtered and stays silent when nothing is filtered.

---

## Workflow capability

### Workflow chaining

A run's output can feed into the next run as a directly-composed pipeline. Two viable shapes:

- CLI: `kilroy run <wf-A> --feed-into <wf-B>` — the output of wf-A becomes an input to wf-B, run sequentially.
- Manifest: `workflow.toml` declares an `[on_success.next]` chain pointing at another workflow package.

Either shape requires that the run integration surface be clean (the housekeeping-commit fix from the foundation work). Without that, chaining produces parent worktrees full of sub-run debris.

Done when: a director can declare a multi-workflow pipeline in a single `kilroy run` invocation, the chain executes in order, each step's output is available to the next, and the final result reports the chain.

---

## New driver capability

### tmux interactive Claude with Haiku polling driver

A driver that runs Claude in an interactive tmux session and polls a smaller Haiku model to monitor session state. The polling shape is novel: most drivers operate as request-response or streamed-response; this one keeps an open interactive session and uses a cheaper model to watch for tool calls or state transitions.

Time-boxed exploratory work. Validates the agent-backend abstraction on a non-standard execution shape (interactive plus polling).

Done when: a workflow can target this driver via class, the interactive session launches under tmux, the Haiku polling watcher detects state transitions, and the agent-backend abstraction handles the polling loop without leaking driver-specific concerns into the engine.

### Cursor CLI driver

Add Cursor's CLI as a supported coding agent. Same shape as `claude_cli`, `codex_cli`, `gemini_cli`. Discoverable by the auth binder, exercises the prelaunch validation surface, dispatched through the unified backend interface.

Done when: a workflow targeting `cursor_cli` (or whatever class name we settle on) launches under Cursor, prelaunch validates the binary and session, and a real run completes end-to-end.

### Minimal UI for run visualization

A small viewer (web or TUI) for live run state. Shows node-by-node status, prelaunch resolution, and the current stage's tool calls. Reads the existing CXDB event surface and the run's manifest. Read-only — no run control from the UI.

Done when: a user can run `kilroy ui` (or open a URL) and see live state for any in-progress or completed run, including the prelaunch resolution, the stage timeline, and the per-stage tool-call sequence.

---

# Part 2 — Lower priority

## Run-result structured outputs

### Structured `decision` field

Workflows declare expected outputs in `workflow.toml` under `[outputs]`. The last stage writes structured fields to `status.json`. `kilroy runs show --json` surfaces them at the top level. `kilroy runs list --decision merge` filters across runs.

Replaces the current pattern of awk-scraping markdown headings out of `result.md`.

Done when: a workflow author can declare an output schema, the run produces a structured payload, and `kilroy runs list` can filter on it.

### `kilroy run review --pr <url>` PR URL target

Auto-fetch base/head via `gh pr view --json baseRefName,headRefName`, set up the worktree from the PR head, and hand the diff to the review workflow. Replaces the manual `gh pr diff > /tmp/foo.patch` boilerplate that directors run today.

Done when: a user can review any GitHub PR with `kilroy run review --pr <url>` without staging the diff manually.

### `kilroy workflows install` subcommand

A symlink helper: copies (or links) shipped workflows from a source directory into `~/.config/kilroy/workflows/`. Useful before embedded workflows ship; mostly redundant after.

Done when: `kilroy workflows install` works for users on the pre-embed surface, and the team has decided whether to keep or remove it after embedded workflows land.

### Per-language verify auto-detect on the implement workflow

The `build-test` workflow already has detection logic for build systems (go, npm, cargo, mvn, etc.). Port that detection into the `implement` workflow's verify stage so `implement` runs the right check command for the project at hand without per-workflow customization.

Done when: `kilroy run implement` on a Go project runs `go build && go test`, on a Node project runs `npm test`, and similar for other detected build systems, with no per-project workflow editing.

---

## Concurrency and platform

### 12-sibling concurrency stress test executed and pinned

A concurrency stress test exists in the engine package. Whether it has been executed at the larger scale (12 simultaneous runs) and the latency / lock-contention claims pinned to measured data is incomplete. Run it, capture results, document the supported parallelism level.

Done when: there is a measured, documented level of concurrent runs the system supports, the stress test runs in CI (or on a documented schedule), and any failures it surfaces have either fixes or known-issue notes.

### rundb WAL writer-lock latency benchmark

Measure the latency of 10MB blob INSERT operations under WAL writer-lock contention. Establishes whether the rundb shape can handle the load implied by chained or many-sibling runs.

Done when: there are measured numbers, captured in a benchmark file that runs alongside other benchmarks.

### Isolated HOME for concurrent cli_session runs

Today the `claude_cli`, `codex_cli`, `gemini_cli` drivers share `~/.claude`, `~/.codex`, `~/.gemini` directories. Concurrent runs may collide on session files, history, or config. Investigate isolated HOME directories per-run for cli_session paths, or document that concurrency under cli_session is unsafe and provide a migration path.

Done when: either the engine launches each cli_session run with an isolated HOME, or there is a documented unsupported-concurrency claim and the prelaunch surface refuses to launch concurrent cli_session runs of the same provider.

### Parent-repo `.git/index.lock` retry policy

When sub-runs operate on a parent repository's git state, they can race the parent's index lock. Decide on a retry shape: bounded retry with backoff in the engine, or surface the failure to the workflow with a clear error.

Done when: concurrent runs sharing a parent repo do not produce mysterious "index.lock exists" failures; the chosen retry/error policy is documented.

### opencode SQLite auth probe

The opencode auth probe has a schema mismatch issue. Fix the probe so opencode session validation works reliably.

Done when: `kilroy auth check` validates opencode sessions correctly across the schema versions opencode ships in production.

### Linux libsecret keychain probe

Add a Linux credential source backed by libsecret. Cross-platform expansion.

Done when: a Linux user with credentials in libsecret can use `kilroy auth init` to detect them and `kilroy auth check` to validate them.

### Windows Credential Manager probe

Add a Windows credential source backed by Credential Manager. Cross-platform expansion.

Done when: a Windows user with credentials in Credential Manager can use `kilroy auth init` to detect them and `kilroy auth check` to validate them.

### `kilroy auth profile <name>` environment switching

Switchable named profiles for swapping between credential environments (personal vs work, dev vs prod). Each profile is a named auth.toml; switching is a CLI command that updates a pointer.

Done when: a user can maintain multiple credential profiles and switch between them with one command.

### macOS Keychain integration for auth

Optional credential source backed by the macOS keychain so users don't need to keep secrets in shell exports.

Done when: macOS users can store credentials in the keychain, `kilroy auth init` detects them, and `kilroy auth check` validates them.

---

## CLI polish

### Run --no-cxdb usage fallthrough

When `kilroy run --no-cxdb <args>` fails to parse or returns a usage error, the help text shown is the top-level CLI usage rather than the run subcommand's usage. Fix the dispatch ordering so the contextually correct usage is shown.

Done when: any failure inside `kilroy run --no-cxdb ...` shows run-subcommand usage, not top-level help.

### Gemini OAuth wording cleanup

The gemini auth probe surfaces a `"expired; refreshable"` note for tokens that will auto-refresh. Refreshable expiry is the normal state — surfacing it makes users think something is wrong. Drop the note.

Done when: gemini sessions that are auto-refreshable do not produce a "note" in `kilroy auth list` output.

### `--input-file` vs `--input` help disambiguation

Today the surface is `--input-file KEY=PATH` only. Help text should explicitly state that (a) the value is a file path, (b) there is no inline `--input KEY=VALUE` form. Reduces confusion when users mistype the flag.

Done when: the run subcommand's help unambiguously describes the input flag's semantics.

### Shared scripts/ helpers

Extract common shell utilities used by multiple shipped workflows (stage-context, fetch-diff, write-result) into a shared `scripts/` directory or sourced helper library. Reduces drift between workflow packages that all reimplement the same idioms slightly differently.

Done when: shipped workflows source a single helper for stage context, diff fetching, and result writing rather than duplicating the logic.

---

## Observability

### Typed-error output schema

A documented set of error codes and structured error payloads emitted by the CLI for machine consumption. Standard envelope, stable codes, machine-parseable.

Done when: the CLI's error-emission contract is documented, every error path uses a coded error, and downstream tooling (CI, scripts, agent-driven directors) can switch on codes without parsing English.

### Concurrency guarantees documentation

A reference document describing what kilroy claims and doesn't claim about parallel run safety. Written after the stress test pins facts (see the concurrency stress test item).

Done when: there is a documented set of claims a user can rely on, with measured backing for each claim.

### Nested run tree surfaced in `kilroy runs show`

Parent-child run relationships flow through the database today. The display side surfaces them in a tree view: `kilroy runs show <id>` shows children, depth, and rollup status.

Done when: `kilroy runs show <id>` for a run with children shows them in a readable tree, and the same data is available in `--json` form.

---

## Cleanup

### `kilroy serve` removal or keep decision

A subcommand exists with no current consumers. Either delete it or document a use case.

Done when: the subcommand is removed, or a documented use case is added with at least one consumer.

### Model-catalog suggest references

The model-catalog package was removed in a prior cleanup. Scrub any remaining references to `kilroy modeldb suggest` or related catalog terminology in help text, error messages, validate fix-its, and doc comments.

Done when: `grep -rn "modeldb\|model_catalog\|catalog suggest"` returns only historical/comment hits with explicit "removed" annotations, never active surface text.

### Cleanup command semantics

A `kilroy cleanup` (or similarly named) subcommand with documented dry-run semantics for purging old runs, orphaned worktrees, and stale logs. Investigation has been done; implementation is pending.

Done when: there is a CLI command that safely cleans accumulated state, with a default dry-run and an explicit `--commit` flag.

### Skills isolation implementation

A design exists for isolating skills (the `.claude/skills/` directory) at run time so workflows don't accidentally inherit skills from the host environment. Implementation is deferred.

Done when: workflows run with a controlled skills environment per the design, or the design is explicitly retired with a documented rationale.

---

## Documentation

### PR-triage playbook section in AGENTS.md

The investigate → implement → manual-push pattern that emerged from real director use is the recommended shape for PR triage and merge orchestration. Document it as a named playbook in AGENTS.md so future directors don't have to rediscover it.

Done when: AGENTS.md has a PR-triage section that names the pattern, describes the steps, and references the relevant CLI commands.

### Quick-launch graph upstream patch

A minor patch to a related project's quick-launch workflow graph file. Sits in someone else's repository — submit as an upstream PR.

Done when: the patch is submitted upstream and merged or explicitly rejected.

---

## Test infrastructure debt

### Engine package test wall-time optimization

The engine package test suite runs at roughly 220 seconds. Investigate parallelization, fixture sharing, or test-scope reduction to bring the wall time down.

Done when: the suite runs in materially less time without sacrificing coverage.

### Test-infra migration to Go APIs

Some advanced tests still invoke kilroy through legacy CLI flags (`--graph`, `--config`, `--workspace`). Migrate them to call the engine APIs directly so the tests don't depend on flag surfaces that may eventually be removed.

Done when: the test suite no longer uses the legacy flag surface to drive integration tests.

### Deprecation-table live entries

The deprecation surface (a mechanism for marking flags or surfaces as deprecated, with timed warnings and removal targets) exists in code but has no live entries today. Populate it as flags or surfaces actually deprecate.

Done when: deprecating something becomes a documented one-line entry rather than a custom code path.
