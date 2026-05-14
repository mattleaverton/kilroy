# Using Kilroy From Another Repo

This guide is for a human or agent using Kilroy as a worker runner in an
ordinary project repository. It is not the Kilroy self-development playbook.

Kilroy's public alpha surface is:

```text
kilroy list | describe <name> | check <name>
kilroy run <workflow> [--input-file KEY=PATH ...] [--label K=V ...] [--sync] [--in-place] [--pretty]
kilroy runs list | show <id> | wait <id>
kilroy status [--logs-root <dir> | --latest] [--watch]
kilroy auth defaults | init | list | check | set | prefer | remove-source | suggest-fix
kilroy policy list | show <class> | resolve <class> | prefer | pin | clear | overrides | explain <run-id>
```

`kilroy run` is async by default. It runs launch validation before detaching; if
validation fails, no worker starts. On success it prints a JSON run handle with
`run_id`, `logs_root`, and `prelaunch`. Use `--sync` only when the caller should
block until the run finishes.

## Current Alpha Status

Fully supported today:

- Discover built-in and local workflow packages with `kilroy list`.
- Inspect workflow inputs, outputs, side effects, and classes with
  `kilroy describe <name> --pretty`.
- Validate a workflow against graph, package, class, auth, and local binary
  checks with `kilroy check <name> --pretty`.
- Launch runs with labels, inspect them later by label, and read collected
  outputs from the run database.
- Launch read-only workflows directly in the source repo with `--in-place` when
  the worker must see uncommitted or untracked files.
- Route agent nodes through policy classes such as `hard_coding`,
  `deep_investigation`, `quick_easy`, and `architectural_critique`.
- Initialize and diagnose auth chains with `kilroy auth init`, `auth list`,
  `auth check`, and `auth suggest-fix`.
- Manage global env-var auth mappings with `kilroy auth set`,
  `auth prefer`, `auth remove-source`, and `auth init --rescan`.
- Override class routing at the global or project level with
  `kilroy policy prefer`, `policy pin`, and `policy clear`.
- Override limited runtime/tool config per project with
  `<project>/.kilroy/config.toml`.

Partially supported today:

- Auth management is env-var based. Kilroy can add, reorder, remove, and
  rescan env sources in global auth config, but it does not store key values,
  rotate keys, or log into providers.
- Policy overrides are class-targeted preferences/pins. They can choose an
  existing policy candidate by model and optional driver, but they do not let a
  downstream repo invent an arbitrary model/provider tuple.

Not supported today:

- There is no `kilroy discover` command. Use `kilroy list`.
- There is no `kilroy policy init/copy/validate` file-management workflow.
  Use `policy prefer`, `policy pin`, `policy clear`, and `policy overrides`.
- A downstream repo cannot define an arbitrary new class fallback chain without
  changing Kilroy itself. Local workflows should choose existing classes and
  use project policy overrides only when the existing chain order is wrong for
  that repo.

## First-Time Setup

From a local Kilroy checkout, install or refresh the binary, built-in
workflows, and first-party agent skills:

```bash
./scripts/install.sh
```

The installer copies `kilroy` to `~/.local/bin/kilroy`, refreshes built-in
workflows under `$XDG_DATA_HOME/kilroy/workflows`, and installs Kilroy skills
into `~/.claude/skills`, `~/.codex/skills`, `~/.agents/skills`, and
`~/.config/opencode/skills`.

Then run these from the project where Kilroy will be used:

```bash
kilroy auth init
kilroy auth list --pretty
kilroy auth check --pretty
kilroy list --pretty
kilroy describe implement --pretty
kilroy check implement --pretty
kilroy policy list
```

`auth init` writes `~/.config/kilroy/auth.toml` from Kilroy's template and the
credentials detected on the machine. Detected sources are active TOML entries.
Undetected but supported sources are written as comments that can be
uncommented after adding the env var or logging into the CLI tool.

For separate Kilroy budgets, prefer `_KILROY` env vars:

```bash
export ANTHROPIC_API_KEY_KILROY=...
export OPENAI_API_KEY_KILROY=...
export GEMINI_API_KEY_KILROY=...
```

Then rerun:

```bash
kilroy auth init --rescan
kilroy auth check --pretty
kilroy policy resolve hard_coding
```

## Factory Workflow

Use the default surface as a small factory line: plan the seed, run the coding
loop, then validate the result. Tag every run with the same session label so the
whole conversation stays auditable.

1. Create a session workspace outside the repo.

```bash
SESSION=dark-mode-$(date +%Y%m%d%H%M%S)
TASK_ROOT="${XDG_STATE_HOME:-$HOME/.local/state}/kilroy/sessions/$SESSION"
mkdir -p "$TASK_ROOT"
$EDITOR "$TASK_ROOT/goal.md"
```

2. Run `plan` first.

```bash
kilroy run plan \
  --input-file goal="$TASK_ROOT/goal.md" \
  --label session="$SESSION" \
  --label phase=plan \
  --label wave=1 \
  --label task=dark-mode \
  --label scope=theme
```

`plan` writes `plan-status.json`, `task-packet.md`, `testing-plan.md`, and
`validation-plan.md`. On a first pass it normally returns `NEEDS_CLARIFICATION`
unless the user explicitly said to proceed with best judgment. Add answers as a
`clarifications` input and rerun `plan` until the status is
`READY_TO_IMPLEMENT` or `NEEDS_DECOMPOSITION`.

Treat `plan-status.json` as authoritative. Do not pass the other artifacts to
`implement` unless the status is `READY_TO_IMPLEMENT`; for non-ready statuses
they may be placeholders for human review.

3. Run `implement` from the plan artifacts.

```bash
kilroy run implement \
  --input-file task_packet="$TASK_ROOT/task-packet.md" \
  --input-file testing_plan="$TASK_ROOT/testing-plan.md" \
  --input-file validation_plan="$TASK_ROOT/validation-plan.md" \
  --input-file verify_command="$TASK_ROOT/verify-command.txt" \
  --label session="$SESSION" \
  --label phase=implement \
  --label wave=2 \
  --label task=dark-mode \
  --label scope=theme
```

`implement` is the public coding loop. It plans one small step, codes it,
verifies, critiques, and repeats until the critic says `COMPLETE` or the loop
cap is reached. It emits `result.md`, `STATUS.md`, and `implementation.patch`.
Before launching, write `verify-command.txt` from the testing plan or actual
package scripts. For JavaScript repos, prefer existing scripts such as
`turbo:check`, `check`, `typecheck`, `lint`, or `test`; do not invent npm
script names.

4. Integrate or continue from the run worktree, then run `validate`.

```bash
kilroy run validate \
  --input-file task_packet="$TASK_ROOT/task-packet.md" \
  --input-file validation_plan="$TASK_ROOT/validation-plan.md" \
  --input-file validation_command="$TASK_ROOT/validation-command.txt" \
  --label session="$SESSION" \
  --label phase=validate \
  --label wave=3 \
  --label task=dark-mode \
  --label scope=theme
```

`validate` writes `evidence.md` and `evidence.json` with terminal state
`PR_READY` or `FAILED_VALIDATION`. If validation fails, feed the evidence back
into another `implement` run from the same worktree or from an integrated branch.
The `validation_command` input may contain multiple lines; Kilroy executes it
as a shell script and stops on the first failing command.

5. Survey and inspect by session.

```bash
kilroy runs list --label session="$SESSION" --pretty
kilroy runs show --latest --label phase=implement --outputs
kilroy runs show --latest --label phase=implement --print result.md
kilroy runs show --latest --label phase=validate --print evidence.md
```

`kilroy runs show` reports `worktree`, `run_branch`, `logs_root`, `outputs`,
and `final_sha` when available. Inspect outputs and the diff before bringing
worker changes back to the original repo.

## Choosing Workflows

The default `kilroy list` shows only the three demo workflows.

| Workflow | Use when |
|---|---|
| `plan` | You have a raw user goal and need classification, clarification, a task packet, a testing plan, and a validation plan. |
| `implement` | You have a task packet and want the planner/coder/critic loop to produce code plus `implementation.patch`. |
| `validate` | You have a worktree, branch, or patch to prove against the task packet and validation plan. |

Add `--all` when you deliberately want internal station workflows such as
`investigate`, `fix`, `review`, `build-test`, `coding-loop`,
`coding-relay`, `implement-codex`, or `implement-oneshot`.

Workflow discovery order is:

1. `KILROY_WORKFLOW_PATHS` (colon-separated directories)
2. `<project-root>/.kilroy/workflows/`
3. `$XDG_CONFIG_HOME/kilroy/workflows/`
4. `$XDG_DATA_HOME/kilroy/workflows/`
5. source-checkout `workflows/` for development binaries

The project root is the nearest ancestor with a `.kilroy/` directory. Set
`KILROY_PROJECT_ROOT` to force a root; Kilroy errors if that directory does not
contain `.kilroy/`.

## Authoring Local Workflows

Put local workflows in the project:

```text
.kilroy/workflows/my-workflow/
  workflow.toml
  graph.dot
  prompts/
  scripts/
```

Minimal `workflow.toml`:

```toml
[workflow]
name = "my-workflow"
version = "1"
description = "Do one focused thing."
default_class = "hard_coding"
graph = "graph.dot"

[inputs.prompt]
type = "string"
required = true

[outputs.result]
type = "path"
path = "result.md"
```

Minimal class-routed graph:

```dot
digraph my_workflow {
  graph [
    inputs="prompt",
    outputs="result.md",
    model_stylesheet="* { agent_class: hard_coding; }"
  ]

  start [shape=Mdiamond]
  agent [shape=box, agent_class="hard_coding", prompt="Read $input.prompt and write result.md."]
  done  [shape=Msquare]

  start -> agent
  agent -> done [condition="outcome=success"]
}
```

Validate before launching:

```bash
kilroy check my-workflow --pretty
```

Use `agent_class`, not raw model IDs, for normal workflows. The validator
rejects raw `llm_model`, `llm_provider`, and `agent_tool` declarations in
`model_stylesheet`. Some legacy node-level raw routing still exists for built-in
exercises, but new workflows should use classes so routing follows Kilroy's
policy recommendations as they change.

## Auth State

Kilroy has two auth layers:

- User config: `~/.config/kilroy/auth.toml`
- Project override: `<project>/.kilroy/auth.toml`

The product auth-management commands write the global user config only. The
resolver still honors a project auth file if one exists, but new setup should
prefer global env-var mappings unless there is a deliberate compatibility need
for project auth.

Project entries replace user entries by binding key or chain name. Source lists
are not merged across layers.

Useful commands:

```bash
kilroy auth defaults
kilroy auth init --force
kilroy auth init --rescan
kilroy auth set openai --env OPENAI_API_KEY_KILROY
kilroy auth prefer openai/api_key OPENAI_API_KEY_KILROY
kilroy auth remove-source openai/api_key OPENAI_API_KEY
kilroy auth list --pretty
kilroy auth list --chains --pretty
kilroy auth check --pretty
kilroy auth doctor --pretty
kilroy auth check --project /path/to/project --pretty
kilroy auth suggest-fix
```

If auth is broken:

1. Run `kilroy auth list --pretty` to see discovered credentials.
2. Run `kilroy auth list --chains --pretty` to see which chains reference them.
3. Set the missing env var or run the provider CLI login flow.
4. Run `kilroy auth init --rescan` to add newly detected template sources.
5. Use `auth set`, `auth prefer`, or `auth remove-source` to map or reorder env
   sources without hand-editing TOML.
6. Run `kilroy auth check --pretty`.

`auth set` and `auth prefer` never store the key value. They store the env var
name Kilroy should read at check/run time.

Current caveat: `opencode` account/session storage is a separate auth surface.
For env-key routes, Kilroy prelaunch and run-config auto-detection honor
`_KILROY` variants before canonical provider env vars, so
`KIMI_API_KEY_KILROY` is enough to launch an opencode/Kimi workflow.

## Policy State

Policy maps abstract classes to ordered fallback chains of concrete model,
driver, provider, and auth requirements. Workflows should ask for classes:

- `hard_coding`
- `quick_easy`
- `deep_investigation`
- `architectural_critique`
- `frontend_aesthetic`
- tool-specific classes such as `coding_codex_apikey`

Useful commands:

```bash
kilroy policy list
kilroy policy show hard_coding
kilroy policy resolve hard_coding
kilroy policy prefer hard_coding gpt-5 --scope project
kilroy policy pin hard_coding gpt-5 --scope global
kilroy policy clear hard_coding --scope project
kilroy policy overrides --json
kilroy policy explain <run-id>
```

Resolution precedence is:

```text
embedded Kilroy policy < global policy override < project policy override
```

`policy resolve` auto-discovers the nearest `.kilroy/` project root and reports
override provenance as `policy_source` / `override_mode` in JSON output. The
`--project <dir>` flag is still available for tests and scripts.

Override files are hidden implementation state:

- Global: `~/.config/kilroy/policy-overrides.toml`
- Project: `<project>/.kilroy/policy-overrides.toml`

Do not hand-author those files for normal use. Use `policy prefer` when you want
a model moved to the front of a class chain. Use `policy pin` when you want only
that model's matching policy candidates to be considered.

If a local workflow needs a routing setup that no current class or candidate
represents, the alpha answer is still to pick the closest class or change
Kilroy's embedded policy in `internal/policy/data/policy.toml`.

## Config State

General Kilroy config is separate from auth:

- User config: `~/.config/kilroy/config.toml`
- Project override: `<project>/.kilroy/config.toml`

Precedence is:

```text
built-in defaults < user config < project config < environment < CLI flags
```

Current config keys are limited to runtime timeouts/retries, CXDB UI settings,
and selected tool paths. Auth chains and policy classes are not configured in
`config.toml`.

## Run State and Labels

Every run is recorded in Kilroy's local run database. Labels are the main way a
director keeps related worker runs together:

```bash
kilroy run plan \
  --input-file goal="$TASK_ROOT/goal.md" \
  --label session=alpha-docs \
  --label phase=plan \
  --label wave=2 \
  --label task=T-101 \
  --label scope=docs

kilroy runs list --label session=alpha-docs --pretty
kilroy runs list --label wave=2 --status running --pretty
kilroy runs show --latest --label task=T-101
```

Use stable labels such as:

- `session=<slug-or-ulid>`
- `phase=plan|implement|validate`
- `wave=<N>`
- `task=<id>`
- `scope=<area>`

Run artifacts live under `logs_root`. Common files include `manifest.json`,
`prelaunch_validation.json`, `final.json`, `outputs.json`, `progress.ndjson`,
per-stage `prompt.md`, per-stage `response.md`, and collected files under
`outputs/`.

## Legacy And Advanced Surfaces

These commands and flags are real but not the preferred agent-worker path:

- `kilroy run --graph <file.dot>`
- `kilroy run --package <dir>`
- `--config <run.yaml>`
- `--run-id <id>`
- `--logs-root <dir>`
- `kilroy ingest`
- `kilroy resume`
- `kilroy stop`
- `kilroy serve`

Use them for Kilroy development, tests, migration work, or explicit operator
requests. For normal repo worker usage, prefer packaged workflows discovered by
name.
