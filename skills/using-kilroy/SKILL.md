---
name: using-kilroy
description: "Operate Kilroy Attractor pipelines end-to-end: ingest English requirements into DOT graphs, validate graph semantics, run and resume pipelines with run config files, configure provider backends (cli/api), and debug runs from logs_root artifacts and checkpoints."
---

# Using Kilroy

Kilroy is a local-first Attractor runner:

1. Generate a DOT pipeline from English requirements.
2. Validate DOT structure + semantics.
3. Run in an isolated git worktree with checkpoint commits.
4. Resume interrupted runs from logs, CXDB, or run branch.

> **If you only need to delegate a one-shot task to a single agent** (investigation, research, a small code change you don't need to supervise live), use `skills/quick-launch/` instead — it's the fire-and-forget workflow built on top of this command surface and handles tagging, detached launch, and result retrieval with one command each.

## Command Surface

Use these exact command forms:

```text
kilroy run <workflow-name> [--input-file KEY=PATH ...] [--input <path>] [--label KEY=VALUE] [--detach] [--wait] [--pretty] [--allow-test-shim] [--confirm-stale-build] [--no-cxdb] [--skip-cli-headless-warning] [--config <run.yaml>] [--run-id <id>] [--logs-root <dir>] [--workspace <dir>]
kilroy run --graph <file.dot> [--config <run.yaml>] [...]   # advanced ad-hoc graph mode
kilroy run --package <dir>    [--config <run.yaml>] [...]   # advanced ad-hoc package mode
kilroy resume --logs-root <dir> [--pretty]
kilroy resume --run-branch <attractor/run/...> [--repo <path>] [--pretty]
kilroy status [--logs-root <dir> | --latest] [--json] [--watch] [--interval <sec>]
kilroy stop --logs-root <dir> [--grace-ms <ms>] [--force]
kilroy runs list   [--json|--pretty] [--label KEY=VALUE] [--status STATUS] [--graph PATTERN] [--limit N]
kilroy runs show   (<id-or-prefix> | --latest [--label KEY=VALUE]) [--json] [--outputs] [--print <file>]
kilroy runs wait   (<id-or-prefix> | --latest [--label KEY=VALUE]) [--timeout <duration>] [--interval <duration>] [--json]
kilroy runs prune  [--before YYYY-MM-DD] [--older-than DURATION] [--graph PATTERN] [--label KEY=VALUE] [--orphans] [--dry-run | --yes]
kilroy workflows list | describe <name> | validate <name>  [--pretty | --all]
kilroy validate --graph <file.dot>            # static DOT validation
kilroy ingest [--output <file.dot>] [--model <model>] [--skill <skill.md>] [--repo <path>] [--max-turns <n>] [--no-validate] <requirements>
kilroy auth list | check | init | suggest-fix
kilroy policy list | show <class> | resolve <class> | explain <run-id>
kilroy serve [--addr <host:port>]
```

### Run flags you may not have seen before

- `kilroy run <workflow-name>` — the canonical form. Names resolve via filesystem discovery: `KILROY_WORKFLOW_PATHS` > `<project-root>/.kilroy/workflows/` > `$XDG_CONFIG_HOME/kilroy/workflows/`. The named workflow is materialized into the run's worktree at `.kilroy/package/`.
- `--graph <file.dot>` / `--package <dir>` — advanced ad-hoc modes. Use these only when you don't have a packaged workflow yet.
- `--input <path>` — structured inputs for the graph. Pass a path to a JSON or YAML file. (Inline JSON like `--input '{"k":"v"}'` is no longer accepted — write a file and pass its path.) Values become `KILROY_INPUT_KEY` env vars for tool nodes, `$input.key` placeholders in agent prompts, and sections in `.kilroy/INPUT.md`. Required inputs are declared via the workflow manifest's `[inputs.<name>] required = true`.
- `--input-file KEY=PATH` — read the file verbatim and assign it to the input key. Repeatable. The canonical way to inject multi-line text (prompts, specs, issue descriptions) without quoting or escaping. Replaces the old narrow `--prompt-file`.
- `--no-cxdb` — skip the content-addressed event store. Applied automatically when no `--config` is supplied.
- `--skip-cli-headless-warning` — bypass the interactive CLI-backend confirmation prompt. Applied automatically when stdin isn't a terminal.
- `--label KEY=VALUE` — attach a label to the run. Repeatable. Used by `runs list --label` and `runs prune --label`. Always tag detached runs so you can find them later.
- `--workspace <dir>` — override the workspace dir (default: cwd). If it's a git repo, the engine creates a dedicated run branch + worktree.

There is **no `--tmux` flag**. Driver dispatch is decided by the resolved
`agent_class` (or explicit `agent_tool=` / `llm_provider=`) — CLI drivers
(claude_cli, codex_cli, gemini_cli, opencode) automatically use tmux;
SDK drivers (anthropic_sdk, openai_sdk, google_sdk) use the HTTP API
path. Workflows control their routing intent in DOT and policy.

There is **no `--force-model` flag**. Strict model selection lives in
workflow/DOT/policy. If the chosen model is wrong, fix the workflow
or policy.

## Workflow

1. Run ingest:

```bash
kilroy ingest -o pipeline.dot "Build a Go CLI link checker"
```

2. Validate:

```bash
kilroy validate --graph pipeline.dot
```

3. Create run config (`run.yaml` or `run.json`).

4. Run:

```bash
kilroy run --graph pipeline.dot --config run.yaml
```

Optional pre-launch validation (validates everything, no stage execution):

```bash
kilroy workflows validate <workflow-name>
```

This runs the same DOT + package + class + auth + binary checks a real
launch does, minus execution. JSON by default; `--pretty` for human output.

5. If interrupted, resume from the most convenient source:

```bash
kilroy resume --logs-root <path>
```

6. For long runs, launch detached so work continues after shell/session exits:

```bash
./kilroy run --detach --graph pipeline.dot --config run.yaml --run-id <run_id> --logs-root <logs_root>
```

7. Observe run health and prelaunch validation:

```bash
./kilroy status --logs-root <logs_root>
cat <logs_root>/prelaunch_validation.json
tail -f <logs_root>/progress.ndjson
```

8. Intervene when a run is stuck or needs termination:

```bash
./kilroy stop --logs-root <logs_root> --grace-ms 30000 --force
```

## Runs: listing, inspecting, cleaning up

Every run (detached or foreground) is recorded in a local SQLite run database. Query it via `kilroy runs`:

```bash
# All runs, newest first
kilroy runs list

# Filter by label (repeatable tags on launch come back here)
kilroy runs list --label task=investigate-gadfly

# Machine-readable
kilroy runs list --json --status running --limit 10

# Full detail for one run (accepts unique prefix)
kilroy runs show 01KP646Y
kilroy runs show 01KP646Y --json

# Latest run matching a label (no id needed)
kilroy runs show --latest --label task=investigate-gadfly

# List just the declared output files
kilroy runs show 01KP646Y --outputs

# Stream a specific output file to stdout
kilroy runs show 01KP646Y --print result.md
kilroy runs show --latest --label task=investigate-gadfly --print result.md

# Block until a run reaches a terminal state
kilroy runs wait 01KP646Y --timeout 10m
kilroy runs wait --latest --label task=investigate-gadfly --timeout 10m

# Clean up old runs (dry-run by default; add --yes to actually delete)
kilroy runs prune --older-than 7d
kilroy runs prune --label experiment=true --yes
```

`runs show` output includes `worktree_dir`, `repo_path`, `run_branch`, and `logs_root` — use these to `cd` back into a finished run's workspace or feed them to other commands.

## Ingest Details

- Uses Claude CLI (`KILROY_CLAUDE_PATH` override, default executable `claude`).
- Default model: `claude-sonnet-4-5`.
- Default repo: current working directory.
- Default skill path auto-detection: `<repo>/skills/create-dotfile/SKILL.md`, then binary-relative fallbacks (for example `<kilroy-prefix>/share/kilroy/skills/create-dotfile/SKILL.md`) and Go module-cache roots from binary build metadata.
- If no skill file exists, ingest fails fast.
- `--max-turns` defaults to 15 when omitted.
- Validation runs by default; use `--no-validate` to skip.

## Validate Semantics

`attractor validate` runs parse + transforms + validators and fails on error-severity diagnostics.

Key checks:

- Exactly one start node and one exit node.
- Start has no incoming edges; exit has no outgoing edges.
- All nodes reachable from start.
- Edge conditions parse correctly.
- `llm_provider` required for codergen nodes (`shape=box`).
- `model_stylesheet` is optional, but if present must parse.

## Run Config (`version: 1`)

Required fields:

- `repo.path`
- `cxdb.binary_addr`
- `cxdb.http_base_url`
- `modeldb.openrouter_model_info_path`

Defaults:

- `git.run_branch_prefix`: `attractor/run`
- `modeldb.openrouter_model_info_update_policy`: `on_run_start`
- `modeldb.openrouter_model_info_url`: `https://openrouter.ai/api/v1/models`
- `modeldb.openrouter_model_info_fetch_timeout_ms`: `5000`

Minimal example:

```yaml
version: 1

repo:
  path: /absolute/path/to/repo

cxdb:
  binary_addr: 127.0.0.1:9009
  http_base_url: http://127.0.0.1:9010

llm:
  providers:
    openai:
      backend: cli
    anthropic:
      backend: api
    google:
      backend: api

modeldb:
  openrouter_model_info_path: /absolute/path/to/openrouter_models.json
  openrouter_model_info_update_policy: on_run_start
  openrouter_model_info_url: https://openrouter.ai/api/v1/models
  openrouter_model_info_fetch_timeout_ms: 5000

git:
  require_clean: true
  run_branch_prefix: attractor/run
  commit_per_node: true
```

Notes:

- Provider keys accept `openai`, `anthropic`, `google` (`gemini` alias maps to `google`), `kimi`, `zai`, `cerebras`, and `minimax`.
- If a graph node uses provider `P`, `llm.providers.P.backend` must be set (`api` or `cli`).
- `backend: cli` is currently supported for `openai`, `anthropic`, and `google` (including the `gemini` alias).
- In v1 behavior, runs require a clean repo and checkpoint each node.
- Prefer first-class run config policy knobs over env tuning:
  - `runtime_policy` for stage timeout, stall watchdog, and retry cap.
  - `preflight.prompt_probes` for prompt-probe mode/transports/policy.

## Provider Backends

CLI backend mappings:

- `openai` -> `codex exec --json --sandbox workspace-write -m <model> -C <worktree>`
- `anthropic` -> `claude -p --dangerously-skip-permissions --output-format stream-json --verbose --model <model> "<prompt>"`
- `google` -> `gemini -p --output-format stream-json --yolo --model <model> "<prompt>"`

CLI executable overrides:

- `KILROY_CODEX_PATH`
- `KILROY_CLAUDE_PATH`
- `KILROY_GEMINI_PATH`

API backend credentials:

- OpenAI: `OPENAI_API_KEY` (`OPENAI_BASE_URL` optional)
- Anthropic: `ANTHROPIC_API_KEY` (`ANTHROPIC_BASE_URL` optional)
- Google: `GEMINI_API_KEY` or `GOOGLE_API_KEY` (`GEMINI_BASE_URL` optional)
- Kimi: `KIMI_API_KEY`
- Z.ai: `ZAI_API_KEY`
- Cerebras: `CEREBRAS_API_KEY`
- MiniMax: `MINIMAX_API_KEY`

API protocol/base URL/path overrides are configured in `llm.providers.<provider>.api` in run config.

## Run Output and Exit Codes

`run` and `resume` print:

- `run_id`
- `logs_root`
- `worktree`
- `run_branch`
- `final_commit`

Exit codes:

- `0`: final status `success` (or validation success)
- `1`: command failure, validation failure, or non-success final status

## Artifacts

Run-level (`{logs_root}`) commonly includes:

- `graph.dot`
- `manifest.json`
- `checkpoint.json`
- `final.json`
- `run_config.json`
- `modeldb/openrouter_models.json`
- `run.tgz`
- `worktree/`

Stage-level (`{logs_root}/{node_id}`) commonly includes:

- `prompt.md`
- `response.md`
- `status.json`
- `stage.tgz`
- `stdout.log`, `stderr.log`
- `events.ndjson`, `events.json`
- `cli_invocation.json`, `cli_timing.json`
- `api_request.json`, `api_response.json`
- `output_schema.json`, `output.json`
- `tool_invocation.json`, `tool_timing.json`
- `diff.patch`

Exact files depend on handler/backend type.

Browser verification notes:
- Browser verify nodes emit `tool_browser_artifacts` events in `{logs_root}/progress.ndjson`.
- Collected browser files are stored in `{logs_root}/{node_id}/browser_artifacts/`; on retries, prior copies are preserved in `{logs_root}/{node_id}/attempt_N/browser_artifacts/`.

## Status Contract for Codergen Nodes

For `shape=box` nodes:

- `llm_provider` and `llm_model` must resolve.
- If backend returns no explicit outcome, Kilroy expects a `status.json` signal.
- `status.json` may be written in worktree root; Kilroy copies it into stage directory.
- If `auto_status=true`, missing `status.json` becomes success; otherwise stage fails.

Canonical `status.json` shape:

```json
{
  "status": "success",
  "preferred_label": "",
  "suggested_next_ids": [],
  "context_updates": {},
  "notes": "",
  "failure_reason": ""
}
```

Valid statuses: `success`, `partial_success`, `retry`, `fail`, `skipped`.

## Resume Behavior

- `--logs-root`: direct and most reliable.
- `--run-branch`: derives run id from branch suffix and scans default runs directory for manifest match.

On resume, Kilroy:

- Loads `manifest.json`, `checkpoint.json`, and `graph.dot`.
- Recreates run branch/worktree at checkpoint commit.
- Requires clean repo before continuing.
- Uses the run's snapshotted model catalog from `logs_root/modeldb/openrouter_models.json`.

## Run-Config Immutability Guard

Once a user asks you to run or launch a Kilroy pipeline, the following files are **frozen** — do NOT modify them without explicit user permission:

- The graph file (`.dot`)
- The run config file (`run.yaml` / `run.json`)
- Any model configuration (catalog files, model IDs in the graph)
- The preferences file (`preferences.yaml`)

If prelaunch validation or launch fails, **diagnose and present options** — never silently fix the inputs. See "Prelaunch Validation Failure Playbook" below.

This guard applies from the moment you begin building or executing a `kilroy run` command until the user explicitly asks for changes. It does NOT apply during graph authoring/editing phases before a run is requested.

## Launch Intent Priority

When the user clearly instructs you to start/launch/run Kilroy, begin the run immediately. Do not ask extra "are you sure?" confirmation questions that delay execution.

Rationale: users often issue launch commands right before stepping away, and waiting for an unnecessary confirmation can waste hours.

Execution rule:

- If the requested run config is a production profile (for example `llm.cli_profile: real`) and the user clearly asked to start the run, start the production run.
- Prefer detached launch for long-running jobs unless the user explicitly requests foreground execution.
- Only stop to ask questions when required launch inputs are genuinely missing or contradictory (for example no graph path and no run config path).

## Prelaunch Validation Failure Playbook

When prelaunch checks fail, follow this sequence:

1. **Read the validation report**: `cat <logs_root>/prelaunch_validation.json`
2. **Diagnose** each failure/warning and identify the root cause.
3. **Present options to the user** with your recommendation:

| Failure | Likely Cause | Options |
|---------|-------------|---------|
| Model not in catalog | Pinned catalog is stale; model is new | (a) Switch run.yaml to `on_run_start` to fetch live catalog (b) Manually update pinned catalog (c) User confirms model ID is wrong |
| CLI binary not found | Provider CLI not installed | (a) Install the CLI tool (b) Switch provider to `backend: api` (c) Use a different provider |
| API key missing | Env var not set | (a) Set the env var (b) Switch to CLI backend (c) Use a different provider |
| Prompt probe timeout | Provider is slow/down | (a) Increase `preflight.prompt_probes.timeout_ms` (b) Retry (c) Disable probes for this run |
| CLAUDECODE conflict | Running inside Claude Code session | (a) Engine strips it automatically (post-fix); rebuild if on old binary |
| Repo not clean | Uncommitted changes | (a) Commit changes (b) Stash changes (c) Set `git.require_clean: false` |

4. **Wait for user decision** before making any changes.
5. After user approves a fix, apply it and re-run.

**Never do any of the following without asking:**
- Downgrade a model ID (the model may be valid but absent from a stale catalog)
- Change the graph topology or node shapes
- Switch provider backends
- Modify prompt text

## CLAUDECODE Environment Variable

When Kilroy runs inside a Claude Code session, the `CLAUDECODE` env var is set. This causes the Claude CLI to refuse to launch (nested session protection). The engine strips `CLAUDECODE` from subprocess environments automatically (both preflight probes and codergen CLI invocations). If you encounter this error on an older binary, rebuild with `go build -o ./kilroy ./cmd/kilroy`.

## Frequent Failures

- `missing llm.providers.<provider>.backend`: add explicit backend in config.
- `missing llm_model on node`: set `llm_model` (or stylesheet model that resolves to it).
- `missing status.json (auto_status=false)`: write status file or set `auto_status=true`.
- `repo has uncommitted changes`: commit/stash before run or resume.
- `could not locate logs_root for run_branch`: use `--logs-root`.
- `resume: missing per-run model catalog snapshot`: ensure run logs are intact.

## Related Files

- Kilroy metaspec: `docs/strongdm/attractor/kilroy-metaspec.md`
- Attractor spec: `docs/strongdm/attractor/attractor-spec.md`
- Ingestor spec: `docs/strongdm/attractor/ingestor-spec.md`
- Test coverage map: `docs/strongdm/attractor/test-coverage-map.md`
- Create-dotfile skill: `skills/create-dotfile/SKILL.md`
