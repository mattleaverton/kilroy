# Kilroy

Local-first CLI that runs AI coding workflows in a git repo. Each workflow is a self-contained package (`workflow.toml` + DOT graph + prompts) that resolves provider, model, backend, and credentials automatically from a *class* (e.g. `hard_coding`, `deep_investigation`), then freezes the choice in a prelaunch snapshot before any LLM call.

**Status: alpha.** Public surface is `kilroy run <workflow>`. Auth setup requires shell env vars; a `kilroy auth set` write surface is on the post-alpha roadmap.

## Install

```bash
brew install danshapiro/kilroy/kilroy        # macOS / Linux
# or
go install github.com/danshapiro/kilroy/cmd/kilroy@latest
# or
go build -o ./kilroy ./cmd/kilroy/
```

## Quick start

```bash
kilroy auth init            # generate ~/.config/kilroy/auth.toml from detected env vars
kilroy auth check           # verify every chain has a usable source
kilroy workflows list       # show shipped workflow packages
kilroy workflows validate implement   # confirm this would launch on this machine

kilroy run implement --input-file prompt=spec.md --label scope=my-task
kilroy runs list --pretty
kilroy runs show <run-id>
```

Run output, artifacts, and isolated execution worktree all land under `~/.local/state/kilroy/attractor/runs/<run-id>/`.

`kilroy run` is **async by default** — it returns immediately with a run handle. Pass `--sync` to block until the run terminates.

## Concepts

**Workflow package.** A directory under `workflows/<name>/` (or `~/.config/kilroy/workflows/<name>/`) with `workflow.toml`, `graph.dot`, and optional prompts/scripts. The CLI discovers them via `KILROY_WORKFLOW_PATHS`, project-root `.kilroy/workflows/`, and XDG config dir, in that order.

**Agent class.** Each stage declares a class (`hard_coding`, `deep_investigation`, `architectural_critique`, `quick_easy`, etc.) instead of a model. The class resolver maps the class to a `(provider, model, driver)` tuple via the policy chain at prelaunch — and the choice is **frozen**. Execution does not re-resolve.

**Auth chain.** Each `(provider, method)` binding is satisfied by an ordered chain of credential sources (env var → CLI session → keychain). Convention: per-tool budgets use `<PROVIDER>_API_KEY_KILROY` — when set, that key beats the canonical key without unsetting it.

**Validation = launch parity.** `kilroy workflows validate <name>` runs the same prelaunch checks `kilroy run` does (graph integrity, class resolution, auth resolution, CLI binary probes, credential probes). If validate passes, launch will not silently fail on these axes.

## Commands

```text
kilroy run <workflow>           [--input-file KEY=PATH ...] [--label K=V ...] [--sync] [--pretty]
kilroy workflows                list | describe <name> | validate <name>
kilroy runs                     list | show <id> | wait <id> | prune
kilroy status                   [--logs-root <dir> | --latest] [--watch]
kilroy resume                   --logs-root <dir>
kilroy stop                     --logs-root <dir> [--grace-ms <ms>] [--force]
kilroy auth                     defaults | init | list | check | suggest-fix
kilroy policy                   list | show <class> | resolve <class> | explain <run-id>
kilroy ingest                   [--output <file.dot>] <requirements>
```

Exit codes: `0` = success or validation pass; `1` = failure or non-success terminal status.

## Run artifacts

Per-run under `<logs_root>`: `graph.dot`, `prelaunch_validation.json`, `manifest.json`, `final.json`, `run_config.json`, `run.tgz`, isolated `worktree/`.

Per-stage under `<logs_root>/<node_id>/`: `prompt.md`, `response.md`, `status.json`, `resolution.json`, plus `events.ndjson` (API path) or `agent_output.jsonl` + `cli_invocation.json` (CLI path).

## Alpha caveats

- **Auth setup is read-only today.** `auth init` generates a config from detected env vars; updates require editing `~/.config/kilroy/auth.toml` or shell exports. A `kilroy auth set/login` write surface is planned post-alpha.
- **Some legacy direct-mode flags** (`--graph`, `--package`, `--config`, `--run-id`, `--logs-root`) are kept for ad-hoc work and tests. The recommended public surface is `kilroy run <workflow>`.
- **Stale-build detection** for dev builds: `kilroy run` refuses to launch a binary older than the source tree. Rebuild or pass `--confirm-stale-build`.

## License

MIT. See `LICENSE`.
