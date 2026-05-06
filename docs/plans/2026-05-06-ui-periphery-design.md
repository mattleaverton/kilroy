# Kilroy UI Periphery Design

## Context

Kilroy currently has a `kilroy serve` command backed by `internal/server`.
That package mixes three responsibilities:

- serving an embedded browser UI from `internal/server/ui`
- exposing read APIs over the run database and run artifacts
- launching, canceling, and answering runs through an in-process registry

The current product direction is different. Kilroy itself should stay a CLI and
worker runtime. The browser UI should live in this repository for now, but on
the periphery, not inside the `kilroy` command surface. The UI should inspect
the one canonical Kilroy run database at `rundb.DefaultPath()`.

The legacy UI has useful prior art: a dense run list, DOT graph rendering,
node execution sidebar, run log view, node artifacts, git diffs, output
previews, file browsing, live polling, and provider selection visibility.
Those ideas should be retained, but the architecture should stop implying that
the UI owns execution.

## Goals

- Add a top-level `ui/` directory containing both the standalone local server
  and browser assets.
- Remove `kilroy serve` from the public `kilroy` CLI surface.
- Keep the server fixed to Kilroy's canonical run database; do not add a user
  `--db` selector.
- Make the initial app browse/inspect focused.
- Surface the information Kilroy records: runs, labels, inputs, invocation,
  effective config, parent/child runs, node attempts, edge decisions, provider
  selections, node artifacts, node diffs, run logs, outputs, DOT source, and
  filesystem-backed run/worktree files when present.
- Add policy and auth views that help explain current routing health and
  run-time provider choices without storing or exposing secrets.
- Verify the UI in a real browser with browser-use.

## Architecture

The new top-level shape is:

```text
ui/
  server/    standalone local Go HTTP server
  web/       browser UI assets
```

`ui/server` is a standalone `package main` runnable with:

```bash
go run ./ui/server
```

It listens on `127.0.0.1:8080` by default and accepts only listen-address flags.
It opens `rundb.DefaultPath()` internally. Tests can redirect the default path
with `XDG_STATE_HOME`, but the operator-facing app should not expose a database
path option.

The server keeps read routes under `/api/...` and serves the app at `/`.
The app should not depend on an in-memory run registry. Active-run updates come
from polling the database and tailing `run.log` where available.

## API Surface

Initial read surface:

- `GET /api/health`
- `GET /api/runs`
- `GET /api/runs/{id}`
- `GET /api/runs/{id}/outputs`
- `GET /api/runs/{id}/outputs/{name...}`
- `GET /api/runs/{id}/nodes/{nodeId}/attempts`
- `GET /api/runs/{id}/nodes/{nodeId}/turns`
- `GET /api/runs/{id}/nodes/{nodeId}/diff`
- `GET /api/runs/{id}/log`
- `GET /api/runs/{id}/files/{path...}`
- `GET /api/runs/{id}/workspace/{path...}`
- `GET /api/policy`
- `GET /api/policy/{class}`
- `GET /api/policy/explain/{runId}`
- `GET /api/auth`

Action routes are intentionally not part of the initial primary flow. If we
add them later, they should be visually secondary, narrowly scoped, and use
explicit confirmation. Good candidates are copying paths, opening artifacts,
or stopping a clearly running run via the existing CLI semantics.

## Data Flow

The server reads from `rundb.DefaultPath()`, enriches run rows with related
tables, and falls back to log-root files only where the database intentionally
stores references rather than full content. Node artifacts should come from
`node_execution_artifacts` first, so loop attempts and reused stage directories
remain inspectable after the filesystem changes.

Policy and auth views should reuse Kilroy's existing internal packages. Policy
views should show the effective class chains, applied overrides, candidate auth
reachability, and run-specific provider selections or prelaunch resolution
artifacts when available. Auth views should show provider/tool/source health,
but must not expose key values or token contents.

## UI Model

The first screen is the operational dashboard, not a landing page. It should be
dense and scan-friendly:

- left/top controls for search and filters
- run table/list with status, workflow, repo, labels, duration, child count,
  provider summary, and recent failure text
- detail view with graph, timeline, node list, metadata, and right-side panels
- panels for info, attempts, artifacts/transcript, diff, log, files, outputs,
  graph source, policy, and auth/routing

The old UI's DOT visualization and node detail panel are the primary starting
point. Launch-specific UI is removed from the primary screen.

## Verification

Verification must include:

- Go tests for the removed `kilroy serve` surface.
- Go tests for the standalone UI server using a temp `XDG_STATE_HOME` run DB.
- Browser verification with browser-use against a locally running server.
- `gofmt -l`, `go vet ./...`, `go build ./cmd/kilroy/`, `go build ./ui/server`,
  and `go test -timeout=300s ./...`.

