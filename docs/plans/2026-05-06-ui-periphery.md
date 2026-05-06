# Kilroy UI Periphery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move Kilroy's browser UI to a top-level periphery app that reads the canonical run database directly and is no longer served by `kilroy`.

**Architecture:** `ui/server` becomes a standalone local Go server and `ui/web` holds browser assets. The server reads `rundb.DefaultPath()`, exposes read-oriented `/api` endpoints, and serves the dense dashboard at `/`. `cmd/kilroy` removes the `serve` command.

**Tech Stack:** Go standard library HTTP server, existing Kilroy `rundb`, `policy`, and `auth` packages, vanilla HTML/CSS/JavaScript, embedded static assets, browser-use MCP for verification.

---

### Task 1: Commit Design And DoD

**Files:**
- Create: `docs/plans/2026-05-06-ui-periphery-design.md`
- Create: `docs/plans/2026-05-06-ui-periphery-dod.md`
- Create: `docs/plans/2026-05-06-ui-periphery.md`

**Step 1: Review the documents**

Run:

```bash
git diff -- docs/plans/2026-05-06-ui-periphery-design.md docs/plans/2026-05-06-ui-periphery-dod.md docs/plans/2026-05-06-ui-periphery.md
```

Expected: the docs match the approved scope: top-level UI, fixed run DB, inspect-first UI.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-06-ui-periphery-design.md docs/plans/2026-05-06-ui-periphery-dod.md docs/plans/2026-05-06-ui-periphery.md
git commit -m "docs(ui): plan periphery dashboard split"
```

### Task 2: Write Failing Contract Tests

**Files:**
- Create: `cmd/kilroy/serve_removed_test.go`
- Create: `ui/server/server_test.go`

**Step 1: Add CLI serve removal test**

Test that `usage()` no longer mentions `kilroy serve`. Also build the binary
and run `kilroy serve`, expecting a nonzero exit instead of a long-running
server.

**Step 2: Add UI server contract tests**

Create temp `XDG_STATE_HOME`, seed `rundb.DefaultPath()` with:

- one run with labels, inputs, invocation, config, DOT source, and parent/child linkage
- one node execution
- one edge decision
- one provider selection
- one node diff
- one node artifact

Assert:

- `GET /api/health` returns OK
- `GET /api/runs` returns the seeded run
- `GET /api/runs/{id}` returns run detail with nodes, edges, providers, diffs, labels, inputs, invocation, config, and children
- `GET /api/runs/{id}/nodes/{node}/turns?attempt=1` returns the artifact content
- `POST /api/runs` is unavailable
- the server type has no user-facing database path config

**Step 3: Verify red**

Run:

```bash
go test ./cmd/kilroy -run 'TestUsageDoesNotAdvertiseServe|TestServeCommandIsRemoved' -count=1
go test ./ui/server -run 'Test' -count=1
```

Expected: tests fail because the CLI still has `serve` and `ui/server` does not exist yet.

### Task 3: Move UI Server To Top Level

**Files:**
- Create/modify: `ui/server/*.go`
- Create/modify: `ui/web/index.html`
- Create/modify: `ui/web/viz.js`
- Create/modify: `ui/web/viz-render.js`
- Delete: `internal/server/*`
- Delete: `cmd/kilroy/attractor_serve.go`
- Modify: `cmd/kilroy/main.go`

**Step 1: Create `ui/server` and `ui/web`**

Move the old embedded UI assets to `ui/web`. Create a standalone `ui/server`
command with:

- `--addr` only
- default `127.0.0.1:8080`
- no `--db`
- `rundb.DefaultPath()` internally
- read-only `/api` routes
- static app at `/`

**Step 2: Port read handlers**

Port the useful read handlers from `internal/server`:

- run list/detail
- node attempts and artifacts
- node diffs
- run log
- outputs
- logs-root/workspace file browsing

Remove pipeline registry, submit, cancel, interviewer, and workflow launch logic
from the first pass.

**Step 3: Remove `kilroy serve`**

Delete `cmd/kilroy/attractor_serve.go`, remove the `serve` switch case, and
remove the serve line from help text.

**Step 4: Verify green**

Run:

```bash
go test ./cmd/kilroy -run 'TestUsageDoesNotAdvertiseServe|TestServeCommandIsRemoved' -count=1
go test ./ui/server -run 'Test' -count=1
```

Expected: tests pass.

### Task 4: Expand Data Coverage

**Files:**
- Modify: `ui/server/*.go`
- Modify: `ui/web/index.html`
- Test: `ui/server/server_test.go`

**Step 1: Add failing tests for coverage gaps**

Based on the investigation output, add focused tests for missing DB-backed
surfaces: parent/child runs, policy explain data, auth list summary, config,
warnings, labels, outputs, artifacts by attempt, and missing file states.

**Step 2: Implement APIs**

Add or adjust endpoints so every known RunDB table and useful file artifact has
a human-readable UI path.

**Step 3: Update UI**

Adapt the legacy SPA to `/api` endpoints, remove launch UI, add graph source,
policy/auth panels, and expose provider/routing summaries in run detail.

**Step 4: Verify targeted tests**

Run:

```bash
go test ./ui/server -count=1
```

Expected: pass.

### Task 5: Browser Verification

**Files:**
- Modify as needed: `ui/web/index.html`, `ui/server/*.go`

**Step 1: Start the app**

Run:

```bash
go run ./ui/server --addr 127.0.0.1:18080
```

Keep the server running for browser verification.

**Step 2: Use browser-use**

Open `http://127.0.0.1:18080/`. Verify:

- dashboard loads
- run list is readable
- run detail opens
- graph area renders or shows an explicit no-graph state
- node panels work
- policy/auth views load
- missing artifacts show clear messages
- layout has no incoherent overlaps at desktop size

**Step 3: Capture issues**

Fix any functional or layout failures found in browser-use and repeat the
targeted tests.

### Task 6: Final Verification

**Files:**
- All changed files

**Step 1: Format**

Run:

```bash
gofmt -w cmd/kilroy ui/server
gofmt -l . | grep -v '^\./\.claude/' | grep -v '^\.claude/'
```

Expected: second command prints nothing.

**Step 2: Full checks**

Run:

```bash
go vet ./...
go build ./cmd/kilroy/
go build ./ui/server
go test -timeout=300s ./...
```

Expected: all exit 0.

**Step 3: Commit**

Run:

```bash
git status --short
git add cmd/kilroy ui docs/plans/2026-05-06-ui-periphery*.md
git commit -m "ui: move dashboard to periphery app"
```

