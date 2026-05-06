# Kilroy UI Periphery — Definition of Done

## Scope

### In Scope

The Kilroy UI exists in this repository under top-level `ui/`, runs separately
from the `kilroy` CLI, reads Kilroy's canonical run database directly, and lets
a human inspect past and present runs with dense operational detail.

### Out of Scope

The initial deliverable does not launch runs from the browser, manage secrets,
choose an alternate run database, or deploy a hosted service.

### Assumptions

The local machine has Go available. Browser verification uses the local
browser-use MCP. Existing run logs and worktrees may have been pruned; the UI
must degrade clearly when referenced files are unavailable.

## Deliverables

| Artifact | Location | Description |
|----------|----------|-------------|
| Standalone UI server | `ui/server/` | Local Go server for DB-backed UI APIs and static assets |
| Browser UI | `ui/web/` | Dense run inspection UI |
| CLI cleanup | `cmd/kilroy/` | `kilroy serve` removed from command switch and usage |
| Legacy cleanup | `internal/server/` | Removed or emptied of the old served-by-Kilroy implementation |
| Tests | `cmd/kilroy`, `ui/server` | Regression and API coverage |

## Acceptance Criteria

### Placement And CLI

| ID | Criterion | Covered by |
|----|-----------|------------|
| AC-1.1 | `ui/server` and `ui/web` exist at the repository root. | IT-1, IT-2 |
| AC-1.2 | `go run ./ui/server` starts the UI server. | IT-2 |
| AC-1.3 | `kilroy --help` does not advertise `kilroy serve`. | IT-1 |
| AC-1.4 | `kilroy serve` exits nonzero instead of starting a server. | IT-1 |

### Data And API

| ID | Criterion | Covered by |
|----|-----------|------------|
| AC-2.1 | The UI server reads `rundb.DefaultPath()` without a user database flag. | IT-1 |
| AC-2.2 | `GET /api/runs` returns runs from the run database. | IT-1, IT-2 |
| AC-2.3 | `GET /api/runs/{id}` returns run metadata, nodes, edges, providers, diffs, labels, inputs, invocation, config, parent, and children when present. | IT-1, IT-2 |
| AC-2.4 | Node artifacts are available through DB-backed APIs for specific attempts. | IT-1, IT-2 |
| AC-2.5 | Log, output, logs-root file, and workspace file views work when referenced files exist and show a clear unavailable state when they do not. | IT-1, IT-2 |

### Human Inspection

| ID | Criterion | Covered by |
|----|-----------|------------|
| AC-3.1 | The first screen is a dense run dashboard, not a marketing page. | IT-2 |
| AC-3.2 | A user can open a run and inspect graph, timeline/node attempts, provider choices, artifacts, diffs, logs, outputs, DOT source, labels, inputs, config, and parent/child runs. | IT-2 |
| AC-3.3 | Policy and auth views show routing health and current candidates without displaying secret values. | IT-2 |
| AC-3.4 | The UI remains usable for empty databases and missing filesystem artifacts. | IT-2 |

### Verification

| ID | Criterion | Covered by |
|----|-----------|------------|
| AC-4.1 | `gofmt -l` reports no formatted Go files. | IT-3 |
| AC-4.2 | `go vet ./...` exits 0. | IT-3 |
| AC-4.3 | `go build ./cmd/kilroy/` exits 0. | IT-3 |
| AC-4.4 | `go build ./ui/server` exits 0. | IT-3 |
| AC-4.5 | `go test -timeout=300s ./...` exits 0. | IT-3 |

## User-Facing Message Inventory

| ID | Message surface | Trigger condition | Covered by |
|----|----------------|-------------------|------------|
| MSG-1 | Empty run database message | No runs exist | IT-2 |
| MSG-2 | Run load failure message | Unknown run ID | IT-2 |
| MSG-3 | Missing log/output/file message | Referenced artifact is unavailable | IT-2 |
| MSG-4 | Auth/policy health state | Auth or policy view is opened | IT-2 |
| MSG-5 | Server health state | UI loads and polls health | IT-2 |

## Test Evidence Contract

| Item | Requirement |
|------|-------------|
| Evidence root | `.ai/runs/$KILROY_RUN_ID/test-evidence/latest/` |
| Scenario folder pattern | `.ai/runs/$KILROY_RUN_ID/test-evidence/latest/IT-<id>/` |
| Manifest | `.ai/runs/$KILROY_RUN_ID/test-evidence/latest/manifest.json` |
| UI scenarios | Include screenshot evidence proving key states |
| Non-UI scenarios | Include text or structured evidence |
| Failure behavior | Emit best-effort artifacts and record missing artifacts explicitly |

## Integration Test Scenarios

| ID | Scenario | Steps | Verification | Evidence Artifacts |
|----|----------|-------|--------------|--------------------|
| IT-1 | Server and CLI contract | Create a temp `XDG_STATE_HOME`; seed a run DB with run, node, edge, provider, diff, and artifact records; assert `kilroy --help` omits serve; assert `kilroy serve` exits nonzero; hit UI server APIs with `httptest`. | `go test ./cmd/kilroy ./ui/server` exits 0 | `surface=non_ui`; test log |
| IT-2 | Browser run inspection | Start `go run ./ui/server`; open it with browser-use; inspect dashboard, run detail, graph/node panels, artifacts/logs/diff/output/DOT/policy/auth views; capture screenshots. | Browser-use confirms expected UI states and screenshots exist | `surface=ui`; dashboard and detail screenshots |
| IT-3 | Full repo verification | Run formatting, vet, build, and test commands from the worktree. | All commands exit 0 | `surface=non_ui`; command logs |

