# Alpha Foundation Local Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Kilroy usable from external repos by closing the alpha truth gaps in dispatch, configuration, workflow discovery, and run-output hygiene.

**Architecture:** Keep the engine's mature stage shell for prompt construction, context preambles, status ingestion, CXDB writes, and artifact handling. Replace the agent execution under that shell with a RunTurn-backed adapter that chooses concrete `AgentBackend` implementations by the frozen `AgentRoute`. Layered config and shipped workflow discovery become part of the launch path, not documentation-only features.

**Tech Stack:** Go, existing `internal/attractor/engine`, `internal/attractor/agents`, `internal/config`, `internal/attractor/workflows`, shipped workflow packages under `workflows/`.

---

## Task 1: Dispatch Consolidation

**Files:**
- Modify: `internal/attractor/agents/dispatcher.go`
- Modify: `internal/attractor/agents/backend.go`
- Modify: `internal/attractor/agents/backend_test.go`
- Modify/Add: `internal/attractor/agents/dispatcher_execute_test.go`

**Steps:**
1. Write tests proving `Dispatcher.ExecuteAgent` drives agent execution through `loop.RunTurn`.
2. Write tests proving `SDKBackend.StartTurn` and `TmuxBackend.StartTurn` do not call `ExecuteAgent`.
3. Implement the RunTurn bridge while preserving the engine stage shell.
4. Remove `NativeExecuteAgent`.
5. Verify `rg "NativeExecuteAgent|b\\.handler\\.ExecuteAgent" internal/attractor/agents` returns no live code hits.

## Task 2: Layered Config

**Files:**
- Create/Modify: `internal/config/layered.go`
- Modify: `internal/config/config.go`
- Modify: `cmd/kilroy/main.go`
- Modify: `cmd/kilroy/run.go`
- Modify: `cmd/kilroy/workflows.go`
- Modify: `internal/attractor/engine/resume.go`

**Steps:**
1. Write config tests for defaults, user/project/env/CLI precedence, missing files, and invalid `KILROY_PROJECT_ROOT`.
2. Add built-in defaults.
3. Wire `LoadLayered` through CLI/runtime config call sites while leaving auth config separate.
4. Preserve the auth config resolver as a separate stack.

## Task 3: Shipped Workflow Discovery

**Files:**
- Modify/Add: `internal/attractor/workflows/*.go`
- Modify: `cmd/kilroy/workflows_test.go`

**Steps:**
1. Write tests showing `kilroy workflows list` and `kilroy run implement` can find shipped workflows without `KILROY_WORKFLOW_PATHS` or source-tree cwd.
2. Keep override precedence unchanged: env paths, project workflows, user workflows, installed data-dir workflows, source-checkout fallback for development binaries.
3. Improve not-found errors to include searched paths and recovery advice.

## Task 4: Run Output Hygiene

**Files:**
- Modify: engine stage commit / checkpoint path under `internal/attractor/engine`
- Modify: output contract handling under `internal/attractor/engine`

**Steps:**
1. Write a regression test proving generated run artifacts are not present under `cmd/kilroy`.
2. Remove tracked E2E output artifacts from `cmd/kilroy`.
3. Exclude graph-declared outputs from checkpoint commits so artifacts such as `result.md`, `fix.patch`, and `review.json` are collected under `logs_root/outputs/` without becoming code changes.
4. Reject unsafe declared output paths so collection cannot escape the worktree or outputs directory.

## Final Verification

Run:

```bash
gofmt -l . | grep -v '^\./\.claude/' | grep -v '^\.claude/'
go vet ./...
go build ./cmd/kilroy/
go test -timeout=300s ./...
```

Then run one external-repo smoke with shipped workflow discovery and one real Kilroy workflow validation.
