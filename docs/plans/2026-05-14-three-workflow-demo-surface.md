# Three Workflow Demo Surface Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Kilroy's default demo surface three workflows: `plan`, `implement`, and `validate`.

**Architecture:** Keep the existing workflow runner and discovery mechanics. Add thin packaged workflows for `plan` and `validate`, convert public `implement` to the relay loop, and mark the old near-duplicate stations experimental so they remain available through `--all`.

**Tech Stack:** Kilroy workflow packages (`workflow.toml`, DOT graphs, bash scripts), Go CLI discovery tests, markdown skill/docs.

---

### Task 1: Curated Surface Test

**Files:**
- Modify: `cmd/kilroy/workflows_test.go`

**Steps:**
1. Add a test that runs `kilroy list` with source-checkout discovery only.
2. Assert the default curated names are exactly `implement`, `plan`, and `validate`.
3. Run the test and verify it fails before workflow changes.

### Task 2: Add Workflow Packages

**Files:**
- Create: `workflows/plan/{workflow.toml,graph.dot}`
- Create: `workflows/validate/{workflow.toml,graph.dot,scripts/validate.sh}`

**Steps:**
1. Add `plan` as a quick planning/intake workflow that outputs `plan-status.json`, `task-packet.md`, `testing-plan.md`, and `validation-plan.md`.
2. Add `validate` as a script workflow that runs optional validation/build/test commands and writes `evidence.md` and `evidence.json`.
3. Run shipped package and graph tests.

### Task 3: Public Implement Is The Relay

**Files:**
- Move old `workflows/implement` to `workflows/implement-oneshot`
- Replace `workflows/implement` with relay graph/scripts
- Modify: `workflows/implement/workflow.toml`
- Modify: `workflows/implement/graph.dot`
- Modify: `workflows/implement/scripts/final-report.sh`

**Steps:**
1. Preserve one-shot implementation as experimental `implement-oneshot`.
2. Make public `implement` consume `task_packet`, optional `testing_plan`, optional `validation_plan`, and optional `verify_command`.
3. Generate `implementation.patch` in final report.
4. Fail completion when the relay says `COMPLETE` but produces an empty patch.

### Task 4: Hide Old Choices

**Files:**
- Modify workflow manifests for `fix`, `investigate`, `review`, `build-test`, `coding-loop`, `coding-relay`, `implement-codex`, `implement-oneshot`

**Steps:**
1. Set `[workflow].experimental = true` on old/internal station workflows.
2. Keep them installed and runnable by name, but hidden from default `kilroy list`.
3. Update install tests to expect the new packages.

### Task 5: Update Agent Guidance

**Files:**
- Modify: `skills/using-kilroy/SKILL.md`
- Modify: `docs/usage.md`

**Steps:**
1. Replace the many-choice workflow table with `plan`, `implement`, `validate`.
2. Require a shared `session=<id>` label on related runs.
3. Direct temporary task files to `~/.local/state/kilroy/sessions/<session-id>/`.
4. State that if Kilroy is requested for a phase, sidecar local research/implementation should be avoided; launch another Kilroy run instead.

### Task 6: Verify

**Commands:**
- `go test ./cmd/kilroy -run 'TestWorkflowsList_SourceBuiltInCuratedSurface|TestUsingKilroySkillFrontmatterAndRunCommandsStayAgentSafe' -count=1`
- `go test ./internal/attractor/validate -run 'TestShippedWorkflow' -count=1`
- `bash scripts/test/install_test.sh`
- `go test -timeout=300s ./...`
- Real smoke: plan exits with `NEEDS_CLARIFICATION`; plan with no-ask produces task packet; validate runs in a tiny repo.
