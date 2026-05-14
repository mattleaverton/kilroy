# Workflow Catalog

**Generated from workflows/*/workflow.toml on 2026-05-05**

This document catalogs all 8 built-in workflows shipped under `workflows/`. Each workflow is defined by a `workflow.toml` file that specifies its inputs, outputs, side effects, and execution graph.

## Overview

Kilroy workflows are reusable, packaged pipelines for common software development tasks. Each workflow includes:

- A **graph** (`.dot` file) defining the execution flow
- Declarative **inputs** and **outputs**
- **Side effect** declarations for safety and idempotency tracking
- Optional **policy class** routing for agent nodes

## Workflows

### build-test

**Status:** Experimental (not shown in default `kilroy list`)

**Description:**  
Detect the project's build system, build, run tests, and write a
structured build-report.json. Pure layer 0: no agent, no LLM cost. The
detect step recognizes Go (go.mod), Rust (Cargo.toml), Node (package.json),
and Python (pyproject.toml/setup.py); each subsequent step uses commands
appropriate for the detected system.

**Agent Description:** Detect, build, test, and report — no LLM involvement.

**Default Class:** *(none — layer 0, no agents)*

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `build_command` | string | optional | — | Override the auto-detected build command. |
| `test_command` | string | optional | — | Override the auto-detected test command. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `build-report.json` | path | `build-report.json` | Structured report with detected build system, build/test status, and command stdout/stderr tails. |

**Side Effects:**

- `mutates_git`: false
- `writes_files`: true
- `network_egress`: false
- `idempotent`: true

**When to use:** As a verify step in CI or as a sanity gate before launching agentic work. Useful when you need deterministic build/test verification without LLM cost.

**When NOT to use:** When you need code changes or agentic reasoning. This is a read-only verification workflow.

---

### coding-loop

**Status:** Experimental (not shown in default `kilroy list`)

**Description:**  
Iterative coding workflow: a task chooser selects sub-tasks, an
implementer codes them, a reviewer scores each iteration, and a
done-gate decides when the spec is complete. Loop bound: ~12 iterations
or until the done-gate writes COMPLETE to `.kilroy/decision.md`, whichever
comes first.

**Agent Description:** Iterate on a spec — choose, implement, review, gate — until complete or capped.

**Default Class:** *(stylesheet-based routing via `.implementer`/`.reviewer` selectors)*

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `spec` | string | **required** | — | Absolute path to the feature/task spec file that defines what to build. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Final report summarizing iterations, commits, and completion status. |

**Side Effects:**

- `mutates_git`: true
- `writes_files`: true
- `network_egress`: true
- `idempotent`: false

**When to use:** For complex, multi-step features where iterative refinement with review feedback is valuable. Good for exploratory work where the exact implementation path isn't clear upfront.

**When NOT to use:** For simple, directed changes where `implement` would be faster and more cost-effective. The loop overhead is unnecessary for well-scoped, single-step tasks.

---

### coding-relay

**Status:** Experimental (not shown in default `kilroy list`)

**Description:**  
Iterative coding loop with four roles handed off in sequence:
- planner (anthropic SDK, sonnet 4.6) reads spec + prior feedback, scopes one sub-task.
- coder (opencode + kimi-k2.5) implements the task, attempts a build.
- critic (codex CLI + gpt-5.4-mini) judges work against spec, writes feedback + decision.
- status (quick_easy class) writes STATUS.md for external monitoring.

Loop bound: 20 iterations or until critic writes COMPLETE to `.kilroy/decision.md`.
Exercises agents.Dispatcher driver routing (SDK vs CLI), the auth binder
across three providers, and the class resolver via the status node.

**Agent Description:** Background coding loop with planner/coder/critic/status. Monitor STATUS.md while it runs; read result.md when done.

**Default Class:** *(per-node routing via agent_tool and SDK/CLI mix)*

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `prompt` | string | **required** | — | Absolute path to the spec / requirements / instruction file the coder should work from. |
| `context_files` | string | optional | — | Newline-separated absolute paths the agents should read for additional context. |
| `verify_command` | string | optional | `go build ./... 2>&1` | Shell command run after each coder iteration to capture build output. Default builds Go; override per-project. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Final summary written when the loop terminates (success or failure). |
| `status` | path | `STATUS.md` | Live status overview, refreshed every iteration. Safe for external readers to poll. |

**Side Effects:**

- `mutates_git`: true
- `writes_files`: true
- `network_egress`: true
- `idempotent`: false

**When to use:** For fire-and-forget background work where you want multi-provider diversity and external monitoring via STATUS.md. Good for exercising cross-provider routing.

**When NOT to use:** For quick, synchronous tasks where the overhead of multi-provider coordination isn't justified.

---

### fix

**Description:**  
Fix a described bug. The agent stages context, makes the smallest change
that addresses the root cause, the verify stage runs your build+test
command, and a fix.patch is captured against the launch HEAD. Failed
verify retries the agent once.

**Agent Description:** Fix a described bug; verifies via build+test and emits a fix.patch.

**Default Class:** `hard_coding`

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `issue` | string | **required** | — | Description of the bug to fix. Be specific: symptom, expected behavior, and a reproduction if you have one. |
| `context_files` | string | optional | — | Newline-separated absolute paths the agent should read for context. |
| `verify_command` | string | optional | `go build ./... && go test ./... -timeout 60s` | Shell command run by the verify stage. Default builds and runs targeted tests. |
| `scope_directive` | string | optional | — | Hard scope guardrail rendered into the agent's prompt. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Structured fix summary. |
| `fix.patch` | path | `fix.patch` | Git diff capturing the substantive change against the launch HEAD. |

**Side Effects:**

- `mutates_git`: false (change is staged in worktree only)
- `writes_files`: true
- `network_egress`: true
- `idempotent`: false

**When to use:** When you have a specific bug description and want a minimal fix with verification. The fix.patch output makes it easy to review before applying.

**When NOT to use:** For feature work (use `implement`) or open-ended investigation (use `investigate`).

---

### implement

**Description:**  
Implement a directed change with build+test verification. The agent stages
the context, makes the change, the verify stage runs your build+test
command, and a summary is written. Failed verify retries the agent once.

**Agent Description:** Implement a directed change in a kilroy worktree; verifies via build+test.

**Default Class:** `hard_coding`

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `prompt` | string | **required** | — | What to implement. Tightly scoped and specific. |
| `context_files` | string | optional | — | Newline-separated absolute paths the agent should read for context. |
| `verify_command` | string | optional | `go build ./... && go test ./... -timeout 60s` | Shell command run by the verify stage. Default builds and runs targeted tests. |
| `scope_directive` | string | optional | — | Hard scope guardrail rendered into the agent's prompt. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Structured run summary the agent (or summary.sh) writes. |

**Side Effects:**

- `mutates_git`: true
- `writes_files`: true
- `network_egress`: true
- `idempotent`: false

**When to use:** For directed feature work with clear requirements. The dogfood workhorse for kilroy's own development.

**When NOT to use:** For bug fixes (use `fix`) or when you need the change staged without git mutation (use `fix` workflow instead).

---

### investigate

**Description:**  
Research a focused question. The agent reads context files and URLs
(when provided), distinguishes evidence from inference, and produces
result.md with TL;DR, findings, open questions, and methodology.

**Agent Description:** Research a question and produce structured findings in result.md.

**Default Class:** `deep_investigation`

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `question` | string | **required** | — | What you're trying to find out. Be specific: what would a complete answer look like? |
| `context_files` | string | optional | — | Newline-separated paths the agent should read for context. |
| `urls` | string | optional | — | Newline-separated URLs the agent should fetch and analyze. |
| `scope_directive` | string | optional | — | Hard scope guardrail rendered into the agent's prompt. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Structured research output (TL;DR, findings, open questions, methodology). |

**Side Effects:**

- `mutates_git`: false
- `writes_files`: true
- `network_egress`: true
- `idempotent`: true

**When to use:** For research tasks where you need structured findings without code changes. Idempotent and safe to re-run.

**When NOT to use:** When you need code changes or transformations (use `implement` or `fix`).

---

### review

**Description:**  
Review a change. The stage_context step gathers the diff to review
(from a .patch file, a branch ref, or — TODO — a PR URL), the agent
reads it alongside an optional checklist, and produces a
human-readable review (result.md) plus a structured findings list
(review.json). Idempotent and read-only — no code is changed and
nothing is posted upstream.

**Agent Description:** Review a change and emit result.md plus review.json findings.

**Default Class:** `hard_coding`

**Inputs:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `target` | string | **required** | — | What to review. One of: a path to a .patch file, a branch ref (e.g. attractor/run/X), or a PR URL. PR URL handling is a TODO for v0 — pass a .patch or branch ref for now. |
| `checklist` | path | optional | — | Optional path to a review checklist file the agent should apply (e.g. project conventions, security checklist). |
| `context_files` | string | optional | — | Newline-separated paths the agent should read for context (design docs, related modules). |
| `scope_directive` | string | optional | — | Hard scope guardrail rendered into the agent's prompt. |

**Outputs:**

| Name | Type | Path | Description |
|------|------|------|-------------|
| `result` | path | `result.md` | Human-readable review: Summary, Findings (with severity), Verdict, Open questions. |
| `review.json` | path | `review.json` | Structured list of findings: severity, file, line, message. |

**Side Effects:**

- `mutates_git`: false
- `writes_files`: true
- `network_egress`: false
- `idempotent`: true

**When to use:** For code review of patches, branches, or diffs. Safe and idempotent — produces structured findings without any code changes or external posting.

**When NOT to use:** When you need to post reviews upstream (PR posting is TODO for v0) or when you need code changes (use `fix` instead).

---

## Command Transcript

The following shows sample output from the `kilroy` workflow CLI:

### `kilroy list --pretty`

```
NAME             CLASS                DESCRIPTION
---------------  -------------------  ------------------------------
fix              hard_coding          Fix a described bug. The agent stages context, makes the smallest change
implement        hard_coding          Implement a directed change with build+test verification. The agent stages
implement-codex  coding_codex_apikey  Implement a directed change with build+test verification. The agent stages
investigate      deep_investigation   Research a focused question. The agent reads context files and URLs
review           hard_coding          Review a change. The stage_context step gathers the diff to review
```

*Note: Experimental workflows (`build-test`, `coding-loop`, `coding-relay`) are excluded from the default curated list.*

### `kilroy describe implement --pretty`

```
name:        implement
version:     1
schema:      v2
source:      /Users/matt/.local/share/kilroy/workflows
dir:         /Users/matt/.local/share/kilroy/workflows/implement
default class: hard_coding

agent description:
  Implement a directed change in a kilroy worktree; verifies via build+test.

description:
  Implement a directed change with build+test verification. The agent stages
  the context, makes the change, the verify stage runs your build+test
  command, and a summary is written. Failed verify retries the agent once.

inputs:
  context_files (optional) type=string
    Newline-separated absolute paths the agent should read for context.
  prompt (REQUIRED) type=string
    What to implement. Tightly scoped and specific.
  scope_directive (optional) type=string
    Hard scope guardrail rendered into the agent's prompt.
  verify_command (optional) type=string default="go build ./... && go test ./... -timeout 60s"
    Shell command run by the verify stage. Default builds and runs targeted tests.

outputs:
  result (path) path=result.md
    Structured run summary the agent (or summary.sh) writes.

side effects:
  mutates_git    = true
  writes_files   = true
  network_egress = true
  idempotent     = false

node overrides:
  agent: class=hard_coding
```

### `kilroy describe review --pretty`

```
name:        review
version:     1
schema:      v2
source:      /Users/matt/.local/share/kilroy/workflows
dir:         /Users/matt/.local/share/kilroy/workflows/review
default class: hard_coding

agent description:
  Review a change and emit result.md plus review.json findings.

description:
  Review a change. The stage_context step gathers the diff to review
  (from a .patch file, a branch ref, or — TODO — a PR URL), the agent
  reads it alongside an optional checklist, and produces a
  human-readable review (result.md) plus a structured findings list
  (review.json). Idempotent and read-only — no code is changed and
  nothing is posted upstream.

inputs:
  checklist (optional) type=path
    Optional path to a review checklist file the agent should apply (e.g. project conventions, security checklist).
  context_files (optional) type=string
    Newline-separated paths the agent should read for context (design docs, related modules).
  scope_directive (optional) type=string
    Hard scope guardrail rendered into the agent's prompt.
  target (REQUIRED) type=string
    What to review. One of: a path to a .patch file, a branch ref (e.g. attractor/run/X), or a PR URL. PR URL handling is a TODO for v0 — pass a .patch or branch ref for now.

outputs:
  result (path) path=result.md
    Human-readable review: Summary, Findings (with severity), Verdict, Open questions.
  review.json (path)
    Structured list of findings: severity, file, line, message.

side effects:
  mutates_git    = false
  writes_files   = true
  network_egress = false
  idempotent     = true

node overrides:
  agent: class=hard_coding
```
