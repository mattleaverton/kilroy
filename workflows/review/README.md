# review

A read-only code review workflow. Given a patch file or branch ref, it runs an agent that reads the diff, applies optional context, and writes a human-readable summary (`result.md`) plus a structured findings list (`review.json`). Nothing is posted upstream and the workflow is not intended to change the code under review.

## What it does

1. **stage_context** — resolves `target` to a unified diff and writes it to `.kilroy/diff.patch`.
2. **agent** (`class=hard_coding`) — reads the diff and optional inputs, then produces `result.md` and `review.json`.

The workflow manifest marks it idempotent and read-only from the target repo's perspective (`mutates_git=false`, `network_egress=false`). It still uses the configured LLM backend for the review agent.

## When to use it

Use `review` when you have a patch file or branch ref you want evaluated before merging. Typical cases:

- Automated review gate in a CI-adjacent pipeline.
- Pre-merge sanity check inside an agent loop.
- One-shot human-requested review of a local branch.

PR URL targets are not implemented yet. Pass a `.patch` file or a branch ref such as `attractor/run/01ABC`.

## Input contract

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `target` | yes | string | What to review. Use a path to a `.patch` file or a branch ref. PR URL support is TODO. |
| `checklist` | no | path | Path to a review checklist file the agent should apply. |
| `context_files` | no | string | Newline-separated paths to files the agent should read for context. |
| `scope_directive` | no | string | Hard scope guardrail rendered into the agent prompt. |

## Launch examples

### Review a patch file

Create an input file:

```yaml
# review-patch.yaml
target: /abs/path/to/changes.patch
```

Then run:

```bash
kilroy run review --detach --wait --input review-patch.yaml
```

### Review a branch ref

```yaml
# review-branch.yaml
target: attractor/run/01KQSZ2C4ACM8YPKRWJ5JNSZAM
checklist: /abs/path/to/checklist.md
context_files: |
  /abs/path/to/design-doc.md
  /abs/path/to/related-module.go
```

```bash
kilroy run review --detach --wait --input review-branch.yaml
```

### With a scope directive

```yaml
# review-scoped.yaml
target: /abs/path/to/changes.patch
scope_directive: "Focus only on the authentication subsystem. Ignore UI changes."
```

```bash
kilroy run review --detach --wait --input review-scoped.yaml
```

### Non-blocking

```bash
kilroy run review --detach --input review-patch.yaml --label task=my-pr-review
```

Inspect the run and its outputs:

```bash
kilroy runs show <run-id> --outputs
kilroy runs show <run-id> --print result.md
kilroy runs show <run-id> --print review.json
```

## Output contract

Both files are written to the worktree root by the agent.

### `result.md`

Human-readable review structured as:

1. **Summary** — 1-3 sentences covering what the change does and the overall take.
2. **Findings** — grouped by severity: `blocker`, `major`, `minor`, then `info`. Each finding includes file/line when meaningful, a headline, and detail.
3. **Verdict** — one of:
   - `MERGE` — ready as-is.
   - `MERGE-FIX` — merge first, address findings as follow-up.
   - `FIX-MERGE` — fix listed blockers/majors, then merge.
   - `REJECT` — fundamental rework needed.
4. **Open questions** — anything the agent could not resolve without more context.

### `review.json`

A JSON array of findings. Each element:

```json
{
  "severity": "blocker|major|minor|info",
  "file": "path/to/file.go",
  "line": 42,
  "message": "What's wrong and why it matters."
}
```

Use `[]` when there are no actionable findings. Use `null` for `line` on file-level findings.

## Routing note

The agent node uses `class=hard_coding`, so the concrete provider/driver comes from policy resolution. On machines where `hard_coding` resolves to a CLI/tmux driver, launch detached and inspect outputs through the run database:

```bash
kilroy run review --detach --wait --input review-patch.yaml --label task=my-pr-review
kilroy runs show --latest --label task=my-pr-review --outputs
kilroy runs show --latest --label task=my-pr-review --print result.md
```

If policy routes `hard_coding` to an SDK/API driver instead, the same command shape is still valid.

## Known limits

- PR URLs are TODO. Pass a `.patch` path or branch ref; URL targets do not gather a PR diff today.
- The workflow is designed as read-only. Agent edits would violate the prompt and should be treated as a run-quality failure.
- Broad `go test ./...` is intentionally discouraged by the review prompt; targeted spot-checks are allowed when confirming a specific finding.
- Nothing is posted upstream. The caller is responsible for acting on `result.md` and `review.json`.
