# coding-loop

An iterative coding workflow that repeatedly chooses sub-tasks, implements them, reviews the results, and decides when the spec is complete.

## What it does

Runs up to 12 loops of:

1. **Task Chooser** — reads the spec + latest review, picks the highest-priority unimplemented sub-task, writes `.kilroy/plan.md`.
2. **Implementer** — reads the task, writes code, commits.
3. **Reviewer** — diffs HEAD~1..HEAD against the spec, writes `.reviews/iter-NNN.md` and `.reviews/latest.md`, commits.
4. **Done Gate** — reads spec + latest review, writes `COMPLETE` or `CONTINUE` to `.kilroy/decision.md`.

When Done Gate writes `COMPLETE` (or `loop_max=12` is reached), the loop exits and a **Report** node writes `result.md`.

## How to launch

```bash
cat > /tmp/coding-loop-input.yaml <<'YAML'
spec: /abs/path/to/spec.md
YAML

kilroy run coding-loop --input /tmp/coding-loop-input.yaml --sync
```

Run from the target repo. Use `--sync` if you want to block until completion.

- `--input` — path to a YAML or JSON input file. For this workflow, `spec` must be the absolute path to the spec/requirements file. Do not use `--input-file spec=...`; that would inline the spec contents, but this older workflow expects a path value and then reads the file itself.

## Input contract

| Key    | Required | Description |
|--------|----------|-------------|
| `spec` | yes      | Absolute path to the spec/requirements markdown file |

## Output contract

| File             | Description |
|------------------|-------------|
| `result.md`      | Summary: what was implemented, iterations run, final status |
| `.kilroy/plan.md` | Current iteration's scoped sub-task. Named to avoid collision with the engine-managed `.kilroy/TASK.md` on case-insensitive filesystems. |
| `.reviews/iter-NNN.md` | Per-iteration reviewer feedback (committed to repo) |
| `.reviews/latest.md`   | Rolling copy of the most recent review |

## Known limits

- `loop_max=12` — hard ceiling; if the done-gate never writes `COMPLETE` after 12 iterations, the run fails.
- Chooser and done-gate use `claude-haiku-4.5` via the Anthropic SDK path. Implementer and reviewer use `claude-sonnet-4.6` through the Claude CLI path selected by `agent_tool="claude"`.
- Spec is NOT committed to the target repo — it is read in-place via the `spec` input path.
- No pre-flight scaffolding — the caller must initialize the repo before launching.
