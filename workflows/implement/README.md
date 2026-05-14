# implement

Public implementation workflow for the three-step Kilroy demo surface:

```text
plan -> implement -> validate
```

`implement` consumes the task packet, testing plan, and validation plan produced
by `plan`, then runs an iterative planner/coder/critic/status loop until the
critic writes `COMPLETE` or the loop cap is reached.

## Roles

| Role | Route | Responsibility |
|---|---|---|
| Planner | Anthropic SDK | Pick one narrow next sub-task from the task/testing/validation packet. |
| Coder | opencode + Kimi | Implement exactly that sub-task and commit code/test changes. |
| Critic | Codex CLI | Judge strictly against the packet; write `COMPLETE` or `CONTINUE`. |
| Status | `quick_easy` class | Refresh `STATUS.md` for external monitoring. |

The exit criteria are intentionally strict. A `COMPLETE` result with an empty
`implementation.patch` fails unless the run produced an explicit `.kilroy/no-op.md`
with evidence.

## Inputs

| Key | Required | Description |
|---|---|---|
| `task_packet` | yes | Task packet produced by `plan`. |
| `testing_plan` | no | Testing plan produced by `plan`. |
| `validation_plan` | no | Validation plan produced by `plan`. |
| `context_files` | no | Newline-separated absolute paths to inline as context. |
| `setup_command` | no | Optional setup script for the isolated worktree, only when authorized. |
| `verify_command` | no | Post-coder build/test command. If omitted, Kilroy infers from package scripts or Go files. |

## Outputs

| File | Notes |
|---|---|
| `result.md` | Final run summary with latest critic/status/build evidence. |
| `implementation.patch` | Diff against the launch SHA, excluding Kilroy run artifacts. |
| `STATUS.md` | Live progress overview refreshed during the loop. |

## Launch

```bash
kilroy run implement \
  --input-file task_packet=/abs/session/task-packet.md \
  --input-file testing_plan=/abs/session/testing-plan.md \
  --input-file validation_plan=/abs/session/validation-plan.md \
  --input-file verify_command=/abs/session/verify-command.txt \
  --label session=<session> \
  --label phase=implement \
  --label task=<slug> \
  --label scope=<area>
```

## Prerequisites

This workflow intentionally exercises mixed routing:

- Planner uses Anthropic API credentials.
- Coder uses opencode with Kimi (`KIMI_API_KEY_KILROY` preferred).
- Critic uses Codex CLI.
- Status resolves through the local `quick_easy` policy class.

The planner/coder are given the actual `package.json` scripts when present and
should not invent npm script names. Dependency installation or other networked
setup happens only through an explicit `setup_command` input.
