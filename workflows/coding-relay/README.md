# coding-relay

Background-friendly iterative coding loop with **four** roles handed off in a relay:

| Role     | Driver path           | Provider/model              | Cost profile     |
|----------|-----------------------|-----------------------------|------------------|
| Planner  | `anthropic_sdk` (API) | `anthropic / claude-sonnet-4.6` | mid              |
| Coder    | `opencode` (CLI/tmux) | `kimi / kimi-k2`            | cheap, fast      |
| Critic   | `codex_cli` (CLI/tmux)| `openai / gpt-5`            | high but slow    |
| Status   | `agent_class=quick_easy` (resolved via policy) | machine-dependent (haiku/sonnet) | very cheap |

Each iteration runs all four. The critic owns the loop decision via
`.kilroy/decision.md` (`COMPLETE` or `CONTINUE`). The status node refreshes
`STATUS.md` at the workspace root every iteration so an external poller can
see progress without reading run logs.

## When to use

You're an agent (or a person) and you have a coding task that's:

1. Specifiable in a markdown file (a few paragraphs of intent + acceptance criteria),
2. Plausibly doable in 6 narrow iterations, and
3. Worth running in the background while you work on something else.

You hand it a prompt file, run it detached, and check `STATUS.md` periodically.

## Why mixed providers

This workflow is intentionally heterogeneous to dogfood the V2 routing surface:

- **SDK path** (planner): exercises `anthropic_sdk` driver dispatch and the API-key auth chain.
- **Two CLI paths** (coder + critic): different binaries (opencode + codex), different providers (kimi + openai), different auth methods (env + cli_oauth).
- **Class-resolved path** (status): exercises the policy class chain — what the user's machine has logged in determines the actual driver/model. On a machine with claude logged in, `quick_easy` resolves to `claude_cli + claude-haiku-4.5 + cli_oauth`.

A workflow that uses one provider end-to-end can't surface routing bugs the
way this one can.

## Inputs

| Key              | Required | Description |
|------------------|----------|-------------|
| `prompt`         | yes      | Absolute path to a markdown file describing the goal/spec. |
| `context_files`  | no       | Newline-separated absolute paths to inline as context. |
| `verify_command` | no       | Shell command for the post-coder build check. Default `go build ./... 2>&1`. |

## Outputs

| File                          | Written by | Notes                                    |
|-------------------------------|------------|------------------------------------------|
| `STATUS.md`                   | status     | Refreshed every iteration. External-readable progress overview. |
| `result.md`                   | final-report | Composed at end of run. |
| `.kilroy/plan.md`             | planner    | Current iteration's scoped sub-task. (Named to avoid collision with the engine-managed `.kilroy/TASK.md` on case-insensitive filesystems.) |
| `.kilroy/feedback/iter-NNN.md`| critic     | Per-iteration feedback (committed). |
| `.kilroy/feedback/latest.md`  | critic     | Rolling latest feedback. |
| `.kilroy/decision.md`         | critic     | `COMPLETE` or `CONTINUE` — loop signal. |
| `.kilroy/build-output.txt`    | verify-build | Build log captured for the critic. |

## How to launch

```bash
kilroy run coding-relay --detach \
  --workspace /abs/path/to/target-repo \
  --input-file prompt=/abs/path/to/spec.md
```

Then poll:

```bash
watch -n 10 cat /abs/path/to/target-repo/STATUS.md
```

Or block on completion:

```bash
kilroy run coding-relay --detach --wait \
  --workspace /abs/path/to/target-repo \
  --input-file prompt=/abs/path/to/spec.md
```

## Known prerequisites / gaps

This workflow intentionally exercises mixed routing. See `UPSTREAM-FEEDBACK.md`
in this directory for the historical feedback that led to the current template
behavior.

- The opencode coder route uses Kimi. You must have a Kimi credential
  available in the launch environment: the coder prefers `KIMI_API_KEY_KILROY`
  and falls back to `KIMI_API_KEY`. The opencode template builds
  provider-aware `OPENCODE_CONFIG_CONTENT` from the resolved route.
- `quick_easy` on the status node resolves through policy. On machines with
  Claude logged in, it commonly routes through `claude_cli` + tmux. That is
  correct behavior for this routing exercise, but slower per iteration than a
  literal Haiku SDK node.
