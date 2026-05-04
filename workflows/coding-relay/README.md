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
| `.kilroy/task.md`             | planner    | Current iteration's scoped sub-task. |
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

This workflow surfaces a real gap in the auth-binder model. See
`UPSTREAM-FEEDBACK.md` in this directory for the running list. As of authoring:

- **`KIMI_API_KEY` must be set in the environment** before launching. Kilroy
  does not yet have a binding for kimi (kimi is not in
  `~/.config/kilroy/auth.toml` template).
- **opencode is outside the auth-binder model** (documented at
  `internal/attractor/agents/templates/opencode.go:36`). The opencode
  template currently hard-codes the anthropic provider in
  `OPENCODE_CONFIG_CONTENT`, so routing kimi (or any non-anthropic provider)
  through `agent_tool=opencode` requires an upstream change to that template.
- **`quick_easy` class on the status node will route through claude_cli +
  tmux** on machines with `claude` logged in. That's correct behavior for
  this exercise but slower per-iteration than a haiku API call. Override the
  class to a literal `agent_mode=agent_loop` + `llm_provider=anthropic` +
  `llm_model=claude-haiku-4.5` if you want SDK-only.
