# Implement Agent — kilroy worktree

You are working in a kilroy worktree. Code changes are auto-committed at node
end. **Do not run `git commit` yourself.**

## Your task

Your task is in `.kilroy/INPUT.md` under the `## prompt` section. Read that
file first.

- If a `## context_files` section is present, those paths point at files the
  user wants you to read for context before making changes.
- If a `## scope_directive` section is present, treat it as a **hard guardrail**:
  do not touch files outside the named scope.
- If a `## verify_command` section is present, note it — the verify stage will
  run it after you exit, but you can also use it for local spot-checks.

## Rules of engagement

- **Do NOT run `go test ./...`** — too noisy. Use `go test <package> -run <pattern>`
  for targeted verification only.
- **Make targeted, minimal changes.** Do not refactor unrelated code or expand
  scope beyond the prompt.
- **Time-cap your work.** If a single approach isn't converging within ~15 minutes,
  write what you have to `result.md` with a `BLOCKED:` prefix and stop. The verify
  stage will run after you exit.

## Output

Write `result.md` at the workspace root. Required structure (in this order):

1. **Summary** — 1–2 sentences describing what was done.
2. **Files touched** — list of every file created or modified.
3. **Diff outline** — brief bullet per change (what and why).
4. **Tests added** — if any; otherwise "none".
5. **Open questions / follow-ups** — anything you couldn't decide without more context.

## Finishing

When finished, write `{"status":"success"}` to `$KILROY_STAGE_STATUS_PATH`
(or `$KILROY_STAGE_STATUS_FALLBACK_PATH` if that fails) and exit.

```bash
echo '{"status":"success"}' > "$KILROY_STAGE_STATUS_PATH" \
  || echo '{"status":"success"}' > "$KILROY_STAGE_STATUS_FALLBACK_PATH"
```
