#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

# Capture the run's pending change as fix.patch. Each prior agent and
# script node auto-commits to the run branch (commit_per_node), so HEAD
# has the agent's edits already. The base we want is the user's launch
# HEAD — the first commit on this branch that isn't an attractor per-node
# commit. Walking the log finds it deterministically without depending on
# remote tracking, reflog, or worktree branch metadata.
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo '{"status":"success","note":"not a git worktree; skipping fix.patch"}' > "$STATUS"
    : > fix.patch
    exit 0
}

LAUNCH_HEAD=""
while IFS=$'\t' read -r sha subj; do
    case "$subj" in
        "attractor("*)
            continue ;;
        *)
            LAUNCH_HEAD="$sha"
            break ;;
    esac
done < <(git log --format='%H%x09%s' 2>/dev/null)

# If no non-attractor commit was found (defensive — shouldn't happen with
# a normal launch), fall back to the genesis commit so the diff at least
# captures everything that exists.
if [ -z "$LAUNCH_HEAD" ]; then
    LAUNCH_HEAD="$(git rev-list --max-parents=0 HEAD 2>/dev/null | tail -n 1)"
fi

git diff --no-color --no-ext-diff "$LAUNCH_HEAD" HEAD -- . \
    ':(exclude).gitignore' \
    ':(exclude)result.md' \
    ':(exclude)fix.patch' \
    ':(exclude)status.json' \
    ':(exclude).kilroy/**' \
    > fix.patch 2>/dev/null || : > fix.patch
SIZE=$(wc -c < fix.patch | tr -d ' ')
echo "{\"status\":\"success\",\"patch_bytes\":$SIZE,\"base\":\"$LAUNCH_HEAD\"}" > "$STATUS"
exit 0
