#!/usr/bin/env bash
# Install JavaScript dependencies in isolated Kilroy worktrees before agents try
# to run package scripts or commit through project hooks.
set -uo pipefail

LOG="${1:-.kilroy/setup-output.txt}"
mkdir -p "$(dirname "$LOG")"

if [ ! -f package.json ]; then
    : > "$LOG"
    exit 0
fi

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    EXCLUDE_FILE="$(git rev-parse --git-path info/exclude 2>/dev/null || true)"
    if [ -n "$EXCLUDE_FILE" ]; then
        mkdir -p "$(dirname "$EXCLUDE_FILE")"
        grep -qxF "node_modules/" "$EXCLUDE_FILE" 2>/dev/null || printf '\nnode_modules/\n' >> "$EXCLUDE_FILE"
    fi
fi

if [ -d node_modules ] && [ -d node_modules/.bin ]; then
    {
        echo "setup_status: skipped"
        echo "reason: node_modules already present"
    } > "$LOG"
    exit 0
fi

CMD=""
if [ -f package-lock.json ]; then
    CMD="npm ci"
elif [ -f pnpm-lock.yaml ]; then
    CMD="pnpm install --frozen-lockfile"
elif [ -f yarn.lock ]; then
    CMD="yarn install --frozen-lockfile"
else
    CMD="npm install --no-package-lock"
fi

{
    echo "setup_status: running"
    echo "setup_command: $CMD"
    echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "---"
} > "$LOG"

bash -lc "$CMD" >> "$LOG" 2>&1
RC=$?

{
    echo "---"
    echo "exit_code: $RC"
} >> "$LOG"

exit "$RC"
