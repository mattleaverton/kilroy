#!/usr/bin/env bash
# Capture build output for the critic. Always exits 0 — build failure is a
# signal for the critic, not a hard run-stopper. The critic reads
# .kilroy/build-output.txt and decides whether to flag CONTINUE.
set -uo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

CMD="$(sed -n '/^## verify_command$/,/^## /p' "$INPUT" 2>/dev/null \
       | sed '1d;$d' \
       | grep -v '^$' \
       | head -n 1)"
[ -z "$CMD" ] && CMD='go build ./... 2>&1'

mkdir -p .kilroy
LOG=.kilroy/build-output.txt
{
    echo "verify_command: $CMD"
    echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "---"
} > "$LOG"
bash -c "$CMD" >> "$LOG" 2>&1
RC=$?
echo "---" >> "$LOG"
echo "exit_code: $RC" >> "$LOG"

# Success regardless of build outcome — critic interprets the log.
echo '{"status":"success"}' > "$STATUS"
exit 0
