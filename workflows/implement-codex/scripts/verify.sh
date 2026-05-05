#!/usr/bin/env bash
set -uo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

# Extract verify_command from INPUT.md, default if absent or empty.
CMD="$(sed -n '/^## verify_command$/,/^## /p' "$INPUT" 2>/dev/null \
       | sed '1d;$d' \
       | grep -v '^$' \
       | head -n 1)"
[ -z "$CMD" ] && CMD='go build ./... && go test ./... -timeout 60s'

mkdir -p .kilroy
LOG=.kilroy/verify.log
echo "verify_command: $CMD" > "$LOG"
echo "---" >> "$LOG"
bash -c "$CMD" >> "$LOG" 2>&1
RC=$?
if [ $RC -eq 0 ]; then
    echo '{"status":"success"}' > "$STATUS"
else
    printf '{"status":"fail","reason":"verify_command exit %d"}' "$RC" > "$STATUS"
fi
exit 0
