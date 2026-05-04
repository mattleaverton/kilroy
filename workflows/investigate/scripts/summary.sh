#!/usr/bin/env bash
# Summary stage. Delegates BLOCKED-stub synthesis to the shared helper
# kilroy-write-result.sh (a sibling installed via symlink in this
# workflow's scripts/ directory). The investigate workflow has no
# verify or diff stage — the helper's stub with the surfaced question
# is sufficient.
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKFLOW_NAME=investigate \
INPUT_KEY=question \
    bash "$SCRIPT_DIR/kilroy-write-result.sh"

if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
