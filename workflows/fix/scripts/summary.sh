#!/usr/bin/env bash
# Summary stage. Delegates BLOCKED-stub synthesis to the shared helper
# kilroy-write-result.sh (a sibling installed via symlink in this
# workflow's scripts/ directory). The fix workflow appends a fix.patch
# appendix specific to its declared output, then preserves the F2
# contract (exit 1 + status=fail when the stub had to be synthesized).
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKFLOW_NAME=fix \
INPUT_KEY=issue \
INCLUDE_VERIFY_LOG=true \
    bash "$SCRIPT_DIR/kilroy-write-result.sh"

# If the helper synthesized result.md, append the fix.patch appendix
# (size + first 200 lines) so triage can see what — if anything — the
# agent committed before bailing.
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    if [ -f fix.patch ]; then
        BYTES=$(wc -c < fix.patch | tr -d ' ')
        {
            echo
            echo "## fix.patch"
            echo
            echo "Size: ${BYTES} bytes"
            if [ "$BYTES" -gt 0 ]; then
                echo
                echo '```diff'
                head -n 200 fix.patch
                echo '```'
            fi
        } >> result.md
    fi
fi

# Ensure fix.patch exists even if diff stage was skipped, so the
# declared output contract holds.
[ -f fix.patch ] || : > fix.patch

if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
