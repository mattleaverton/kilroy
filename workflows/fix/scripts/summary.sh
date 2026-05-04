#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
synthesized=0
# Re-entrancy: see implement/scripts/summary.sh for rationale.
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    synthesized=1
fi
if [ ! -f result.md ]; then
    synthesized=1
    {
        echo "# fix workflow result (synthesized)"
        echo
        echo "BLOCKED: agent did not write result.md."
        echo
        if [ -f .kilroy/verify.log ]; then
            echo "## verify.log (tail)"
            echo '```'
            tail -n 60 .kilroy/verify.log
            echo '```'
        fi
        if [ -f fix.patch ]; then
            BYTES=$(wc -c < fix.patch | tr -d ' ')
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
        fi
    } > result.md
fi
# Ensure fix.patch exists even if diff stage was skipped, so the
# declared output contract holds.
[ -f fix.patch ] || : > fix.patch
if [ "$synthesized" -eq 1 ]; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
