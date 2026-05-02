#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
if [ ! -f result.md ]; then
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
echo '{"status":"success"}' > "$STATUS"
