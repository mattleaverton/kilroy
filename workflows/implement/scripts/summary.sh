#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
if [ ! -f result.md ]; then
    {
        echo "# implement workflow result (synthesized)"
        echo
        echo "BLOCKED: agent did not write result.md."
        echo
        if [ -f .kilroy/verify.log ]; then
            echo "## verify.log (tail)"
            echo '```'
            tail -n 60 .kilroy/verify.log
            echo '```'
        fi
    } > result.md
fi
echo '{"status":"success"}' > "$STATUS"
