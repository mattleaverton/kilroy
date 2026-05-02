#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
if [ ! -f result.md ]; then
    {
        echo "# investigate workflow result (synthesized)"
        echo
        echo "BLOCKED: agent did not write result.md."
        echo
        echo "## what was asked"
        if [ -f .kilroy/INPUT.md ]; then
            sed -n '/^## question$/,/^## /p' .kilroy/INPUT.md | sed '1d;$d'
        else
            echo "(input file not found)"
        fi
    } > result.md
fi
echo '{"status":"success"}' > "$STATUS"
