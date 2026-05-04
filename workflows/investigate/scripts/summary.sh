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
if [ "$synthesized" -eq 1 ]; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
