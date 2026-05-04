#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
synthesized=0
# Re-entrancy: see implement/scripts/summary.sh for rationale.
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    synthesized=1
fi

# Synthesize result.md if the agent didn't write one (BLOCKED stub).
if [ ! -f result.md ]; then
    synthesized=1
    {
        echo "# review workflow result (synthesized)"
        echo
        echo "BLOCKED: agent did not write result.md."
        echo
        if [ -f .kilroy/diff.patch ]; then
            BYTES=$(wc -c < .kilroy/diff.patch | tr -d ' ')
            echo "## diff.patch"
            echo
            echo "Size: ${BYTES} bytes"
            if [ "$BYTES" -gt 0 ]; then
                echo
                echo '```diff'
                head -n 200 .kilroy/diff.patch
                echo '```'
            fi
        fi
    } > result.md
fi

# Ensure review.json exists even if the agent skipped it, so the
# declared output contract holds. An empty array is a valid "no
# findings" state.
if [ ! -f review.json ]; then
    echo '[]' > review.json
fi

if [ "$synthesized" -eq 1 ]; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
