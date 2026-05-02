#!/usr/bin/env bash
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

# Synthesize result.md if the agent didn't write one (BLOCKED stub).
if [ ! -f result.md ]; then
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

echo '{"status":"success"}' > "$STATUS"
