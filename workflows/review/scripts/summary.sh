#!/usr/bin/env bash
# Summary stage. Delegates BLOCKED-stub synthesis to the shared helper
# kilroy-write-result.sh (a sibling installed via symlink in this
# workflow's scripts/ directory). The review workflow has no verify
# stage — but it does have a workflow-specific diff.patch (under
# .kilroy/) that gets appended to a synthesized stub. review.json is
# guaranteed to exist (empty array == "no findings").
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKFLOW_NAME=review \
INPUT_KEY=target \
    bash "$SCRIPT_DIR/kilroy-write-result.sh"

# If the helper synthesized result.md, append the .kilroy/diff.patch
# appendix (the staged diff the agent was supposed to review).
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    if [ -f .kilroy/diff.patch ]; then
        BYTES=$(wc -c < .kilroy/diff.patch | tr -d ' ')
        {
            echo
            echo "## diff.patch"
            echo
            echo "Size: ${BYTES} bytes"
            if [ "$BYTES" -gt 0 ]; then
                echo
                echo '```diff'
                head -n 200 .kilroy/diff.patch
                echo '```'
            fi
        } >> result.md
    fi
fi

# Ensure review.json exists even if the agent skipped it, so the
# declared output contract holds. An empty array is a valid "no
# findings" state.
if [ ! -f review.json ]; then
    echo '[]' > review.json
fi

if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
