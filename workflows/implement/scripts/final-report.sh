#!/usr/bin/env bash
# Final report stage. Composes result.md from whatever artifacts the loop
# produced. Synthesizes a BLOCKED stub via the shared helper if no
# upstream stage left a result.md.
#
# Loop signals interpreted:
#   .kilroy/decision.md = COMPLETE   → loop succeeded
#   .kilroy/decision.md = CONTINUE   → loop hit max iterations without completion
#   .kilroy/decision.md missing/empty → fatal failure mid-loop (planner/coder/critic)
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DECISION=""
[ -f .kilroy/decision.md ] && DECISION="$(cat .kilroy/decision.md 2>/dev/null | tr -d ' \t\n\r')"

ITER_COUNT=0
if [ -d .kilroy/feedback ]; then
    ITER_COUNT=$(ls .kilroy/feedback/iter-*.md 2>/dev/null | wc -l | tr -d ' ')
fi

PATCH_FILE="implementation.patch"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    BASE="${KILROY_BASE_SHA:-}"
    if [ -z "$BASE" ] && [ -s .kilroy/base-sha.txt ]; then
        BASE="$(head -n 1 .kilroy/base-sha.txt | tr -d ' \t\r\n')"
    fi
    if [ -n "$BASE" ] && git rev-parse --verify "$BASE" >/dev/null 2>&1; then
        git diff "$BASE"..HEAD -- . \
            ':(exclude).kilroy/**' \
            ':(exclude)STATUS.md' \
            ':(exclude)result.md' \
            ':(exclude)implementation.patch' \
            > "$PATCH_FILE" 2>/dev/null || : > "$PATCH_FILE"
    else
        git diff HEAD~20..HEAD -- . \
            ':(exclude).kilroy/**' \
            ':(exclude)STATUS.md' \
            ':(exclude)result.md' \
            ':(exclude)implementation.patch' \
            > "$PATCH_FILE" 2>/dev/null || : > "$PATCH_FILE"
    fi
else
    : > "$PATCH_FILE"
fi

PATCH_BYTES=0
[ -f "$PATCH_FILE" ] && PATCH_BYTES=$(wc -c < "$PATCH_FILE" | tr -d ' ')

# Compose result.md ourselves — the relay produces enough structured artifacts
# that we don't need an LLM here. Helper kicks in only as last-resort fallback.
{
    echo "# implement result"
    echo
    if [ "$DECISION" = "COMPLETE" ]; then
        echo "**Status:** COMPLETE — critic confirmed all spec items implemented."
    elif [ "$DECISION" = "CONTINUE" ]; then
        echo "**Status:** INCOMPLETE — loop hit max iterations with critic still saying CONTINUE."
    else
        echo "**Status:** FAILED — loop aborted before critic reached a decision (planner/coder/critic crashed)."
    fi
    echo
    echo "**Iterations completed:** $ITER_COUNT"
    echo "**Implementation patch bytes:** $PATCH_BYTES"
    echo

    if [ -f .kilroy/INPUT.md ]; then
        echo "## Task packet"
        echo
        awk '
            $0 == "## task_packet" { in_section = 1; next }
            /^## / && in_section { in_section = 0 }
            in_section { print }
        ' .kilroy/INPUT.md
        echo
    fi

    if [ -f .kilroy/feedback/latest.md ]; then
        echo "## Latest critic review"
        echo
        cat .kilroy/feedback/latest.md
        echo
    fi

    if [ -f STATUS.md ]; then
        echo "## Latest status pulse"
        echo
        cat STATUS.md
        echo
    fi

    if [ -f .kilroy/build-output.txt ]; then
        echo "## Final build output (tail)"
        echo '```'
        tail -n 40 .kilroy/build-output.txt
        echo '```'
        echo
    fi

    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "## Commits this run"
        echo '```'
        if [ -n "${BASE:-}" ] && git rev-parse --verify "$BASE" >/dev/null 2>&1; then
            git log --oneline "$BASE"..HEAD 2>/dev/null
        else
            git log --oneline -20 2>/dev/null
        fi
        echo '```'
    fi
} > result.md

# Fall back to the shared helper if (somehow) result.md is empty.
if [ ! -s result.md ]; then
    WORKFLOW_NAME=implement \
    INPUT_KEY=task_packet \
    INCLUDE_DIFF=true \
    INCLUDE_VERIFY_LOG=false \
        bash "$SCRIPT_DIR/kilroy-write-result.sh"
fi

# Stage-status mapping:
#   COMPLETE → success → done terminal
#   anything else → fail → failed terminal
if [ "$DECISION" = "COMPLETE" ]; then
    if [ "$PATCH_BYTES" = "0" ] && [ ! -s .kilroy/no-op.md ]; then
        printf '{"status":"fail","failure_reason":"complete_with_empty_patch"}\n' > "$STATUS"
        exit 1
    fi
    echo '{"status":"success"}' > "$STATUS"
    exit 0
fi
printf '{"status":"fail","failure_reason":"loop_did_not_complete"}\n' > "$STATUS"
exit 1
