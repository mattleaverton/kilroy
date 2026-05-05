#!/usr/bin/env bash
# kilroy-write-result.sh — synthesize a triage-friendly BLOCKED stub for
# result.md when the agent stage didn't produce one.
#
# Workflow summary stages historically duplicated the same "if result.md
# is missing, build a stub with the input prompt + optional diff + optional
# verify log" logic. This helper consolidates that logic so each workflow's
# summary script becomes a thin caller.
#
# Distribution / convention:
#   * Canonical source lives here (scripts/kilroy-write-result.sh) so tests
#     and humans have one file to edit.
#   * Each workflow that wants the helper installs it into its own
#     scripts/ directory via a relative symlink (e.g.
#     workflows/implement/scripts/kilroy-write-result.sh ->
#     ../../../scripts/kilroy-write-result.sh). Package materialization
#     (internal/attractor/workflows/package.go::copyTree) resolves the
#     symlink during MaterializeTo and writes a regular file into the
#     workspace's .kilroy/package/scripts/, so at run time the helper is
#     a sibling of the workflow's other stage scripts. No engine changes
#     required.
#   * A future follow-up may add explicit shared-helper plumbing to the
#     materializer; until then, the symlink convention keeps the source
#     of truth single.
#
# Contract:
#   * Always exits 0. The caller (the workflow summary script) inspects the
#     resulting file — checking for the "(synthesized)" marker on line 1 —
#     and writes the stage status accordingly. This preserves the F2
#     contract: summary fails when a stub had to be synthesized.
#   * Idempotent: if $RESULT_FILE already exists (synthesized stub from a
#     prior attempt OR a real file the agent wrote), the helper does
#     nothing. The marker check stays in the caller.
#
# Env vars:
#   RESULT_FILE         Output path. Default: result.md
#   WORKFLOW_NAME       Name shown in the synthesized header. Default: workflow
#   INPUT_KEY           Section in .kilroy/INPUT.md to surface (e.g. "prompt",
#                       "question", "issue"). Default: prompt
#   INCLUDE_DIFF        If "true", append a git diff vs $KILROY_BASE_SHA
#                       (or .kilroy/BASE_SHA). Default: false
#   INCLUDE_VERIFY_LOG  If "true", append the tail of .kilroy/verify.log.
#                       Default: false
#
# Args: none.

set -uo pipefail

RESULT_FILE="${RESULT_FILE:-result.md}"
WORKFLOW_NAME="${WORKFLOW_NAME:-workflow}"
INPUT_KEY="${INPUT_KEY:-prompt}"
INCLUDE_DIFF="${INCLUDE_DIFF:-false}"
INCLUDE_VERIFY_LOG="${INCLUDE_VERIFY_LOG:-false}"

if [ -e "$RESULT_FILE" ]; then
    exit 0
fi

{
    echo "# $WORKFLOW_NAME workflow result (synthesized)"
    echo
    echo "BLOCKED: agent did not write $RESULT_FILE."
    echo

    if [ -f .kilroy/INPUT.md ]; then
        echo "## $INPUT_KEY"
        echo
        awk -v key="## $INPUT_KEY" '
            $0 == key { in_section = 1; next }
            /^## / && in_section { in_section = 0 }
            in_section { print }
        ' .kilroy/INPUT.md
        echo
    fi

    if [ "$INCLUDE_DIFF" = "true" ] && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        base_sha=""
        if [ -n "${KILROY_BASE_SHA:-}" ]; then
            base_sha="$KILROY_BASE_SHA"
        elif [ -f .kilroy/BASE_SHA ]; then
            base_sha=$(cat .kilroy/BASE_SHA 2>/dev/null)
        fi
        if [ -n "$base_sha" ] && git rev-parse --verify "$base_sha" >/dev/null 2>&1; then
            echo "## git diff (since launch HEAD: $base_sha)"
            echo
            echo '```diff'
            git diff "$base_sha"...HEAD --stat 2>/dev/null
            echo
            git diff "$base_sha"...HEAD 2>/dev/null | head -c 16384
            echo '```'
            echo
        fi
    fi

    if [ "$INCLUDE_VERIFY_LOG" = "true" ] && [ -f .kilroy/verify.log ]; then
        echo "## verify.log (tail)"
        echo '```'
        tail -n 60 .kilroy/verify.log
        echo '```'
    fi
} > "$RESULT_FILE"

exit 0
