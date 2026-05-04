#!/usr/bin/env bash
# Inline context_files into INPUT.md so the agents can read them without
# repeated tool calls per iteration. Mirrors workflows/implement/scripts/stage-context.sh.
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"

if grep -q '^## context_files' "$INPUT" 2>/dev/null; then
    {
        echo
        echo "## context_files_contents"
        echo
        sed -n '/^## context_files$/,/^## /p' "$INPUT" \
            | grep -v '^##' \
            | sed '/^$/d' \
            | while IFS= read -r path; do
                if [ -n "$path" ] && [ -f "$path" ]; then
                    echo "### $path"
                    echo '```'
                    cat "$path"
                    echo '```'
                    echo
                fi
            done
    } >> "$INPUT"
fi

# Seed empty feedback dir + decision file so iter-1 readers don't error.
mkdir -p .kilroy/feedback
: > .kilroy/build-output.txt
touch .kilroy/decision.md

echo '{"status":"success"}' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
