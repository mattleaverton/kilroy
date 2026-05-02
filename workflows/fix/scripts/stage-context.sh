#!/usr/bin/env bash
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
# Engine wrote ## issue, ## context_files, etc. already. Optional: append
# the contents of any listed context files for the agent's convenience.
if grep -q '^## context_files' "$INPUT" 2>/dev/null; then
    {
        echo
        echo "## context_files_contents"
        echo
        # Extract paths from the ## context_files section
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
echo '{"status":"success"}' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
