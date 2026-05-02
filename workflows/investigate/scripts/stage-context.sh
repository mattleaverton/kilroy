#!/usr/bin/env bash
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
# Engine wrote ## question, ## context_files, ## urls, etc. Optional:
# inline the contents of any listed context files for the agent's
# convenience. URLs stay as references — the agent's tooling fetches
# those itself if needed.
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
echo '{"status":"success"}' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
