#!/usr/bin/env bash
# Inline context_files into INPUT.md so the agents can read them without
# repeated tool calls per iteration.
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

if [ -f package.json ]; then
    {
        echo
        echo "## project_scripts"
        echo
        if command -v node >/dev/null 2>&1; then
            node - <<'NODE' 2>/dev/null || true
const fs = require("fs");
const pkg = JSON.parse(fs.readFileSync("package.json", "utf8"));
const scripts = pkg.scripts || {};
for (const name of Object.keys(scripts).sort()) {
  console.log(`- ${name}: ${scripts[name]}`);
}
NODE
        else
            sed -n '/"scripts"[[:space:]]*:/,/^[[:space:]]*}/p' package.json || true
        fi
    } >> "$INPUT"
fi

if [ -f package.json ]; then
    if ! bash "$SCRIPT_DIR/node-setup.sh" .kilroy/setup-output.txt; then
        printf '{"status":"fail","failure_reason":"dependency_setup_failed"}\n' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
        exit 1
    fi
fi

# Seed empty feedback dir + decision file so iter-1 readers don't error.
mkdir -p .kilroy/feedback
: > .kilroy/build-output.txt
touch .kilroy/decision.md
if [ -n "${KILROY_BASE_SHA:-}" ]; then
    printf '%s\n' "$KILROY_BASE_SHA" > .kilroy/base-sha.txt
else
    git rev-parse HEAD > .kilroy/base-sha.txt 2>/dev/null || true
fi

echo '{"status":"success"}' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
