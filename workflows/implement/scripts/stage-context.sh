#!/usr/bin/env bash
# Inline context_files into INPUT.md so the agents can read them without
# repeated tool calls per iteration.
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"

section() {
    local name="$1"
    awk -v name="$name" '
        $0 == "## " name { in_section = 1; next }
        /^## / && in_section { in_section = 0 }
        in_section { print }
    ' "$INPUT" 2>/dev/null
}

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

SETUP_COMMAND="$(section setup_command | sed '/^[[:space:]]*$/d')"
if [ -n "$SETUP_COMMAND" ]; then
    mkdir -p .kilroy
    SETUP_SCRIPT=.kilroy/setup-command.sh
    SETUP_LOG=.kilroy/setup-output.txt
    {
        echo "#!/usr/bin/env bash"
        echo "set -euo pipefail"
        printf '%s\n' "$SETUP_COMMAND"
    } > "$SETUP_SCRIPT"
    {
        echo "setup_command:"
        printf '%s\n' "$SETUP_COMMAND"
        echo "---"
    } > "$SETUP_LOG"
    if ! bash "$SETUP_SCRIPT" >> "$SETUP_LOG" 2>&1; then
        RC=$?
        {
            echo "---"
            echo "exit_code: $RC"
        } >> "$SETUP_LOG"
        printf '{"status":"fail","failure_reason":"setup_command_failed"}\n' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
        exit 1
    fi
    echo "---" >> "$SETUP_LOG"
    echo "exit_code: 0" >> "$SETUP_LOG"
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
