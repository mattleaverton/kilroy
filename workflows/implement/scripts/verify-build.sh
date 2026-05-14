#!/usr/bin/env bash
# Capture build output for the critic. Always exits 0 — build failure is a
# signal for the critic, not a hard run-stopper. The critic reads
# .kilroy/build-output.txt and decides whether to flag CONTINUE.
set -uo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

section() {
    local name="$1"
    awk -v name="$name" '
        $0 == "## " name { in_section = 1; next }
        /^## / && in_section { in_section = 0 }
        in_section { print }
    ' "$INPUT" 2>/dev/null
}

infer_verify_command() {
    if [ -f package.json ] && command -v node >/dev/null 2>&1; then
        node - <<'NODE' 2>/dev/null && return 0
const fs = require("fs");
const scripts = JSON.parse(fs.readFileSync("package.json", "utf8")).scripts || {};
for (const name of ["turbo:check", "check", "typecheck", "lint", "test"]) {
  if (scripts[name]) {
    console.log(name === "test" ? "npm test 2>&1" : `npm run ${name} 2>&1`);
    process.exit(0);
  }
}
process.exit(1);
NODE
    fi
    if ls go.mod ./*.go >/dev/null 2>&1; then
        printf '%s\n' 'go build ./... 2>&1'
        return 0
    fi
    printf '%s\n' 'git status --short 2>&1'
}

CMD="$(section verify_command | sed '/^[[:space:]]*$/d')"
[ -n "$CMD" ] || CMD="$(infer_verify_command)"

mkdir -p .kilroy
LOG=.kilroy/build-output.txt
{
    echo "verify_command: $CMD"
    echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "---"
} > "$LOG"
VERIFY_SCRIPT=.kilroy/verify-command.sh
{
    echo "#!/usr/bin/env bash"
    echo "set -euo pipefail"
    printf '%s\n' "$CMD"
} > "$VERIFY_SCRIPT"
bash "$VERIFY_SCRIPT" >> "$LOG" 2>&1
RC=$?
find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} + 2>/dev/null || true
echo "---" >> "$LOG"
echo "exit_code: $RC" >> "$LOG"

# Success regardless of build outcome — critic interprets the log.
echo '{"status":"success"}' > "$STATUS"
exit 0
