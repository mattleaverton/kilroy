#!/usr/bin/env bash
set -uo pipefail

INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
mkdir -p .kilroy

section() {
    local name="$1"
    awk -v name="$name" '
        $0 == "## " name { in_section = 1; next }
        /^## / && in_section { in_section = 0 }
        in_section { print }
    ' "$INPUT" 2>/dev/null
}

TASK_PACKET="$(section task_packet)"
VALIDATION_PLAN="$(section validation_plan)"
VALIDATION_COMMAND="$(section validation_command | sed '/^[[:space:]]*$/d')"
TARGET="$(section target | sed '/^$/d' | head -n 1)"

COMMAND_STATUS="skipped"
COMMAND_EXIT=0
COMMAND_OUTPUT=""
if [ -n "$VALIDATION_COMMAND" ]; then
    COMMAND_STATUS="pass"
    COMMAND_SCRIPT=".kilroy/validation-command.sh"
    {
        echo "#!/usr/bin/env bash"
        echo "set -euo pipefail"
        printf '%s\n' "$VALIDATION_COMMAND"
    } > "$COMMAND_SCRIPT"
    COMMAND_OUTPUT="$(bash "$COMMAND_SCRIPT" 2>&1)" || {
        COMMAND_EXIT=$?
        COMMAND_STATUS="fail"
    }
fi

GIT_STATUS="$(git status --short 2>/dev/null || true)"
GIT_DIFF_STAT="$(git diff --stat 2>/dev/null || true)"

{
    echo "# Validation Evidence"
    echo
    if [ "$COMMAND_STATUS" = "fail" ]; then
        echo "**Terminal state:** FAILED_VALIDATION"
    elif [ "$COMMAND_STATUS" = "pass" ]; then
        echo "**Terminal state:** PR_READY"
    else
        echo "**Terminal state:** PR_READY"
    fi
    echo
    echo "## Target"
    echo
    echo "${TARGET:-current workspace}"
    echo
    echo "## Task Packet"
    echo
    echo "${TASK_PACKET:-not provided}"
    echo
    echo "## Validation Plan"
    echo
    echo "${VALIDATION_PLAN:-not provided}"
    echo
    echo "## Command"
    echo
    if [ -n "$VALIDATION_COMMAND" ]; then
        echo '```bash'
        echo "$VALIDATION_COMMAND"
        echo '```'
        echo
        echo "exit_code: $COMMAND_EXIT"
        echo
        echo '```'
        printf '%s\n' "$COMMAND_OUTPUT" | tail -120
        echo '```'
    else
        echo "No validation_command provided; captured git status only."
    fi
    echo
    echo "## Git Status"
    echo
    echo '```'
    printf '%s\n' "$GIT_STATUS"
    echo '```'
    echo
    echo "## Git Diff Stat"
    echo
    echo '```'
    printf '%s\n' "$GIT_DIFF_STAT"
    echo '```'
} > evidence.md

python3 - "$COMMAND_STATUS" "$COMMAND_EXIT" "$VALIDATION_COMMAND" "$TARGET" <<'PY' > evidence.json
import json
import sys

status, exit_code, command, target = sys.argv[1:5]
terminal = "FAILED_VALIDATION" if status == "fail" else "PR_READY"
print(json.dumps({
    "terminal_state": terminal,
    "command": command or None,
    "command_status": status,
    "exit_code": int(exit_code),
    "target": target or "current workspace",
}, indent=2))
PY

if [ "$COMMAND_STATUS" = "fail" ]; then
    printf '{"status":"fail","failure_reason":"validation_failed"}\n' > "$STATUS"
    exit 1
fi

echo '{"status":"success"}' > "$STATUS"
exit 0
