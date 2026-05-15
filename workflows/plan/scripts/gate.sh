#!/usr/bin/env bash
# Read the classifier's JSON response, write plan-status.json, exit 0 only if READY_TO_IMPLEMENT.
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
LOGS_ROOT="${KILROY_LOGS_ROOT:-}"

if [ -z "$LOGS_ROOT" ]; then
    printf '{"status":"fail","failure_reason":"KILROY_LOGS_ROOT not set"}\n' > "$STATUS"
    exit 1
fi

RESPONSE_FILE="$LOGS_ROOT/classifier/response.md"
if [ ! -f "$RESPONSE_FILE" ]; then
    printf '{"status":"fail","failure_reason":"classifier_response_missing"}\n' > "$STATUS"
    exit 1
fi

# Strip optional markdown code fences, parse JSON, write plan-status.json.
PLAN_STATUS=$(python3 - "$RESPONSE_FILE" <<'PY'
import json, re, sys

text = open(sys.argv[1]).read().strip()
# Strip optional ```json or ``` fences
text = re.sub(r'^```(?:json)?\s*\n?', '', text)
text = re.sub(r'\n?```\s*$', '', text).strip()

try:
    data = json.loads(text)
except json.JSONDecodeError as e:
    print(f"gate: classifier response is not valid JSON: {e}", file=sys.stderr)
    print(f"content: {text[:300]}", file=sys.stderr)
    sys.exit(1)

with open("plan-status.json", "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print(data.get("status", "UNKNOWN"))
PY
) || {
    printf '{"status":"fail","failure_reason":"classifier_json_parse_error"}\n' > "$STATUS"
    exit 1
}

if [ "$PLAN_STATUS" = "READY_TO_IMPLEMENT" ]; then
    echo '{"status":"success"}' > "$STATUS"
    exit 0
fi

# Non-ready: write minimal stubs so the clarified terminal has stable output files.
[ -s task-packet.md ]      || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > task-packet.md
[ -s testing-plan.md ]     || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > testing-plan.md
[ -s validation-plan.md ]  || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > validation-plan.md

# Non-ready: exit 1 to route to the clarification terminal.
printf '{"status":"fail","failure_reason":"not_ready_to_implement","plan_status":"%s"}\n' "$PLAN_STATUS" > "$STATUS"
exit 1
