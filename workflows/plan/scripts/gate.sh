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

# Non-ready: write structured stubs so the clarified terminal has stable artifacts.
# The three planning files are stub seeds — useful as templates when the user supplies
# clarifications and re-runs plan.
if [ ! -s task-packet.md ]; then
    cat > task-packet.md << EOF
# Task Packet Seed (plan-status: $PLAN_STATUS)

See \`plan-status.json\` for clarification questions or blocking reason.
Do not pass this artifact to \`implement\` until status is READY_TO_IMPLEMENT.

## Intent
What should become true when this task is complete?

## Source
User request / bug / ticket / prior run.

## Scope
Exact repos, packages, files, or features in scope.

## Non-goals
What to leave alone.

## Starting Evidence
How to reproduce the bug or observe current behavior.

## Implementation Direction
The approach to take.

## No-op / Escalation Rules
When no code change is the right answer. When a human must approve.

## Budget / Risk
Allowed network, external API calls, destructive actions.
EOF
fi

if [ ! -s testing-plan.md ]; then
    printf '# Testing Plan Seed (plan-status: %s)\n\nSee `plan-status.json`. Not ready for use.\n' "$PLAN_STATUS" > testing-plan.md
fi

if [ ! -s validation-plan.md ]; then
    printf '# Validation Plan Seed (plan-status: %s)\n\nSee `plan-status.json`. Not ready for use.\n' "$PLAN_STATUS" > validation-plan.md
fi

# Non-ready: exit 1 to route to the clarification terminal.
printf '{"status":"fail","failure_reason":"not_ready_to_implement","plan_status":"%s"}\n' "$PLAN_STATUS" > "$STATUS"
exit 1
