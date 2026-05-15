#!/usr/bin/env bash
# Parse the classifier's JSON response into plan-status.json and print a routing
# token: "ready" (proceed to planning) or "clarify" (goal not yet actionable).
#
# This is a tool node: the engine derives the stage outcome from the exit code
# (0 = success, non-zero = fail) and exposes the script's stdout+stderr as
# context.tool.output. Routing between "ready" and "clarify" happens on
# context.tool.output in graph.dot; a genuine error exits non-zero so the
# engine routes to the fail terminal. Both normal paths exit 0 — no retries.
set -uo pipefail

LOGS_ROOT="${KILROY_LOGS_ROOT:-}"

# Genuine error: message to stderr, non-zero exit. The engine records the stage
# as failed and graph.dot routes outcome=fail to the failed terminal.
fail() {
    echo "gate: $1" >&2
    exit 1
}

[ -n "$LOGS_ROOT" ] || fail "KILROY_LOGS_ROOT not set"

RESPONSE_FILE="$LOGS_ROOT/classifier/response.md"
[ -f "$RESPONSE_FILE" ] || fail "classifier response missing: $RESPONSE_FILE"

# Extract JSON from the classifier response (handles preamble text and code
# fences) and write plan-status.json. Echoes the plan status on stdout.
PLAN_STATUS=$(python3 - "$RESPONSE_FILE" <<'PY'
import json, re, sys

text = open(sys.argv[1]).read()

# Try a ```json ... ``` fence first
fence = re.search(r'```(?:json)?\s*\n(.*?)\n```', text, re.DOTALL)
candidate = fence.group(1).strip() if fence else None

# Fall back to everything from the first { onward
if not candidate:
    idx = text.find('{')
    if idx == -1:
        print("no JSON object found in classifier response", file=sys.stderr)
        sys.exit(1)
    candidate = text[idx:]

try:
    data = json.loads(candidate)
except json.JSONDecodeError as e:
    print(f"classifier response is not valid JSON: {e}", file=sys.stderr)
    sys.exit(1)

with open("plan-status.json", "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print(data.get("status", "UNKNOWN"))
PY
) || fail "could not parse classifier response into plan-status.json"

if [ "$PLAN_STATUS" = "READY_TO_IMPLEMENT" ]; then
    # Proceed to the planning fan-out. printf with no newline so
    # context.tool.output is exactly "ready" for edge routing.
    printf 'ready'
    exit 0
fi

# Not ready (NEEDS_CLARIFICATION / NEEDS_DECOMPOSITION / ESCALATE_RISK): write
# minimal stubs so the clarified terminal has stable output files, then print
# the "clarify" routing token. Exit 0 — this is a clean, non-fail result; the
# run succeeded, the goal just is not actionable yet.
[ -s task-packet.md ]     || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > task-packet.md
[ -s testing-plan.md ]    || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > testing-plan.md
[ -s validation-plan.md ] || printf '# NOT READY (plan-status: %s)\n\nSee plan-status.json.\n' "$PLAN_STATUS" > validation-plan.md

printf 'clarify'
exit 0
