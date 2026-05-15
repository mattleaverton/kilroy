#!/usr/bin/env bash
# Stub any missing plan output artifacts so downstream tooling has stable files.
set -euo pipefail

STATUS_PATH="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

PLAN_STATUS="UNKNOWN"
if [ -s plan-status.json ]; then
    PLAN_STATUS=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('status','UNKNOWN'))" < plan-status.json 2>/dev/null || echo "UNKNOWN")
fi

if [ ! -s task-packet.md ]; then
    printf '# NOT READY: Task Packet Placeholder\n\nPlanner status: `%s`\n\nDo not run `implement` from this artifact.\nSee `plan-status.json` for the reason and any clarification questions.\n' "$PLAN_STATUS" > task-packet.md
fi

if [ ! -s testing-plan.md ]; then
    printf '# NOT READY: Testing Plan Placeholder\n\nPlanner status: `%s`\n\nNo concrete testing plan is available. See `plan-status.json`.\n' "$PLAN_STATUS" > testing-plan.md
fi

if [ ! -s validation-plan.md ]; then
    printf '# NOT READY: Validation Plan Placeholder\n\nPlanner status: `%s`\n\nNo concrete validation plan is available. See `plan-status.json`.\n' "$PLAN_STATUS" > validation-plan.md
fi

printf '{"status":"success"}\n' > "$STATUS_PATH" 2>/dev/null || true
