#!/usr/bin/env bash
# Ensure the public plan workflow always returns its declared artifact set.
# Agent models sometimes stop after plan-status.json when asking clarification;
# downstream tooling still needs stable files to inspect and copy.
set -euo pipefail

STATUS_PATH="${KILROY_STAGE_STATUS_PATH:-/dev/null}"

if [ ! -s plan-status.json ]; then
    cat > plan-status.json <<'JSON'
{
  "status": "NEEDS_CLARIFICATION",
  "classification": "unknown",
  "questions": [
    {
      "question": "What concrete outcome should Kilroy produce?",
      "recommended_default": "Proceed with the smallest safe implementation that satisfies the request.",
      "why_it_matters": "The planner did not produce a structured status, so the next step needs a human-confirmed seed."
    }
  ],
  "assumptions": [],
  "risk_flags": [],
  "session_notes": "Planner omitted plan-status.json; ensure-outputs synthesized a safe clarification status."
}
JSON
fi

extract_status() {
    sed -n 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' plan-status.json | head -n 1
}

PLAN_STATUS="$(extract_status)"
[ -n "$PLAN_STATUS" ] || PLAN_STATUS="UNKNOWN"

if [ ! -s task-packet.md ]; then
    cat > task-packet.md <<EOF
# Task Packet

Planner status: \`$PLAN_STATUS\`

## Intent
See \`plan-status.json\`. The planner did not produce a full task packet.

## Source
The user's raw goal from this plan run.

## Scope
Unconfirmed.

## Non-goals
Do not start implementation until clarification, decomposition, or risk approval
is resolved.

## Starting Evidence
See \`plan-status.json\`.

## Implementation Direction
Blocked pending the planner status.

## No-op / Escalation Rules
If \`plan-status.json\` is not \`READY_TO_IMPLEMENT\`, ask the user or perform the
requested clarification/decomposition step before running \`implement\`.

## Budget / Risk
Unconfirmed.
EOF
fi

if [ ! -s testing-plan.md ]; then
    cat > testing-plan.md <<EOF
# Testing Plan

Planner status: \`$PLAN_STATUS\`

No concrete testing plan is available yet. Do not run \`implement\` until the
task is clarified enough to define unit, integration, build, or scenario checks.
EOF
fi

if [ ! -s validation-plan.md ]; then
    cat > validation-plan.md <<EOF
# Validation Plan

Planner status: \`$PLAN_STATUS\`

No concrete validation plan is available yet. Before implementation, define the
external behavior or evidence that would convince a skeptical human the task is
done.
EOF
fi

printf '{"status":"success"}\n' > "$STATUS_PATH" 2>/dev/null || true
