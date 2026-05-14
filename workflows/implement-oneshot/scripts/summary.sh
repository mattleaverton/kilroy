#!/usr/bin/env bash
# Summary stage. Delegates BLOCKED-stub synthesis to the shared helper
# kilroy-write-result.sh (a sibling installed via symlink in this workflow's
# scripts/ directory). The implement workflow augments the synthesized stub
# with the agent's response transcript when available — that's specific to
# this workflow because the agent's commentary is often the only signal
# explaining why result.md is missing.
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKFLOW_NAME=implement \
INPUT_KEY=prompt \
INCLUDE_DIFF=true \
INCLUDE_VERIFY_LOG=true \
    bash "$SCRIPT_DIR/kilroy-write-result.sh"

# If the helper synthesized result.md, append the agent's response (if we
# can find it in the run's logs_root) and report stage failure so the F2
# contract still routes to the `failed` terminal.
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    agent_response=""
    if [ -n "${KILROY_LOGS_ROOT:-}" ] && [ -f "$KILROY_LOGS_ROOT/agent/response.md" ]; then
        agent_response="$KILROY_LOGS_ROOT/agent/response.md"
    elif [ -n "${KILROY_RUN_ID:-}" ] && [ -d "$HOME/.local/state/kilroy/attractor/runs/$KILROY_RUN_ID/agent" ]; then
        agent_response="$HOME/.local/state/kilroy/attractor/runs/$KILROY_RUN_ID/agent/response.md"
    fi
    if [ -n "$agent_response" ] && [ -f "$agent_response" ]; then
        {
            echo
            echo "## agent response"
            echo
            echo '```'
            head -c 8192 "$agent_response"
            echo
            echo '```'
        } >> result.md
    fi
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi

echo '{"status":"success"}' > "$STATUS"
