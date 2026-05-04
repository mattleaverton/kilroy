#!/usr/bin/env bash
# When the agent stage didn't write result.md, synthesize one that's
# actually useful for triage: include the agent's response (what it
# actually said) and the git diff (what it actually changed). A
# "BLOCKED" stub without these is unactionable noise; this gives
# enough context to decide whether the worker did the work and just
# forgot result.md, or did nothing.
set -uo pipefail
STATUS="${KILROY_STAGE_STATUS_PATH:-/dev/null}"
synthesized=0

# Re-entrancy: if an earlier attempt already synthesized result.md, the
# "(synthesized)" marker on line 1 lets us preserve the fail outcome on
# retry instead of silently succeeding because the file now exists.
if [ -f result.md ] && head -n 1 result.md 2>/dev/null | grep -q '(synthesized)'; then
    synthesized=1
fi

if [ ! -f result.md ]; then
    synthesized=1
    {
        echo "# implement workflow result (synthesized)"
        echo
        echo "BLOCKED: agent did not write result.md."
        echo
        echo "The synthesized sections below let you triage without spelunking"
        echo "the run's logs_root: the agent's own response, the git diff vs"
        echo "launch HEAD, and verify-stage output if any."
        echo

        # Agent's response (from agent_output.jsonl via the codec). This
        # is in the run's logs_root, not the worktree, so try a few
        # locations. KILROY_STAGE_DIR points at the previous stage's
        # logs dir; the agent stage dir is its sibling.
        agent_response=""
        if [ -n "${KILROY_LOGS_ROOT:-}" ] && [ -f "$KILROY_LOGS_ROOT/agent/response.md" ]; then
            agent_response="$KILROY_LOGS_ROOT/agent/response.md"
        elif [ -n "${KILROY_RUN_ID:-}" ] && [ -d "$HOME/.local/state/kilroy/attractor/runs/$KILROY_RUN_ID/agent" ]; then
            agent_response="$HOME/.local/state/kilroy/attractor/runs/$KILROY_RUN_ID/agent/response.md"
        fi
        if [ -n "$agent_response" ] && [ -f "$agent_response" ]; then
            echo "## agent response"
            echo
            echo '```'
            head -c 8192 "$agent_response"
            echo
            echo '```'
            echo
        fi

        # Git diff vs the launch HEAD. Even if the agent didn't write
        # result.md, the commits ARE preserved on the run branch — and
        # often that's the actual deliverable.
        if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
            base_sha=""
            if [ -n "${KILROY_BASE_SHA:-}" ]; then
                base_sha="$KILROY_BASE_SHA"
            elif [ -f .kilroy/BASE_SHA ]; then
                base_sha=$(cat .kilroy/BASE_SHA 2>/dev/null)
            fi
            if [ -n "$base_sha" ] && git rev-parse --verify "$base_sha" >/dev/null 2>&1; then
                echo "## git diff (since launch HEAD: $base_sha)"
                echo
                echo '```diff'
                git diff "$base_sha"...HEAD --stat 2>/dev/null
                echo
                git diff "$base_sha"...HEAD 2>/dev/null | head -c 16384
                echo '```'
                echo
            else
                echo "## git diff (since first commit on this branch)"
                echo
                echo '```diff'
                git log --oneline -5 2>/dev/null
                echo '```'
                echo
            fi
        fi

        # Verify stage output for build/test status.
        if [ -f .kilroy/verify.log ]; then
            echo "## verify.log (tail)"
            echo '```'
            tail -n 60 .kilroy/verify.log
            echo '```'
        fi
    } > result.md
fi
if [ "$synthesized" -eq 1 ]; then
    printf '{"status":"fail","failure_reason":"agent_did_not_write_result"}\n' > "$STATUS"
    exit 1
fi
echo '{"status":"success"}' > "$STATUS"
