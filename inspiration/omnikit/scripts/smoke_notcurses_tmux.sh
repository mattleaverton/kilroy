#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SESSION="${1:-omni_nc_smoke}"
OUTDIR="${OUTDIR:-/tmp/omni_nc_smoke}"
mkdir -p "$OUTDIR"
STATUS_FILE="$OUTDIR/status.txt"
PANE_FILE="$OUTDIR/pane.txt"
ERR_FILE="$OUTDIR/stderr.txt"
LOG_FILE="$OUTDIR/cmd.log"

rm -f "$STATUS_FILE" "$PANE_FILE" "$ERR_FILE" "$LOG_FILE"

tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true

# Run in tmux, capture stderr to a file, and keep the shell alive long enough
# for capture-pane to work even after the app exits.
tmux new-session -d -x 120 -y 40 -s "$SESSION" -c "$(pwd)" "bash -lc 'echo \"pwd=$(pwd)\" >\"$LOG_FILE\"; echo start >>\"$LOG_FILE\"; OMNIUI_SMOKE_SECONDS=3 OMNIUI_DEMO_ANIM=0 swift run KitchenSink -- --notcurses 2>\"$ERR_FILE\"; rc=\\\$?; echo \"rc=\\\$rc\" >>\"$LOG_FILE\"; echo \\\$rc >\"$STATUS_FILE\"; sleep 0.5'"

# Wait for status (up to ~30s including first-time build).
for _ in {1..20}; do
  if [[ -f "$STATUS_FILE" ]]; then
    break
  fi
  sleep 0.5
done

if [[ ! -f "$STATUS_FILE" ]]; then
  # Fall back to SIGINT.
  tmux send-keys -t "$SESSION" C-c
  for _ in {1..20}; do
    if [[ -f "$STATUS_FILE" ]]; then
      break
    fi
    sleep 0.5
  done
fi

tmux capture-pane -t "$SESSION" -p -S -400 >"$PANE_FILE" 2>/dev/null || true

tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true

echo "status_file=$STATUS_FILE"
echo "pane_file=$PANE_FILE"
echo "stderr_file=$ERR_FILE"
