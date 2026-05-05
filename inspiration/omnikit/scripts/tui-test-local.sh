#!/usr/bin/env bash
# Local macOS TUI test runner (no Docker required)
# Runs smoke test and VHS tapes. Pixel tests require Docker.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo -e "${CYAN}[tui-test-local]${RESET} Building KitchenSink..."
swift build --product KitchenSink 2>&1 | tail -3
BIN="$(swift build --show-bin-path)/KitchenSink"

echo ""
echo -e "${CYAN}=== Smoke test ===${RESET}"
if OMNIUI_SMOKE_SECONDS=3 OMNIUI_DEMO_ANIM=0 timeout 15 "$BIN" --notcurses 2>/dev/null; then
    echo -e "  ${GREEN}PASS${RESET}: smoke test (exited cleanly)"
else
    echo -e "  ${RED}FAIL${RESET}: smoke test (rc=$?)"
fi

echo ""
echo -e "${CYAN}=== VHS tests ===${RESET}"
if command -v vhs &>/dev/null; then
    for tape in Tests/tui/tapes/*.tape; do
        [ -f "$tape" ] || continue
        name="$(basename "$tape" .tape)"
        echo -e "  Running tape: ${name}"
        VHS_NO_SANDBOX=1 vhs "$tape" 2>&1 | tail -3
    done
else
    echo -e "  ${YELLOW}SKIP${RESET}: vhs not installed (brew install vhs)"
fi

echo ""
echo -e "${CYAN}=== Kitty pixel tests ===${RESET}"
echo -e "  ${YELLOW}SKIP${RESET}: Run via Docker for pixel-accurate tests:"
echo "    docker compose run tui-test"
echo "    TUI_TEST_MODE=kitty docker compose run tui-test"
