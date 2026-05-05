#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/swiftui_non_renderer_parity.py --check --swiftui-sdk required --write-markdown docs/swiftui-non-renderer-parity.md
python3 scripts/swiftui_symbol_diff.py --check --swiftui-sdk required --write-json docs/swiftui-non-renderer-symbol-diff.json --write-markdown docs/swiftui-non-renderer-symbol-diff.md
swift build --target SwiftUICompatibilityHarness
swift build --target OmniUINotcursesRenderer --target KitchenSink
swift test --filter OmniUICoreTests
TERM=xterm-256color OMNIUI_SMOKE_SECONDS=1 OMNIUI_DEMO_ANIM=0 "$(swift build --show-bin-path)/KitchenSink" --notcurses >/dev/null

git diff --exit-code -- docs/swiftui-non-renderer-parity.md docs/swiftui-non-renderer-symbol-diff.json docs/swiftui-non-renderer-symbol-diff.md
