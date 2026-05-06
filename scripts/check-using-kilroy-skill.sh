#!/usr/bin/env bash
set -euo pipefail

SKILL="skills/using-kilroy/SKILL.md"

require_fixed() {
  local pattern="$1"
  if command -v rg >/dev/null 2>&1; then
    if rg -q --fixed-strings -- "$pattern" "$SKILL"; then
      return
    fi
  else
    if grep -q -F -- "$pattern" "$SKILL"; then
      return
    fi
  fi
  echo "using-kilroy skill missing required text: $pattern" >&2
  exit 1
}

require_fixed 'description: "'
require_fixed "kilroy list | describe <name> | check <name>"
require_fixed "kilroy run <workflow> [--input-file KEY=PATH"
require_fixed "--in-place"
require_fixed "kilroy runs show --latest --label task=<slug> --print result.md"

if grep -Eq "kilroy runs show <run-id> --pretty|kilroy run investigate --help|quick-launch workflow" "$SKILL"; then
  echo "using-kilroy skill contains stale command guidance" >&2
  exit 1
fi
