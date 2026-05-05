#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# E2E suite: deterministic integration tests + DOT validation contract.

echo "== KILROY_INTEGRATION=1 go test ./... =="
KILROY_INTEGRATION=1 go test ./...

echo

echo "== validate contract dotfiles =="
DOTS=(
  "docs/strongdm/dot specs/consensus_task.dot"
  "docs/strongdm/dot specs/semport.dot"
  "docs/strongdm/dot specs/simple_example.dot"
  "research/green-test-vague.dot"
  "research/green-test-moderate.dot"
  "research/green-test-complex.dot"
  "research/refactor-test-vague.dot"
  "research/refactor-test-moderate.dot"
  "research/refactor-test-complex.dot"
)

# Build the local binary once for validation.
go build -o ./kilroy ./cmd/kilroy

fail=0
for f in "${DOTS[@]}"; do
  echo "-- kilroy attractor validate --graph '$f'"
  if ./kilroy attractor validate --graph "$f"; then
    :
  else
    echo "VALIDATE FAIL: $f" >&2
    fail=1
  fi
  echo
  
  # Avoid huge output and keep the loop responsive.
  sleep 0.1
done

if [[ $fail -ne 0 ]]; then
  echo "One or more dotfiles failed validation" >&2
  exit 1
fi

echo "All validations passed"

# Optional: bounded fuzz runs for the two primary input surfaces.
# These are not run by default (they require -fuzz flag); the seed corpus
# is verified as part of the integration-enabled `go test ./...` above via -run mode.
#
# To run extended fuzzing locally:
#   go test -fuzz=FuzzParse       -fuzztime=30s ./internal/attractor/dot/
#   go test -fuzz=FuzzConditionEval -fuzztime=30s ./internal/attractor/cond/

# Optional quality regression check (requires kilroy binary and Claude API)
# RUN_INGEST_QUALITY=1 ./scripts/test-ingest-quality.sh
