#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run() {
  echo
  echo "==> $*"
  "$@"
}

run ./scripts/verify_parity_sources.sh
run swift test --filter LLMKitBackendParityTests

if [[ "${RUN_OMNIAI_LIVE_PARITY_GATES:-0}" != "1" ]]; then
  echo
  echo "Skipping live parity gates (set RUN_OMNIAI_LIVE_PARITY_GATES=1 to enable)."
  exit 0
fi

: "${OPENAI_API_KEY:?OPENAI_API_KEY is required for live parity gates}"
: "${ANTHROPIC_API_KEY:?ANTHROPIC_API_KEY is required for live parity gates}"
: "${GEMINI_API_KEY:?GEMINI_API_KEY is required for live parity gates}"

export RUN_OMNIAI_INTEGRATION_TESTS=1
export OMNIAI_INTEGRATION_PROVIDERS="openai,anthropic,gemini"
# Use stable defaults for parity gates unless the caller overrides.
: "${OPENAI_INTEGRATION_MODEL:=gpt-5.2}"
: "${ANTHROPIC_INTEGRATION_MODEL:=claude-haiku-4-5}"
: "${GEMINI_INTEGRATION_MODEL:=gemini-3-flash-preview}"
export OPENAI_INTEGRATION_MODEL ANTHROPIC_INTEGRATION_MODEL GEMINI_INTEGRATION_MODEL

run swift test --filter testCrossProviderParityMatrixOpenAIAnthropicGemini

export RUN_OMNIAI_CACHE_INTEGRATION_TESTS=1
run swift test --filter testMultiTurnCacheReadAcrossOpenAIAnthropicGemini

export RUN_ANTHROPIC_LIVE_PARITY_TESTS=1
run swift test --filter testAnthropicClaudeLiveParityMatrix
