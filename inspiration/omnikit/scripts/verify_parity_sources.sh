#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK_FILE="${1:-$ROOT_DIR/parity_sources.lock.json}"

if [[ ! -f "$LOCK_FILE" ]]; then
  echo "Lock file not found: $LOCK_FILE" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to verify parity sources." >&2
  exit 1
fi

sha256_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return 0
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return 0
  fi
  echo "No SHA-256 utility found (need shasum or sha256sum)." >&2
  exit 1
}

sha256_stdin() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
    return 0
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
    return 0
  fi
  echo "No SHA-256 utility found (need shasum or sha256sum)." >&2
  exit 1
}

failures=0

echo "== Verifying pinned repositories =="
while IFS= read -r key; do
  repo_url="$(jq -r ".repositories[\"$key\"].url" "$LOCK_FILE")"
  expected_commit="$(jq -r ".repositories[\"$key\"].commit" "$LOCK_FILE")"
  local_clone_rel="$(jq -r ".repositories[\"$key\"].local_clone // \"\"" "$LOCK_FILE")"
  if [[ -z "$local_clone_rel" ]]; then
    echo "[WARN] $key has no local_clone configured; skipping local commit verification"
    continue
  fi

  local_clone="$ROOT_DIR/$local_clone_rel"
  if [[ ! -d "$local_clone/.git" ]]; then
    echo "[WARN] $key clone not found at $local_clone (expected $repo_url @ $expected_commit)"
    continue
  fi

  actual_commit="$(git -C "$local_clone" rev-parse HEAD)"
  if [[ "$actual_commit" != "$expected_commit" ]]; then
    echo "[FAIL] $key commit mismatch: expected $expected_commit got $actual_commit"
    failures=$((failures + 1))
  else
    echo "[OK]   $key commit pinned at $actual_commit"
  fi
done < <(jq -r '.repositories | keys[]' "$LOCK_FILE")

echo
echo "== Verifying byte-for-byte parity files =="
count="$(jq '.byte_parity_files | length' "$LOCK_FILE")"
for ((i=0; i<count; i++)); do
  source_key="$(jq -r ".byte_parity_files[$i].source" "$LOCK_FILE")"
  local_rel="$(jq -r ".byte_parity_files[$i].local" "$LOCK_FILE")"
  upstream_rel="$(jq -r ".byte_parity_files[$i].upstream" "$LOCK_FILE")"
  expected_sha="$(jq -r ".byte_parity_files[$i].sha256" "$LOCK_FILE")"
  local_path="$ROOT_DIR/$local_rel"

  if [[ ! -f "$local_path" ]]; then
    echo "[FAIL] Missing local parity file: $local_rel"
    failures=$((failures + 1))
    continue
  fi

  local_sha="$(sha256_file "$local_path")"
  if [[ "$local_sha" != "$expected_sha" ]]; then
    echo "[FAIL] SHA mismatch for $local_rel: expected $expected_sha got $local_sha"
    failures=$((failures + 1))
  else
    echo "[OK]   $local_rel hash matches lock"
  fi

  repo_clone_rel="$(jq -r ".repositories[\"$source_key\"].local_clone // \"\"" "$LOCK_FILE")"
  if [[ -n "$repo_clone_rel" ]]; then
    repo_clone="$ROOT_DIR/$repo_clone_rel"
    upstream_path="$repo_clone/$upstream_rel"
    if [[ -f "$upstream_path" ]]; then
      upstream_sha="$(sha256_file "$upstream_path")"
      if [[ "$upstream_sha" != "$local_sha" ]]; then
        echo "[FAIL] Upstream mismatch for $local_rel against $source_key:$upstream_rel"
        failures=$((failures + 1))
      else
        echo "[OK]   Upstream match for $local_rel"
      fi
    else
      echo "[WARN] Upstream path missing: $upstream_path"
    fi
  fi
done

echo
echo "== Verifying Claude reference pin =="
claude_version="$(jq -r '.claude_reference.version // ""' "$LOCK_FILE")"
claude_file="$(jq -r '.claude_reference.file // ""' "$LOCK_FILE")"
claude_sha_expected="$(jq -r '.claude_reference.sha256 // ""' "$LOCK_FILE")"
claude_url="$(jq -r '.claude_reference.url // ""' "$LOCK_FILE")"

if [[ -n "$claude_version" && -n "$claude_file" && -n "$claude_sha_expected" && -n "$claude_url" ]]; then
  if command -v curl >/dev/null 2>&1; then
    gist_id="${claude_url##*/}"
    raw_url="https://gist.githubusercontent.com/navanchauhan/${gist_id}/raw/${claude_version}/${claude_file}"
    tmp_file="$(mktemp)"
    if curl -fsSL "$raw_url" -o "$tmp_file"; then
      claude_sha_actual="$(sha256_file "$tmp_file")"
      if [[ "$claude_sha_actual" != "$claude_sha_expected" ]]; then
        echo "[FAIL] Claude gist hash mismatch: expected $claude_sha_expected got $claude_sha_actual"
        failures=$((failures + 1))
      else
        echo "[OK]   Claude gist pin verified ($claude_version)"
      fi

      api_json="$(mktemp)"
      if curl -fsSL "https://api.github.com/gists/${gist_id}" -o "$api_json"; then
        latest_version="$(jq -r '.history[0].version // ""' "$api_json")"
        if [[ -n "$latest_version" && "$latest_version" != "$claude_version" ]]; then
          echo "[WARN] Newer Claude gist revision exists: $latest_version (pinned: $claude_version)"
        fi
      fi
      rm -f "$api_json"
    else
      echo "[FAIL] Could not fetch pinned Claude gist raw file: $raw_url"
      failures=$((failures + 1))
    fi
    rm -f "$tmp_file"
  else
    echo "[WARN] curl not found; skipping Claude gist verification"
  fi
else
  echo "[WARN] Claude reference pin is incomplete; skipping"
fi

echo
if ((failures > 0)); then
  echo "Parity source verification failed with $failures issue(s)." >&2
  exit 1
fi

echo "Parity source verification passed."
