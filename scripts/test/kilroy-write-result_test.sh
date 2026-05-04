#!/usr/bin/env bash
# Bash test for scripts/kilroy-write-result.sh.
#
# Verifies:
#   1. First call against a missing result.md synthesizes a BLOCKED stub
#      with the configured workflow header and the requested INPUT.md
#      section.
#   2. Second call (file already exists, has the synthesized marker) is
#      a no-op — the file is left exactly as written the first time.
#   3. Custom INPUT_KEY=question surfaces the right section and stops at
#      the next "## " header.
#   4. A pre-existing real result.md (no synthesized marker) is left
#      untouched — the helper never overwrites the agent's output.
#
# Run from anywhere:
#   bash scripts/test/kilroy-write-result_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPER="$SCRIPT_DIR/../kilroy-write-result.sh"
if [ ! -x "$HELPER" ]; then
    echo "FAIL: helper not executable at $HELPER" >&2
    exit 1
fi

PASS=0
FAIL=0

assert_contains() {
    local file="$1" needle="$2" desc="$3"
    if grep -qF -- "$needle" "$file"; then
        PASS=$((PASS + 1))
        echo "  ok: $desc"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    expected to find: $needle"
        echo "    in: $file"
        echo "    --- contents ---"
        sed 's/^/    /' "$file"
        echo "    --- end ---"
    fi
}

assert_not_contains() {
    local file="$1" needle="$2" desc="$3"
    if grep -qF -- "$needle" "$file"; then
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    did not expect: $needle"
        echo "    in: $file"
    else
        PASS=$((PASS + 1))
        echo "  ok: $desc"
    fi
}

assert_eq() {
    local got="$1" want="$2" desc="$3"
    if [ "$got" = "$want" ]; then
        PASS=$((PASS + 1))
        echo "  ok: $desc"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    got:  $got"
        echo "    want: $want"
    fi
}

setup_fixture() {
    local dir
    dir="$(mktemp -d -t kilroy-write-result-test.XXXXXX)"
    mkdir -p "$dir/.kilroy"
    cat > "$dir/.kilroy/INPUT.md" <<'EOF'
# Input

## prompt

implement the thing in foo.go

## question

why does bar.go return nil here

## context_files

internal/foo/foo.go
EOF
    echo "$dir"
}

# ----- Test 1: first call synthesizes a stub with prompt section -----
echo "test 1: first call synthesizes BLOCKED stub"
fixture="$(setup_fixture)"
(
    cd "$fixture"
    WORKFLOW_NAME=implement INPUT_KEY=prompt bash "$HELPER"
)
result="$fixture/result.md"
[ -f "$result" ] || { echo "  FAIL: result.md not created"; FAIL=$((FAIL + 1)); }
assert_contains "$result" "# implement workflow result (synthesized)" "header has workflow name + synthesized marker"
assert_contains "$result" "BLOCKED:" "BLOCKED line present"
assert_contains "$result" "## prompt" "prompt section header surfaced"
assert_contains "$result" "implement the thing in foo.go" "prompt body surfaced"
assert_not_contains "$result" "why does bar.go return nil" "question section NOT surfaced when INPUT_KEY=prompt"
assert_not_contains "$result" "internal/foo/foo.go" "context_files section NOT surfaced when INPUT_KEY=prompt"
rm -rf "$fixture"

# ----- Test 2: second call is a no-op when file already has the marker -----
echo "test 2: second call is a no-op (idempotent)"
fixture="$(setup_fixture)"
(
    cd "$fixture"
    WORKFLOW_NAME=implement INPUT_KEY=prompt bash "$HELPER"
)
checksum_before="$(shasum "$fixture/result.md" | awk '{print $1}')"
mtime_before="$(stat -f '%m' "$fixture/result.md" 2>/dev/null || stat -c '%Y' "$fixture/result.md")"
sleep 1
(
    cd "$fixture"
    WORKFLOW_NAME=implement INPUT_KEY=prompt bash "$HELPER"
)
checksum_after="$(shasum "$fixture/result.md" | awk '{print $1}')"
mtime_after="$(stat -f '%m' "$fixture/result.md" 2>/dev/null || stat -c '%Y' "$fixture/result.md")"
assert_eq "$checksum_after" "$checksum_before" "result.md content unchanged on second call"
assert_eq "$mtime_after" "$mtime_before" "result.md mtime unchanged on second call (file was not rewritten)"
rm -rf "$fixture"

# ----- Test 3: custom INPUT_KEY=question surfaces the right section -----
echo "test 3: INPUT_KEY=question surfaces the question section"
fixture="$(setup_fixture)"
(
    cd "$fixture"
    WORKFLOW_NAME=investigate INPUT_KEY=question bash "$HELPER"
)
result="$fixture/result.md"
assert_contains "$result" "# investigate workflow result (synthesized)" "header reflects WORKFLOW_NAME"
assert_contains "$result" "## question" "question section header surfaced"
assert_contains "$result" "why does bar.go return nil here" "question body surfaced"
assert_not_contains "$result" "implement the thing in foo.go" "prompt section NOT surfaced when INPUT_KEY=question"
assert_not_contains "$result" "internal/foo/foo.go" "section extraction stops at next ## header"
rm -rf "$fixture"

# ----- Test 4: pre-existing real result.md is left untouched -----
echo "test 4: pre-existing result.md without marker is left alone"
fixture="$(setup_fixture)"
echo "real agent output, do not overwrite" > "$fixture/result.md"
(
    cd "$fixture"
    WORKFLOW_NAME=implement INPUT_KEY=prompt bash "$HELPER"
)
contents="$(cat "$fixture/result.md")"
assert_eq "$contents" "real agent output, do not overwrite" "real result.md preserved verbatim"
rm -rf "$fixture"

# ----- Summary -----
echo ""
echo "passed: $PASS"
echo "failed: $FAIL"
if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
exit 0
