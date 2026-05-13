#!/usr/bin/env bash
# Bash regression test for scripts/install.sh.
#
# Verifies that the local installer:
#   1. installs an executable kilroy binary;
#   2. fully refreshes the global workflow package directory;
#   3. preserves auth/policy files outside the workflow directory;
#   4. installs first-party Kilroy skills for Claude, Codex, and opencode;
#   5. removes stale global quick-launch / pr-review skills;
#   6. keeps the local install surface to the canonical installer.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL="$REPO/scripts/install.sh"

PASS=0
FAIL=0

ok() {
    PASS=$((PASS + 1))
    printf '  ok: %s\n' "$1"
}

fail() {
    FAIL=$((FAIL + 1))
    printf '  FAIL: %s\n' "$1"
}

assert_file() {
    local path="$1" desc="$2"
    if [ -f "$path" ]; then
        ok "$desc"
    else
        fail "$desc (missing $path)"
    fi
}

assert_dir() {
    local path="$1" desc="$2"
    if [ -d "$path" ]; then
        ok "$desc"
    else
        fail "$desc (missing $path)"
    fi
}

assert_not_exists() {
    local path="$1" desc="$2"
    if [ ! -e "$path" ] && [ ! -L "$path" ]; then
        ok "$desc"
    else
        fail "$desc (unexpected $path)"
    fi
}

assert_contains() {
    local file="$1" needle="$2" desc="$3"
    if grep -qF -- "$needle" "$file"; then
        ok "$desc"
    else
        fail "$desc (missing $needle in $file)"
    fi
}

tmp_home="$(mktemp -d -t kilroy-install-test-home.XXXXXX)"
tmp_repo="$(mktemp -d -t kilroy-install-test-repo.XXXXXX)"
trap 'chmod -R u+w "$tmp_home" "$tmp_repo" 2>/dev/null || true; rm -rf "$tmp_home" "$tmp_repo"' EXIT

assert_not_exists "$REPO/scripts/install-skills.sh" "old install-skills wrapper removed"
assert_not_exists "$REPO/scripts/check-using-kilroy-skill.sh" "old using-kilroy check helper removed"

export HOME="$tmp_home"
export XDG_CONFIG_HOME="$tmp_home/.config"
export XDG_DATA_HOME="$tmp_home/.local/share"
export XDG_STATE_HOME="$tmp_home/.local/state"
export GOCACHE="${GOCACHE:-$(go env GOCACHE)}"
export GOMODCACHE="${GOMODCACHE:-$(go env GOMODCACHE)}"

workflow_dir="$XDG_DATA_HOME/kilroy/workflows"
config_dir="$XDG_CONFIG_HOME/kilroy"

mkdir -p "$HOME/.local/bin" "$workflow_dir/stale-workflow" "$workflow_dir/workflows/quick-launch" "$config_dir"
stale_bin="$(mktemp -d -t kilroy-install-test-stale-bin.XXXXXX)/kilroy"
printf '#!/usr/bin/env bash\nexit 42\n' > "$stale_bin"
chmod +x "$stale_bin"
ln -s "$stale_bin" "$HOME/.local/bin/kilroy"

stale_config_workflows="$(mktemp -d -t kilroy-install-test-config-workflows.XXXXXX)"
mkdir -p "$stale_config_workflows/implement"
printf 'stale config workflow\n' > "$stale_config_workflows/implement/workflow.toml"
ln -s "$stale_config_workflows" "$config_dir/workflows"

printf 'stale = true\n' > "$workflow_dir/stale-workflow/workflow.toml"
printf 'old quick launch\n' > "$workflow_dir/workflows/quick-launch/workflow.toml"
printf 'auth-preserved\n' > "$config_dir/auth.toml"
printf 'policy-preserved\n' > "$config_dir/policy-overrides.toml"
printf 'data-preserved\n' > "$XDG_DATA_HOME/kilroy/keep.txt"

for root in "$HOME/.claude/skills" "$HOME/.agents/skills" "$HOME/.config/opencode/skills"; do
    mkdir -p "$root/quick-launch" "$root/pr-review"
    printf 'stale\n' > "$root/quick-launch/SKILL.md"
    printf 'stale\n' > "$root/pr-review/SKILL.md"
done

echo "running installer in temp HOME"
PATH="$HOME/.local/bin:$PATH" bash "$INSTALL" >/tmp/kilroy-install-test.out 2>/tmp/kilroy-install-test.err || {
    cat /tmp/kilroy-install-test.out
    cat /tmp/kilroy-install-test.err >&2
    exit 1
}

assert_file "$HOME/.local/bin/kilroy" "kilroy binary installed"
if [ -x "$HOME/.local/bin/kilroy" ]; then
    ok "kilroy binary is executable"
else
    fail "kilroy binary is executable"
fi
if [ -L "$HOME/.local/bin/kilroy" ]; then
    fail "kilroy binary replaces stale symlink"
else
    ok "kilroy binary replaces stale symlink"
fi

assert_not_exists "$workflow_dir/stale-workflow" "stale workflow removed"
assert_not_exists "$workflow_dir/workflows" "nested stale workflow root removed"
assert_not_exists "$config_dir/workflows" "stale config workflow symlink removed"

while IFS= read -r wf; do
    name="$(basename "$wf")"
    assert_file "$workflow_dir/$name/workflow.toml" "workflow installed: $name"
done < <(find "$REPO/workflows" -mindepth 1 -maxdepth 1 -type d | sort)

assert_contains "$config_dir/auth.toml" "auth-preserved" "auth config preserved"
assert_contains "$config_dir/policy-overrides.toml" "policy-preserved" "policy overrides preserved"
assert_contains "$XDG_DATA_HOME/kilroy/keep.txt" "data-preserved" "non-workflow data preserved"

while IFS= read -r skill; do
    name="$(basename "$(dirname "$skill")")"
    assert_dir "$HOME/.claude/skills/$name" "Claude skill installed: $name"
    assert_dir "$HOME/.agents/skills/$name" "Codex skill installed: $name"
    assert_dir "$HOME/.config/opencode/skills/$name" "opencode skill installed: $name"
done < <(find "$REPO/skills" -mindepth 2 -maxdepth 2 -name SKILL.md | sort)

for stale in quick-launch pr-review; do
    assert_not_exists "$HOME/.claude/skills/$stale" "stale Claude skill removed: $stale"
    assert_not_exists "$HOME/.agents/skills/$stale" "stale Codex skill removed: $stale"
    assert_not_exists "$HOME/.config/opencode/skills/$stale" "stale opencode skill removed: $stale"
done

(
    cd "$tmp_repo"
    git init -q
    "$HOME/.local/bin/kilroy" list --all --pretty > "$tmp_home/list.out"
    "$HOME/.local/bin/kilroy" describe implement --pretty > "$tmp_home/describe.out"
)
assert_contains "$tmp_home/list.out" "implement" "installed binary discovers workflows from another repo"
assert_contains "$tmp_home/describe.out" "source:      $workflow_dir" "discovery uses refreshed workflow dir"

echo ""
echo "passed: $PASS"
echo "failed: $FAIL"
if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
