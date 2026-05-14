#!/usr/bin/env bash
# Install kilroy for local development.
#
# This is intentionally a local checkout installer, not a release packager:
# it builds the binary from this repo, refreshes the global workflow package
# directory from the current checkout, and links first-party Kilroy skills
# into the user-level agent skill directories.
#
# Re-run after git pull to update the binary, built-in workflows, and skills.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${HOME}/.local/bin"
DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/kilroy"
WORKFLOW_DIR="${DATA_DIR}/workflows"
CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/kilroy"
CONFIG_WORKFLOW_DIR="${CONFIG_DIR}/workflows"

say() { printf '  %s\n' "$1"; }

is_shipped_workflow() {
    # Keep dev-only harnesses in the repo without making them global defaults
    # for downstream project repos.
    case "$(basename "$1")" in
        multi-tool-exercise)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

replace_link() {
    local target="$1" linkname="$2"
    mkdir -p "$(dirname "$linkname")"
    rm -rf "$linkname"
    ln -s "$target" "$linkname"
    say "$linkname -> $target"
}

refresh_workflows() {
    case "$WORKFLOW_DIR" in
        ""|"/"|"/workflows")
            echo "error: refusing to refresh unsafe workflow dir: ${WORKFLOW_DIR:-<empty>}" >&2
            exit 1
            ;;
    esac

    # Older local installs sometimes put built-in workflows under the XDG
    # config layer, which has higher precedence than the XDG data layer. Remove
    # only symlinks here so auth/policy files and real user-authored workflow
    # directories remain untouched.
    if [ -L "$CONFIG_WORKFLOW_DIR" ]; then
        rm -f "$CONFIG_WORKFLOW_DIR"
    fi

    rm -rf "$WORKFLOW_DIR"
    mkdir -p "$WORKFLOW_DIR"

    local workflow name
    while IFS= read -r workflow; do
        if ! is_shipped_workflow "$workflow"; then
            continue
        fi
        name="$(basename "$workflow")"
        mkdir -p "$WORKFLOW_DIR/$name"
        cp -R "$workflow/." "$WORKFLOW_DIR/$name/"
    done < <(find "$REPO/workflows" -mindepth 1 -maxdepth 1 -type d | sort)
}

install_skills_for_root() {
    local root="$1"
    mkdir -p "$root"

    # These were old global skills with stale kilroy attractor-era commands.
    # They may still exist as project-local skills elsewhere; only global
    # user-level entries are removed here.
    rm -rf "$root/quick-launch" "$root/pr-review"

    local skill_dir name
    while IFS= read -r skill_dir; do
        name="$(basename "$skill_dir")"
        replace_link "$skill_dir" "$root/$name"
    done < <(find "$REPO/skills" -mindepth 1 -maxdepth 1 -type d -exec test -f "{}/SKILL.md" \; -print | sort)
}

echo "building kilroy..."
(cd "$REPO" && go build -o "${REPO}/kilroy" ./cmd/kilroy)

mkdir -p "$BIN_DIR" "$DATA_DIR"

rm -f "${BIN_DIR}/kilroy"
cp -f "${REPO}/kilroy" "${BIN_DIR}/kilroy"
chmod +x "${BIN_DIR}/kilroy"

echo "refreshing built-in workflows..."
refresh_workflows

echo "installing agent skills..."
install_skills_for_root "$HOME/.claude/skills"
install_skills_for_root "$HOME/.codex/skills"
install_skills_for_root "$HOME/.agents/skills"
install_skills_for_root "$HOME/.config/opencode/skills"

echo ""
echo "installed:"
echo "  binary:    ${BIN_DIR}/kilroy"
echo "  workflows: ${WORKFLOW_DIR}/"
echo "  skills:    ${HOME}/.claude/skills/, ${HOME}/.codex/skills/, ${HOME}/.agents/skills/, ${HOME}/.config/opencode/skills/"
echo ""
if ! echo ":${PATH}:" | grep -q ":${BIN_DIR}:"; then
    echo "note: add ${BIN_DIR} to your PATH"
    echo "  bash/zsh: echo 'export PATH=\"\$PATH:${BIN_DIR}\"' >> ~/.zshrc"
fi
