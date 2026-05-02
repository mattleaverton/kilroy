#!/usr/bin/env bash
set -euo pipefail
INPUT="${INPUT_FILE:-.kilroy/INPUT.md}"
DIFF=".kilroy/diff.patch"
mkdir -p .kilroy

# Initialise diff.patch empty up-front so the contract holds even if any
# extraction below exits non-zero (set -euo pipefail aborts the script,
# but downstream readers — summary.sh, the agent — get a present empty
# file rather than a missing one). This was the second of two bugs the
# reviewer caught: a one-section INPUT.md aborted the sed pipeline
# before the file was ever created.
: > "$DIFF"

# Engine wrote ## target, ## checklist, ## context_files, ## scope_directive
# already. Our job: gather the diff to review into .kilroy/diff.patch so
# the agent has a single canonical artifact to read regardless of the
# target shape (.patch file / branch ref / PR URL).

# Extract the value of the ## target section. We use awk for robustness:
# unlike `sed -n /a/,/b/p | sed '1d;$d' | grep -v '^$'`, awk doesn't
# require a closing ## section header (one-section INPUT.md is valid),
# and it can't return a non-zero pipeline status that pipefail would
# turn into a script abort.
TARGET="$(awk '
    /^## target$/      { in_section = 1; next }
    /^## /             { in_section = 0 }
    in_section && NF   { print; exit }
' "$INPUT" 2>/dev/null || true)"

stage_status_success() {
    echo '{"status":"success"}' > "${KILROY_STAGE_STATUS_PATH:-/dev/null}" 2>/dev/null || true
}

if [ -z "${TARGET:-}" ]; then
    {
        echo
        echo "## stage_context_note"
        echo
        echo "No \`## target\` section found in INPUT.md. The agent will have an empty .kilroy/diff.patch."
    } >> "$INPUT"
    stage_status_success
    exit 0
fi

case "$TARGET" in
    *.patch)
        # Path to a .patch file — copy it verbatim.
        if [ -f "$TARGET" ]; then
            cp "$TARGET" "$DIFF"
            {
                echo
                echo "## stage_context_note"
                echo
                echo "Copied \`$TARGET\` -> \`$DIFF\` ($(wc -c < "$DIFF" | tr -d ' ') bytes)."
            } >> "$INPUT"
        else
            {
                echo
                echo "## stage_context_note"
                echo
                echo "TODO: target \`$TARGET\` looks like a .patch path but the file does not exist; .kilroy/diff.patch is empty."
            } >> "$INPUT"
        fi
        ;;
    http://*|https://*)
        # PR URL — full gh integration is out of scope for v0.
        {
            echo
            echo "## stage_context_note"
            echo
            echo "TODO: PR URL targets are not yet supported by the v0 review workflow."
            echo "Target was: \`$TARGET\`"
            echo "For now, fetch the diff manually (e.g. \`gh pr diff <num> > /tmp/x.patch\`) and re-run with that path as target."
        } >> "$INPUT"
        ;;
    *)
        # Treat as a git ref (branch, tag, sha). Generate the diff that
        # the target REF introduces relative to its merge-base with the
        # current HEAD — i.e. what `gh pr diff <ref>` would show. The
        # earlier version of this script diffed BASE..HEAD instead of
        # BASE..$TARGET, which is wrong: it reviews the caller's branch
        # rather than the requested target. (Reviewer-reproduced bug.)
        if git rev-parse --verify --quiet "$TARGET" >/dev/null 2>&1; then
            BASE="$(git merge-base "$TARGET" HEAD 2>/dev/null || echo "$TARGET")"
            {
                echo "# Commits in $BASE..$TARGET (base: $BASE, target: $TARGET)"
                echo "#"
                git log --no-color --format='# %h %s' "$BASE..$TARGET" 2>/dev/null || true
                echo "#"
                git diff --no-color --no-ext-diff "$BASE..$TARGET" 2>/dev/null || true
            } > "$DIFF"
            {
                echo
                echo "## stage_context_note"
                echo
                echo "Generated diff for ref \`$TARGET\` (base: \`$BASE\`) -> \`$DIFF\` ($(wc -c < "$DIFF" | tr -d ' ') bytes)."
            } >> "$INPUT"
        else
            {
                echo
                echo "## stage_context_note"
                echo
                echo "Target \`$TARGET\` is not a recognized git ref, .patch file, or PR URL; .kilroy/diff.patch is empty."
            } >> "$INPUT"
        fi
        ;;
esac

# Optional: append the contents of any listed context_files for the
# agent's convenience (same affordance as fix/implement). Use awk for
# the same robustness reason as above — sed-then-grep can fail under
# pipefail when no paths are listed.
if grep -q '^## context_files' "$INPUT" 2>/dev/null; then
    {
        echo
        echo "## context_files_contents"
        echo
        awk '
            /^## context_files$/  { in_section = 1; next }
            /^## /                { in_section = 0 }
            in_section && NF      { print }
        ' "$INPUT" \
            | while IFS= read -r path; do
                if [ -n "$path" ] && [ -f "$path" ]; then
                    echo "### $path"
                    echo '```'
                    cat "$path"
                    echo '```'
                    echo
                fi
            done
    } >> "$INPUT"
fi

stage_status_success
