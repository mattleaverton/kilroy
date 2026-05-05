#!/usr/bin/env bash
# Install kilroy binary and built-in workflows from this repo.
# Re-run after git pull to update both.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${HOME}/.local/bin"
DATA_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/kilroy"

echo "building kilroy..."
(cd "$REPO" && go build -o "${REPO}/kilroy" ./cmd/kilroy)

mkdir -p "$BIN_DIR" "${DATA_DIR}/workflows"

cp -f "${REPO}/kilroy" "${BIN_DIR}/kilroy"
chmod +x "${BIN_DIR}/kilroy"

cp -RL "${REPO}/workflows/." "${DATA_DIR}/workflows/"

echo ""
echo "installed:"
echo "  binary:    ${BIN_DIR}/kilroy"
echo "  workflows: ${DATA_DIR}/workflows/"
echo ""
if ! echo ":${PATH}:" | grep -q ":${BIN_DIR}:"; then
    echo "note: add ${BIN_DIR} to your PATH"
    echo "  bash/zsh: echo 'export PATH=\"\$PATH:${BIN_DIR}\"' >> ~/.bashrc"
fi
