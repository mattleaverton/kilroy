#!/usr/bin/env bash
# Starts the CXDB UI (the built-in HTTP server already serves UI).
set -euo pipefail

# The CXDB server's HTTP endpoint already serves the UI at its root.
# This script is a no-op placeholder since the UI is built into the server.
# If a separate gateway or frontend is needed, wire it here.

echo "CXDB UI available at ${KILROY_CXDB_UI_URL:-http://127.0.0.1:9010}"

# Keep alive so Kilroy doesn't treat this as a failed process.
exec sleep infinity
