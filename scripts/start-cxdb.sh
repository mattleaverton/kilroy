#!/usr/bin/env bash
# Starts the CXDB server for Kilroy attractor runs.
set -euo pipefail

: "${KILROY_LOGS_ROOT:?KILROY_LOGS_ROOT must be set}"

CXDB_DATA_DIR="${KILROY_LOGS_ROOT}/cxdb-data"
mkdir -p "$CXDB_DATA_DIR"

export CXDB_DATA_DIR
export CXDB_BIND="${KILROY_CXDB_BINARY_ADDR:-127.0.0.1:9009}"
export CXDB_HTTP_BIND="${KILROY_CXDB_HTTP_BASE_URL:-http://127.0.0.1:9010}"
# Strip protocol prefix for bind address
CXDB_HTTP_BIND="${CXDB_HTTP_BIND#http://}"
CXDB_HTTP_BIND="${CXDB_HTTP_BIND#https://}"
export CXDB_HTTP_BIND

exec cxdb-server
