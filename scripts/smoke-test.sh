#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
exec python3 "$ROOT/scripts/acceptance.py" \
    --server "$ROOT/target/debug/transcoder-server" \
    --client "$ROOT/target/debug/transcoder-client" "$@"
