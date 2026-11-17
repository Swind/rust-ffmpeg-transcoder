#!/bin/sh
set -eu
if [ "$#" -eq 0 ]; then
    echo "usage: $0 DIRECTORY [--without-backpressure]" >&2
    exit 2
fi
exec python3 "$(dirname "$0")/fixtures.py" "$@"
