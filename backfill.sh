#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export GOPATH="$SCRIPT_DIR/build"

"$SCRIPT_DIR/build/bin/massive_backfiller" --config "$SCRIPT_DIR/mkts.yml" "$@"
