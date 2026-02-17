#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOPATH="$SCRIPT_DIR/build" ./marketstore start --config "$SCRIPT_DIR/mkts.yml"
