#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Start the MarketStore server."
    echo ""
    echo "Options:"
    echo "  --port PORT        Override the JSON-RPC listen port (default: 5993)"
    echo "  --grpc-port PORT   Override the gRPC listen port (default: 5995)"
    echo "  --help             Show this help message and exit"
    echo ""
    echo "Any other flags are passed through to 'marketstore start'."
}

EXTRA_ARGS=()
PASSTHROUGH_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)
            EXTRA_ARGS+=(--listen-port "$2")
            shift 2
            ;;
        --grpc-port)
            EXTRA_ARGS+=(--grpc-listen-port "$2")
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
    esac
done

GOPATH="$SCRIPT_DIR/build" ./marketstore start --config "$SCRIPT_DIR/mkts.yml" "${EXTRA_ARGS[@]}" "${PASSTHROUGH_ARGS[@]}"
