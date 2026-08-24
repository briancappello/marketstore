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

# Mutual exclusion with the user systemd service: both share one data dir, so
# only one server may run at a time. flock holds the lock for the lifetime of
# the exec'd process (fd 9 stays open) and the kernel releases it on exit/crash.
LOCKFILE="/home/brian/.local/share/marketstore/marketstore.lock"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    echo "ERROR: another MarketStore instance already holds $LOCKFILE" >&2
    echo "       (the 'marketstore' user systemd service or another start.sh is running)." >&2
    echo "       Stop it first:  systemctl --user stop marketstore" >&2
    exit 1
fi

exec env GOPATH="$SCRIPT_DIR/build" ./marketstore start --config "$SCRIPT_DIR/mkts.yml" "${EXTRA_ARGS[@]}" "${PASSTHROUGH_ARGS[@]}"
