#!/usr/bin/env bash
# logs.sh — Tail the marketstore service logs to stdout.
#
# Usage: ./logs.sh [--system] [-- <journalctl args>]
#   --system   View logs for the system-wide service.
#              Default: view logs for the systemd user service.
#   --         Pass additional arguments to journalctl (e.g., -n 100, --since "1 hour ago")

set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
SYSTEM_SERVICE=false
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --system) SYSTEM_SERVICE=true; shift ;;
        --) shift; EXTRA_ARGS=("$@"); break ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE_NAME="marketstore"

if [[ "$SYSTEM_SERVICE" == true ]]; then
    JOURNALCTL="journalctl"
else
    JOURNALCTL="journalctl --user"
fi

# ---------------------------------------------------------------------------
# Tail logs
# ---------------------------------------------------------------------------
exec $JOURNALCTL -u "$SERVICE_NAME" -f "${EXTRA_ARGS[@]}"
