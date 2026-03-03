#!/usr/bin/env bash
# service.sh — Manage the marketstore systemd service.
#
# Usage: ./service.sh <command> [--system]
#   command    One of: start, stop, restart, status
#   --system   Manage the system-wide service (requires sudo/root).
#              Default: manage the systemd user service.

set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
SYSTEM_SERVICE=false
COMMAND=""

for arg in "$@"; do
    case $arg in
        --system) SYSTEM_SERVICE=true ;;
        start|stop|restart|status)
            if [[ -n "$COMMAND" ]]; then
                echo "ERROR: Multiple commands specified: '$COMMAND' and '$arg'"
                exit 1
            fi
            COMMAND="$arg"
            ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

if [[ -z "$COMMAND" ]]; then
    echo "Usage: $0 <start|stop|restart|status> [--system]"
    exit 1
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE_NAME="marketstore"

if [[ "$SYSTEM_SERVICE" == true ]]; then
    SYSTEMCTL="systemctl"
else
    SYSTEMCTL="systemctl --user"
fi

# ---------------------------------------------------------------------------
# Execute command
# ---------------------------------------------------------------------------
exec $SYSTEMCTL "$COMMAND" "$SERVICE_NAME"
