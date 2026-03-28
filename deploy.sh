#!/usr/bin/env bash
# deploy.sh — Deploy the current state of main to the prod worktree, rebuild
#             the binary, and restart the systemd service.
#
# Usage: ./deploy.sh [--no-restart] [--system]
#   --no-restart   Update the prod worktree and rebuild but skip service restart.
#   --system       Manage the system-wide service (requires sudo/root).
#                  Default: manage the systemd user service.

set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
NO_RESTART=false
SYSTEM_SERVICE=false

for arg in "$@"; do
    case $arg in
        --no-restart) NO_RESTART=true ;;
        --system)     SYSTEM_SERVICE=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$SCRIPT_DIR"
PROD_DIR="${HOME}/.local/share/marketstore"
PROD_BRANCH="prod"
SERVICE_NAME="marketstore"

if [[ "$SYSTEM_SERVICE" == true ]]; then
    SYSTEMCTL="systemctl"
else
    SYSTEMCTL="systemctl --user"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()    { echo "[deploy] $*"; }
success() { echo "[deploy] ✓ $*"; }

# ---------------------------------------------------------------------------
# 1. Sanity checks
# ---------------------------------------------------------------------------
info "Checking prerequisites..."

# Ensure the prod worktree exists
if [[ ! -e "$PROD_DIR/.git" ]]; then
    echo "ERROR: Prod worktree not found at $PROD_DIR"
    echo "       Run ./setup.sh first."
    exit 1
fi

# Ensure we're on master (deploying from an unintended branch is usually a mistake)
CURRENT_BRANCH="$(git -C "$DEV_DIR" rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" != "master" ]]; then
    echo "WARNING: You are on branch '$CURRENT_BRANCH', not 'master'."
    read -r -p "         Deploy '$CURRENT_BRANCH' to prod anyway? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

# Ensure source files are clean (avoid deploying uncommitted changes to code).
# Only check files that affect the built binary — Go source, protobuf, modules, Makefiles.
SOURCE_PATTERNS=('*.go' '*.proto' 'go.mod' 'go.sum' 'Makefile')
DIRTY_SRC="$(git -C "$DEV_DIR" diff --name-only -- "${SOURCE_PATTERNS[@]}"
            git -C "$DEV_DIR" diff --cached --name-only -- "${SOURCE_PATTERNS[@]}")"
if [[ -n "$DIRTY_SRC" ]]; then
    echo "ERROR: You have uncommitted changes to source files:"
    echo "$DIRTY_SRC" | sed 's/^/         /'
    echo "       Commit or stash them before deploying."
    exit 1
fi

success "Checks passed (branch: $CURRENT_BRANCH)"

# ---------------------------------------------------------------------------
# 2. Reset prod to current branch
# ---------------------------------------------------------------------------
info "Updating '$PROD_BRANCH' to '$CURRENT_BRANCH'..."
DEPLOY_SHA="$(git -C "$DEV_DIR" rev-parse HEAD)"
git -C "$PROD_DIR" fetch "$DEV_DIR" "$CURRENT_BRANCH"
git -C "$PROD_DIR" reset --hard "$DEPLOY_SHA"

DEPLOYED_SHA="$(git -C "$PROD_DIR" rev-parse --short "$DEPLOY_SHA")"
success "Prod is now at $DEPLOYED_SHA"

# ---------------------------------------------------------------------------
# 3. Copy the config file if present in the current directory
# ---------------------------------------------------------------------------
CALLER_DIR="$(pwd)"
if [[ -f "$CALLER_DIR/mkts.yml" ]]; then
    cp "$CALLER_DIR/mkts.yml" "$PROD_DIR/mkts.yml"
    success "Config file copied to $PROD_DIR/mkts.yml"
else
    info "No mkts.yml in current directory — keeping existing config"
fi

# ---------------------------------------------------------------------------
# 4. Rebuild the binary and plugins
# ---------------------------------------------------------------------------
info "Building marketstore binary and plugins..."
(cd "$PROD_DIR" && GOPATH="$PROD_DIR/build" make build)
success "Binary and plugins rebuilt to $PROD_DIR/build/bin/"

# ---------------------------------------------------------------------------
# 5. Restart the service
# ---------------------------------------------------------------------------
if [[ "$NO_RESTART" == true ]]; then
    echo ""
    echo "Skipped service restart (--no-restart)."
    echo "To restart manually: $SYSTEMCTL restart $SERVICE_NAME"
else
    info "Restarting systemd service..."
    $SYSTEMCTL restart "$SERVICE_NAME"
    success "Service restarted"

    # Brief pause then check it actually came up
    sleep 1
    if $SYSTEMCTL is-active --quiet "$SERVICE_NAME"; then
        success "Service is running"
    else
        echo "ERROR: Service failed to start after restart."
        echo "       Check logs with: journalctl $( [[ "$SYSTEM_SERVICE" == false ]] && echo '--user' ) -u $SERVICE_NAME -n 50"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "Deployed $DEPLOYED_SHA"
echo "  JSON-RPC : http://127.0.0.1:5993"
echo "  gRPC     : 127.0.0.1:5995"
echo ""
