#!/usr/bin/env bash
# setup.sh — One-time setup: creates the prod git worktree, builds the binary,
#             and installs + enables the systemd service.
#
# Usage: ./setup.sh [--system]
#   --system   Install as a system-wide service (requires sudo/root).
#              Default: install as a systemd user service.
#
# Run this once from the dev repo root (or from anywhere — it uses its own path).
# Safe to re-run: each step checks whether it's already been done.

set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
SYSTEM_SERVICE=false

for arg in "$@"; do
    case $arg in
        --system) SYSTEM_SERVICE=true ;;
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
    SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
    SYSTEMCTL="systemctl"
else
    SERVICE_FILE="${HOME}/.config/systemd/user/${SERVICE_NAME}.service"
    SYSTEMCTL="systemctl --user"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()    { echo "[setup] $*"; }
success() { echo "[setup] ✓ $*"; }
warn()    { echo "[setup] ! $*"; }

# ---------------------------------------------------------------------------
# 1. Ensure we are not on the prod branch
# ---------------------------------------------------------------------------
info "Checking current branch..."
CURRENT_BRANCH="$(git -C "$DEV_DIR" rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" == "$PROD_BRANCH" ]]; then
    echo "ERROR: You are currently on the '$PROD_BRANCH' branch."
    echo "       Switch to 'master' before running setup."
    exit 1
fi
success "Current branch: $CURRENT_BRANCH"

# ---------------------------------------------------------------------------
# 2. Create (or verify) the prod git worktree
# ---------------------------------------------------------------------------
info "Setting up git worktree at $PROD_DIR..."

# Prune stale worktree entries (e.g., if directory was manually deleted)
git -C "$DEV_DIR" worktree prune

if [[ -d "$PROD_DIR" ]] && git -C "$DEV_DIR" worktree list --porcelain | grep -q "worktree $PROD_DIR$"; then
    warn "Worktree already exists at $PROD_DIR — skipping creation."
else
    # Create the prod branch if it doesn't exist, then add worktree
    if git -C "$DEV_DIR" show-ref --verify --quiet "refs/heads/$PROD_BRANCH"; then
        git -C "$DEV_DIR" worktree add "$PROD_DIR" "$PROD_BRANCH"
    else
        git -C "$DEV_DIR" worktree add -b "$PROD_BRANCH" "$PROD_DIR" HEAD
    fi
    success "Worktree created at $PROD_DIR on branch '$PROD_BRANCH'."
fi

# ---------------------------------------------------------------------------
# 3. Build the marketstore binary and plugins in the prod worktree
# ---------------------------------------------------------------------------
info "Building marketstore binary and plugins..."
(cd "$PROD_DIR" && GOPATH="$PROD_DIR/build" make build)
success "Binary built at $PROD_DIR/marketstore"
success "Plugins built at $PROD_DIR/build/bin/"

# ---------------------------------------------------------------------------
# 4. Copy the config file from the current directory
# ---------------------------------------------------------------------------
CALLER_DIR="$(pwd)"
if [[ ! -f "$CALLER_DIR/mkts.yml" ]]; then
    echo "ERROR: No mkts.yml found in the current directory ($CALLER_DIR)."
    echo "       Run this script from a directory containing mkts.yml."
    exit 1
fi
cp "$CALLER_DIR/mkts.yml" "$PROD_DIR/mkts.yml"
success "Config file copied to $PROD_DIR/mkts.yml"

# ---------------------------------------------------------------------------
# 5. Install the systemd service
# ---------------------------------------------------------------------------
info "Installing systemd service..."

if [[ "$SYSTEM_SERVICE" == true ]]; then
    mkdir -p "$(dirname "$SERVICE_FILE")"
    # Write via sudo since /etc/systemd/system requires root
    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=MarketStore time-series database server
After=network.target

[Service]
Type=simple
User=${USER}
Environment="GOPATH=${PROD_DIR}/build"
WorkingDirectory=${PROD_DIR}
ExecStart=${PROD_DIR}/marketstore start --config ${PROD_DIR}/mkts.yml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
else
    mkdir -p "$(dirname "$SERVICE_FILE")"
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=MarketStore time-series database server
After=network.target

[Service]
Type=simple
Environment="GOPATH=${PROD_DIR}/build"
WorkingDirectory=${PROD_DIR}
ExecStart=${PROD_DIR}/marketstore start --config ${PROD_DIR}/mkts.yml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
fi

success "Service file written to $SERVICE_FILE"

# ---------------------------------------------------------------------------
# 6. Enable and start the service
# ---------------------------------------------------------------------------
info "Reloading systemd daemon..."
$SYSTEMCTL daemon-reload

info "Enabling service to start on login..."
$SYSTEMCTL enable "$SERVICE_NAME"

info "Starting service..."
$SYSTEMCTL start "$SERVICE_NAME"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "Setup complete."
echo ""
echo "  Service status : $SYSTEMCTL status $SERVICE_NAME"
echo "  JSON-RPC URL   : http://127.0.0.1:5993"
echo "  gRPC URL       : 127.0.0.1:5995"
echo "  Deploy updates : ./deploy.sh"
echo ""
