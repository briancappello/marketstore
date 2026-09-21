#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# State dir is shared with the lock file below so the log always sits next to
# the instance it belongs to. Overridable so the script can be exercised against
# a scratch directory without touching the real instance's lock or logs.
STATE_DIR="${MARKETSTORE_STATE_DIR:-/home/brian/.local/share/marketstore}"
DEFAULT_LOG_FILE="$STATE_DIR/logs/marketstore.log"
MAX_LOG_BYTES="${MARKETSTORE_MAX_LOG_BYTES:-$((256 * 1024 * 1024))}"
KEEP_LOGS="${MARKETSTORE_KEEP_LOGS:-5}"

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Start the MarketStore server."
    echo ""
    echo "Options:"
    echo "  --port PORT        Override the JSON-RPC listen port (default: 5993)"
    echo "  --grpc-port PORT   Override the gRPC listen port (default: 5995)"
    echo "  --log-file PATH    Append server output here (default: $DEFAULT_LOG_FILE)"
    echo "  --no-log           Do not write a log file; inherit stdout/stderr as-is"
    echo "  --help             Show this help message and exit"
    echo ""
    echo "Any other flags are passed through to 'marketstore start'."
    echo ""
    echo "Server output is written to the log file AND echoed to this terminal."
    echo "The log is rotated at startup once it exceeds $((MAX_LOG_BYTES / 1024 / 1024))MB, keeping $KEEP_LOGS generations."
}

EXTRA_ARGS=()
PASSTHROUGH_ARGS=()
LOG_FILE="$DEFAULT_LOG_FILE"

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
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        --no-log)
            LOG_FILE=""
            shift
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

# Size-based rotation, done once at startup rather than during the run: the
# server holds its stdout open for its whole lifetime, so a rename underneath it
# would leave it writing to an unlinked inode. Rotating before the server starts
# is the only point where that is safe without a helper like logrotate.
rotate_log() {
    local f="$1" size i
    [[ -f "$f" ]] || return 0
    size=$(stat -c %s "$f" 2>/dev/null || echo 0)
    (( size < MAX_LOG_BYTES )) && return 0

    for (( i = KEEP_LOGS - 1; i >= 1; i-- )); do
        [[ -f "$f.$i" ]] && mv -f "$f.$i" "$f.$((i + 1))"
    done
    mv -f "$f" "$f.1"
    rm -f "$f.$((KEEP_LOGS + 1))"
}

SERVER_BIN="$SCRIPT_DIR/marketstore"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: no server binary at $SERVER_BIN" >&2
    echo "       Build it first:  make build" >&2
    exit 1
fi

# Mutual exclusion with the user systemd service: both share one data dir, so
# only one server may run at a time. flock holds the lock for the lifetime of
# the exec'd process (fd 9 stays open) and the kernel releases it on exit/crash.
LOCKFILE="$STATE_DIR/marketstore.lock"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    echo "ERROR: another MarketStore instance already holds $LOCKFILE" >&2
    echo "       (the 'marketstore' user systemd service or another start.sh is running)." >&2
    echo "       Stop it first:  systemctl --user stop marketstore" >&2
    exit 1
fi

# Set up logging only after the lock is held, so a failed start still reports to
# the terminal instead of into a log nobody is about to tail.
#
# Previously the server simply inherited this shell's stdout. Launched from a
# terminal that later went away, that left it writing into a pipe with no reader
# -- the replication backfill's own per-pass accounting (rows compared, revised,
# written, MiB to disk) was produced and then dropped on the floor, which is
# exactly the instrumentation you need to diagnose a write-rate regression.
#
# Process substitution rather than a pipeline is deliberate: `cmd | tee` cannot
# be exec'd, so the shell would stay alive as the pipeline's parent and the
# server would no longer be PID-stable or inherit the lock fd directly. This way
# the redirect is installed on the shell first, and the exec below hands those
# same descriptors to the server.
#
# tee runs via setsid, in its own session, so the terminal cannot signal it.
# Ctrl+C goes to every process in the foreground process group, and a tee that
# dies with it leaves the server writing its graceful shutdown into a pipe with
# no reader: the shutdown log is lost and SIGPIPE can kill the server part-way
# through its final WAL flush -- the exact opposite of what Ctrl+C should do.
#
# `trap '' INT` in the subshell is NOT sufficient here; bash does not carry that
# disposition through to the exec'd tee, so tee still dies on Ctrl+C. Detaching
# the session is what actually makes it immune. tee then exits on EOF, once the
# server has closed the pipe for real.
if [[ -n "$LOG_FILE" ]]; then
    mkdir -p "$(dirname "$LOG_FILE")" || exit 1
    rotate_log "$LOG_FILE"
    echo "Logging to $LOG_FILE"
    exec > >(exec setsid tee -a "$LOG_FILE") 2>&1
    echo "=== marketstore start $(date -Is) (pid $$) ==="
fi

# Always the binary `make build` produces, next to this script -- never whatever
# happens to sit in the caller's working directory. The config path below is
# already script-relative, so resolving the binary any other way means a
# start.sh run from elsewhere pairs this instance's config with a different
# build.
#
# exec keeps the server as this shell's replacement: it inherits the lock on fd
# 9, stays the foreground process of the terminal's process group, and so
# receives Ctrl+C (SIGINT) directly, which cmd/start/main.go handles as a
# graceful shutdown.
exec env GOPATH="$SCRIPT_DIR/build" "$SERVER_BIN" start --config "$SCRIPT_DIR/mkts.yml" "${EXTRA_ARGS[@]}" "${PASSTHROUGH_ARGS[@]}"
