#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export GOPATH="$SCRIPT_DIR/build"

CONFIG="$SCRIPT_DIR/mkts.yml"
BIN="$SCRIPT_DIR/build/bin/massive_flatfile_backfiller"

# --timeframe is a thin wrapper over the backfiller's native -from KEY=DATE:
# it picks the start date from query_start in mkts.yml so daily-only is a
# one-flag call (./backfill.sh --timeframe 1D --symbols AAPL) instead of the
# magic -from 1D=DATE form. --from DATE overrides the config start date.
# Everything else is forwarded untouched.
timeframe=""
from_override=""
passthrough=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeframe|-t) timeframe="$2"; shift 2 ;;
    --from|-from)   from_override="$2"; shift 2 ;;
    --config)       CONFIG="$2"; shift 2 ;;
    *)              passthrough+=("$1"); shift ;;
  esac
done

if [[ -n "$timeframe" ]]; then
  case "$timeframe" in
    1D|1Min) ;;
    *) echo "backfill.sh: --timeframe must be 1D or 1Min, got '$timeframe'" >&2; exit 2 ;;
  esac
  start="$from_override"
  if [[ -z "$start" ]]; then
    # grep the query_start date for this timeframe out of mkts.yml, e.g. '1D: "2016-01-01"'.
    start="$(grep -E "^\s*${timeframe}:\s*\"[0-9-]+\"" "$CONFIG" | head -1 | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')"
    if [[ -z "$start" ]]; then
      echo "backfill.sh: no query_start for '$timeframe' in $CONFIG; pass --from YYYY-MM-DD" >&2
      exit 2
    fi
  fi
  passthrough+=("-from" "${timeframe}=${start}")
elif [[ -n "$from_override" ]]; then
  passthrough+=("-from" "$from_override")
fi

exec "$BIN" --config "$CONFIG" "${passthrough[@]}"
