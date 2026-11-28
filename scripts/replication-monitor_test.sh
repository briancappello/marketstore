#!/usr/bin/env bash
# Checks scripts/replication-monitor.sh's daily report and 8.2 verdict against
# fixtures, with a fake marketstore binary: no server is needed.
#
#   scripts/replication-monitor_test.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITOR="$SCRIPT_DIR/replication-monitor.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fails=0

check() { # check DESCRIPTION COMMAND...
	if "${@:2}"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}

# The fake binary answers `tool replication market-day [--date DAY]`.
cat > "$TMP/marketstore" <<'EOF'
#!/usr/bin/env bash
day=2026-11-30
[[ "${4:-}" == --date ]] && day="$5"
case "$day" in
	2026-11-27) echo "$day is a trading day; regular close 13:00 EST" ;;
	2026-11-28|2026-11-29) echo "$day is not a trading day"; exit 1 ;;
	*) echo "$day is a trading day; regular close 16:00 EST" ;;
esac
EOF
chmod +x "$TMP/marketstore"
export MONITOR_BIN="$TMP/marketstore" MONITOR_STATE_DIR="$TMP/state"
mkdir -p "$TMP/state"

# Report volume: one interval of 53 entries holding 441 MiB (an overnight
# backfill), one of 100,000 entries holding 1 MiB, and one where retention
# deleted a segment (first_seq moved), whose bytes cannot be known.
MiB=$((1 << 20))
{
	echo "time_ny,master_last_seq,master_first_seq,log_bytes,p1_cursor,lag,dropped,broken,streams,gaps,unrecoverable_tick_gaps,full_resync,applied"
	echo "2026-11-26 23:45:00,100,1,1000,100,0,0,0,1,0,0,0,100"
	echo "2026-11-27 00:00:00,100,1,1000,100,0,0,0,1,0,0,0,100"
	echo "2026-11-27 00:15:00,153,1,$((1000 + 441 * MiB)),153,0,0,0,1,0,0,0,153"
	echo "2026-11-27 00:30:00,100153,1,$((1000 + 442 * MiB)),100153,0,0,0,1,0,0,0,100153"
	echo "2026-11-27 00:45:00,100160,50,$((300 * MiB)),100160,0,0,0,1,0,0,0,100160"
} > "$TMP/state/health.csv"
out="$("$MONITOR" report 2026-11-27)"
check "report counts the appended bytes exactly" \
	grep -qx "replication volume (1.2): 100060 entries, 442 MiB appended" <<< "$out"
check "report names the interval whose bytes retention hid" \
	grep -qx "  bytes not counted: 1 interval(s) with a retention deletion, 7 entries (00:45)" <<< "$out"
check "report finds the busiest 15 min by bytes" \
	grep -qx "  busiest 15 min by bytes: 441 MiB to 00:15, 53 entries" <<< "$out"
check "report finds the busiest 15 min by entries" \
	grep -qx "  busiest 15 min by entries: 100000 entries to 00:30, 1 MiB" <<< "$out"

# 8.2 verdict: every check passed for the half day 11/27 and the full day 11/30.
for day in 2026-11-27 2026-11-30 2026-12-01; do
	mkdir -p "$TMP/state/reports/$day"
	for f in parity-evening.txt ticks.txt parity-morning.txt; do echo "exit=0" > "$TMP/state/reports/$day/$f"; done
done
"$MONITOR" ready-check 2026-11-27 2026-11-30 > /dev/null
"$MONITOR" ready-check 2026-11-30 2026-12-01 > /dev/null
check "a half day does not pass 8.2" test ! -e "$TMP/state/READY-8.2-2026-11-27"
check "a half day is recorded as a half day" grep -q "^Half day 2026-11-27 (regular close 13:00)" "$TMP/state/HALFDAY-8.2-2026-11-27"
check "a full trading day passes 8.2" grep -q "^8.2 passed for trading day 2026-11-30" "$TMP/state/READY-8.2-2026-11-30"

if (( fails > 0 )); then echo "$fails check(s) failed"; exit 1; fi
echo "all checks passed"
