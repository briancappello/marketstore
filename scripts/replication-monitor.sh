#!/usr/bin/env bash
# Monitors the replica (this host) against its master and records the checks
# of openspec change replication-gapless-stream, section 8. Read-only: it only
# queries both servers and hashes files.
#
#   scripts/replication-monitor.sh start    run in the background (pidfile, log)
#   scripts/replication-monitor.sh stop     stop the background monitor
#   scripts/replication-monitor.sh status   latest health line, alerts, reports
#   scripts/replication-monitor.sh once     run every check now, in the foreground
#
# Schedule (America/New_York):
#   every 15 min        health: drops, broken log, lag, gaps, p1 up; taichi log volume
#   06:00 trading days  parity: compare 1Min/1D/SESSIONS, last 5 trading days
#   open-5/+5, close-5/+5 trading days
#                       watchlist snapshots from both servers, compared
#   20:30 trading days  parity again, and tick files hashed on both hosts
#   21:00 every day     the day's report; READY-8.2-<day> once a trading day passed
#                       every check, including the next morning's parity
#
# Alerts go to $STATE_DIR/alerts.log and, when a desktop session is reachable,
# notify-send. Everything is written under $STATE_DIR.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
STATE_DIR="${MONITOR_STATE_DIR:-$HOME/.local/share/marketstore/monitor}"
BIN="${MONITOR_BIN:-$REPO_DIR/marketstore}"
MASTER_SSH="${MONITOR_MASTER_SSH:-taichi}"
MASTER_GRPC="${MONITOR_MASTER_GRPC:-taichi:5995}"
MASTER_HTTP="${MONITOR_MASTER_HTTP:-http://taichi:5993}"
REPLICA_GRPC="${MONITOR_REPLICA_GRPC:-localhost:5995}"
REPLICA_HTTP="${MONITOR_REPLICA_HTTP:-http://localhost:5993}"
LOCAL_DATA="${MONITOR_LOCAL_DATA:-$HOME/.local/share/marketstore/data}"
MASTER_DATA="${MONITOR_MASTER_DATA:-.local/share/marketstore/data}"
LAG_ALERT="${MONITOR_LAG_ALERT:-5000}"   # entries behind, for 3 checks in a row
TZNY=America/New_York

mkdir -p "$STATE_DIR/reports" "$STATE_DIR/done"
PIDFILE="$STATE_DIR/monitor.pid"
LOCKFILE="$STATE_DIR/monitor.lock"
LOGFILE="$STATE_DIR/monitor.log"

ny() { TZ=$TZNY date "$@"; }
log() { echo "$(ny '+%F %T %Z') $*" >> "$LOGFILE"; }
today() { ny +%F; }
reportdir() { local d="$STATE_DIR/reports/${1:-$(today)}"; mkdir -p "$d"; echo "$d"; }

alert() {
	local msg="$*"
	echo "$(ny '+%F %T %Z') ALERT $msg" >> "$STATE_DIR/alerts.log"
	log "ALERT $msg"
	if command -v notify-send >/dev/null 2>&1 && [[ -n "${DBUS_SESSION_BUS_ADDRESS:-}" ]]; then
		notify-send -u critical "MarketStore replication" "$msg" >/dev/null 2>&1 || true
	fi
}

notify() {
	log "NOTE $*"
	if command -v notify-send >/dev/null 2>&1 && [[ -n "${DBUS_SESSION_BUS_ADDRESS:-}" ]]; then
		notify-send "MarketStore replication" "$*" >/dev/null 2>&1 || true
	fi
}

# metric NAME [LABEL-REGEX] from a scrape held in $1.
metric() {
	local scrape="$1" name="$2" labels="${3:-}"
	awk -v n="alpaca_marketstore_$name" -v l="$labels" '
		$1 == n || (index($1, n "{") == 1 && (l == "" || $1 ~ l)) { v = $2; found = 1 }
		END { if (found) printf "%.0f", v; else print "NA" }' <<< "$scrape"
}

trading_day() { "$BIN" tool replication market-day ${1:+--date "$1"} >/dev/null 2>&1; }

# close_hhmm prints the regular close (HHMM, New York) of today, e.g. 1600 or 1300.
close_hhmm() {
	"$BIN" tool replication market-day 2>/dev/null | sed -n 's/.*regular close \([0-9][0-9]\):\([0-9][0-9]\).*/\1\2/p'
}

state_get() { cat "$STATE_DIR/$1" 2>/dev/null || echo "${2:-}"; }
state_set() { echo "$2" > "$STATE_DIR/$1"; }

health() {
	local p1 ma ts
	ts="$(ny '+%F %T')"
	p1="$(curl -s -m 15 "$REPLICA_HTTP/metrics")" || p1=""
	ma="$(curl -s -m 15 "$MASTER_HTTP/metrics")" || ma=""
	if ! pgrep -x marketstore >/dev/null; then
		alert "p1: no marketstore process is running"
	fi
	if [[ -z "$p1" ]]; then alert "p1: $REPLICA_HTTP/metrics unreachable"; fi
	if [[ -z "$ma" ]]; then alert "master: $MASTER_HTTP/metrics unreachable"; fi

	local last first bytes dropped broken streams cursor gaps unrec resync applied
	last=$(metric "$ma" replication_log_last_seq)
	first=$(metric "$ma" replication_log_first_seq)
	bytes=$(metric "$ma" replication_log_bytes)
	dropped=$(metric "$ma" replication_dropped_messages_total)
	broken=$(metric "$ma" replication_log_broken)
	streams=$(grep -c '^alpaca_marketstore_replication_replica_lag_seq{' <<< "$ma")
	cursor=$(metric "$p1" replication_replica_cursor)
	gaps=$(metric "$p1" replication_replica_gaps_total)
	unrec=$(metric "$p1" replication_unrecoverable_tick_gaps_total)
	resync=$(metric "$p1" replication_full_resync_required)
	applied=$(metric "$p1" replication_replay_applied_total)
	local lag=NA
	if [[ "$last" != NA && "$cursor" != NA ]]; then lag=$((last - cursor)); fi

	local csv="$STATE_DIR/health.csv"
	[[ -f "$csv" ]] || echo "time_ny,master_last_seq,master_first_seq,log_bytes,p1_cursor,lag,dropped,broken,streams,gaps,unrecoverable_tick_gaps,full_resync,applied" > "$csv"
	echo "$ts,$last,$first,$bytes,$cursor,$lag,$dropped,$broken,$streams,$gaps,$unrec,$resync,$applied" >> "$csv"

	# Edge-triggered alerts: each fires when its counter moves, not every check.
	local prev
	for kv in "dropped:$dropped" "gaps:$gaps" "unrec:$unrec"; do
		local k="${kv%%:*}" v="${kv#*:}"
		[[ "$v" == NA ]] && continue
		prev="$(state_get "last-$k" "$v")"
		if (( v > prev )); then alert "$k rose from $prev to $v"; fi
		state_set "last-$k" "$v"
	done
	if [[ "$dropped" != NA ]] && (( dropped > 0 )) && [[ "$(state_get alerted-dropped)" != yes ]]; then
		alert "replication_dropped_messages_total is $dropped (8.2 requires 0)"
		state_set alerted-dropped yes
	fi
	[[ "$broken" == 1 ]] && alert "master replication log is broken (appends failing)"
	[[ "$resync" == 1 && "$(state_get alerted-resync)" != yes ]] && { alert "p1: full resync required"; state_set alerted-resync yes; }
	[[ -n "$ma" && "$streams" == 0 ]] && alert "master: no replica stream is connected"
	if [[ "$lag" != NA ]] && (( lag > LAG_ALERT )); then
		local n; n=$(( $(state_get lag-high 0) + 1 )); state_set lag-high "$n"
		(( n == 3 )) && alert "p1 has been more than $LAG_ALERT entries behind for 3 checks (now $lag)"
	else
		state_set lag-high 0
	fi
	log "health last=$last cursor=$cursor lag=$lag dropped=$dropped broken=$broken streams=$streams gaps=$gaps unrec=$unrec"
}

parity() {
	local label="$1" out rc
	out="$(reportdir)/parity-$label.txt"
	timeout 3600 "$BIN" tool replication compare --master "$MASTER_GRPC" --replica "$REPLICA_GRPC" --days 5 \
		2>/dev/null > "$out"
	rc=$?
	echo "exit=$rc" >> "$out"
	if (( rc != 0 )); then alert "parity ($label): the replica differs from the master, see $out"; fi
	log "parity $label exit=$rc"
}

ticks() {
	local out rc=0
	out="$(reportdir)/ticks.txt"
	local files
	files="$(ssh -o BatchMode=yes "$MASTER_SSH" "cd $MASTER_DATA && ls -1 */1Sec/TRADE/*.bin */1Sec/QUOTE/*.bin 2>/dev/null")" || files=""
	if [[ -z "$files" ]]; then
		echo "no tick files listed on the master" > "$out"; alert "ticks: cannot list the master's tick files"; return
	fi
	# shellcheck disable=SC2086
	ssh -o BatchMode=yes "$MASTER_SSH" "cd $MASTER_DATA && sha256sum $(echo $files)" | sort -k2 > "$out.master"
	# shellcheck disable=SC2086
	(cd "$LOCAL_DATA" && sha256sum $files 2>&1) | sort -k2 > "$out.replica"
	if diff "$out.master" "$out.replica" > "$out"; then
		echo "identical: $(wc -l < "$out.master") tick files" >> "$out"
	else
		rc=1; alert "ticks: tick files differ from the master, see $out"
	fi
	echo "exit=$rc" >> "$out"
	log "ticks exit=$rc"
}

rankings() {
	local label="$1" dir; dir="$(reportdir)"
	curl -s -m 30 "$MASTER_HTTP/v1/watchlists" > "$dir/watchlists-$label-master.json" &
	curl -s -m 30 "$REPLICA_HTTP/v1/watchlists" > "$dir/watchlists-$label-replica.json" &
	wait
	python3 - "$dir/watchlists-$label-master.json" "$dir/watchlists-$label-replica.json" \
		> "$dir/rankings-$label.txt" 2>&1 <<'EOF'
import json, sys
def load(p):
    try:
        return {w["name"]: w for w in json.load(open(p))["watchlists"]}
    except Exception as e:
        print(f"cannot read {p}: {e}"); return {}
m, r = load(sys.argv[1]), load(sys.argv[2])
equal = 0
for n in sorted(set(m) | set(r)):
    em = [(e["symbol"], e.get("fields")) for e in m.get(n, {}).get("entries", [])]
    er = [(e["symbol"], e.get("fields")) for e in r.get(n, {}).get("entries", [])]
    same = em == er
    equal += same
    common = len({s for s, _ in em} & {s for s, _ in er})
    print(f"{n:20s} master={len(em):3d} replica={len(er):3d} common={common:3d} identical={same}")
print(f"watchlists identical: {equal} of {len(set(m) | set(r))}")
EOF
	log "rankings $label: $(tail -1 "$dir/rankings-$label.txt")"
}

report() {
	local day="$1" dir; dir="$(reportdir "$day")"
	python3 - "$STATE_DIR" "$day" > "$dir/REPORT.md" 2>&1 <<'EOF'
import csv, os, sys, glob
state, day = sys.argv[1], sys.argv[2]
d = os.path.join(state, "reports", day)
rows = [r for r in csv.DictReader(open(os.path.join(state, "health.csv"))) if r["time_ny"].startswith(day)] \
    if os.path.exists(os.path.join(state, "health.csv")) else []
def num(r, k):
    try: return int(r[k])
    except Exception: return None
print(f"# Replication report {day} (America/New_York)\n")
alerts = [l.strip() for l in open(os.path.join(state, "alerts.log"))] if os.path.exists(os.path.join(state, "alerts.log")) else []
alerts = [a for a in alerts if a.startswith(day)]
print(f"health checks: {len(rows)}; alerts: {len(alerts)}")
for a in alerts[:20]: print(f"  {a}")
if rows:
    lags = [num(r, "lag") for r in rows if num(r, "lag") is not None]
    drops = [num(r, "dropped") for r in rows if num(r, "dropped") is not None]
    print(f"max lag: {max(lags) if lags else 'NA'} entries; dropped at end: {drops[-1] if drops else 'NA'}")
    seqs = [(num(r, "master_last_seq"), num(r, "master_first_seq"), num(r, "log_bytes")) for r in rows]
    seqs = [s for s in seqs if None not in s]
    if len(seqs) >= 2:
        n = seqs[-1][0] - seqs[0][0]
        last, first, b = seqs[-1]
        avg = b / max(1, last - first + 1)
        print(f"replication volume (1.2): {n} entries, about {n*avg/2**20:.0f} MiB (avg {avg/1024:.1f} KiB/entry)")
        peak = 0
        for (a, _, _), (c, _, _) in zip(seqs, seqs[1:]):
            peak = max(peak, c - a)
        print(f"  busiest 15 min: {peak} entries, about {peak*avg/2**20:.0f} MiB")
for f in sorted(glob.glob(os.path.join(d, "parity-*.txt")) + glob.glob(os.path.join(d, "ticks.txt"))):
    lines = open(f).read().strip().splitlines()
    print(f"\n{os.path.basename(f)}:")
    for l in lines[-14:]: print(f"  {l}")
for f in sorted(glob.glob(os.path.join(d, "rankings-*.txt"))):
    print(f"\n{os.path.basename(f)}: {open(f).read().strip().splitlines()[-1]}")
EOF
	log "report $day written"
}

# ready_check DAY: 8.2 for trading day DAY passes when its health stayed clean,
# its evening parity and tick hashes passed, and the next morning's parity passed.
ready_check() {
	local day="$1" next="$2" d n
	d="$STATE_DIR/reports/$day"; n="$STATE_DIR/reports/$next"
	[[ -f "$STATE_DIR/READY-8.2-$day" ]] && return
	grep -q '^exit=0' "$d/parity-evening.txt" 2>/dev/null || return
	grep -q '^exit=0' "$d/ticks.txt" 2>/dev/null || return
	grep -q '^exit=0' "$n/parity-morning.txt" 2>/dev/null || return
	if grep -q "^$day" "$STATE_DIR/alerts.log" 2>/dev/null || grep -q "^$next 0[0-6]" "$STATE_DIR/alerts.log" 2>/dev/null; then
		return
	fi
	{
		echo "8.2 passed for trading day $day:"
		echo "- no alerts from $day through the next morning's parity on $next"
		echo "- evening parity, tick hashes and the next morning's parity all exit 0"
		echo "Reports: $d and $n. 8.3 also needs the watchlist checks reviewed (rankings-*.txt)."
	} > "$STATE_DIR/READY-8.2-$day"
	notify "8.2 passed for $day; see $STATE_DIR/READY-8.2-$day"
}

# once_per KEY CMD...: run CMD at most once per key (e.g. date + job).
once_per() {
	local key="$1"; shift
	[[ -f "$STATE_DIR/done/$key" ]] && return
	touch "$STATE_DIR/done/$key"
	"$@"
}

hhmm_add() { # HHMM +/- minutes
	local h=$((10#${1:0:2})) m=$((10#${1:2:2})) d="$2" t
	t=$(( h * 60 + m + d )); printf "%02d%02d" $((t / 60)) $((t % 60))
}

tick() {
	local day now min
	day="$(today)"; now="$(ny +%H%M)"; min=$((10#$(ny +%M)))
	(( min % 15 == 0 )) && once_per "$day-health-$now" health
	if trading_day; then
		local close; close="$(close_hhmm)"; close="${close:-1600}"
		[[ "$now" == 0600 ]] && once_per "$day-parity-morning" parity morning
		for b in "$(hhmm_add 0930 -5)" "$(hhmm_add 0930 5)" "$(hhmm_add "$close" -5)" "$(hhmm_add "$close" 5)"; do
			[[ "$now" == "$b" ]] && once_per "$day-rankings-$b" rankings "$b"
		done
		[[ "$now" == 2030 ]] && { once_per "$day-parity-evening" parity evening; once_per "$day-ticks" ticks; }
	fi
	if [[ "$now" == 2100 ]]; then
		once_per "$day-report" report "$day"
	fi
	# A trading day's 8.2 verdict needs the next trading day's morning parity.
	if (( 10#$now > 605 )); then
		local prev; prev="$(ny -d "$day -1 day" +%F)"
		for _ in 1 2 3 4 5; do trading_day "$prev" && break; prev="$(ny -d "$prev -1 day" +%F)"; done
		ready_check "$prev" "$day"
	fi
}

run() {
	exec 9>"$LOCKFILE"
	if ! flock -n 9; then echo "another monitor holds $LOCKFILE" >&2; exit 1; fi
	echo $$ > "$PIDFILE"
	log "monitor started (pid $$, master $MASTER_GRPC, replica $REPLICA_GRPC)"
	trap 'log "monitor stopped"; rm -f "$PIDFILE"; exit 0' INT TERM
	while true; do
		# Jobs must not inherit the lock (fd 9): one still running after the
		# monitor stopped would keep a new monitor from starting.
		tick 9>&-
		# Sleep in the background: a foreground sleep would hold off the
		# TERM trap until it ends.
		sleep $(( 60 - 10#$(date +%S) )) 9>&- &
		wait $!
	done
}

case "${1:-}" in
	start)
		if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
			echo "already running (pid $(cat "$PIDFILE"))"; exit 0
		fi
		[[ -x "$BIN" ]] || { echo "no binary at $BIN: run make build" >&2; exit 1; }
		setsid nohup "$0" run >> "$LOGFILE" 2>&1 < /dev/null &
		sleep 1
		echo "monitor started (pid $(cat "$PIDFILE" 2>/dev/null)); state in $STATE_DIR"
		;;
	stop)
		if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
			pid="$(cat "$PIDFILE")"; kill "$pid"
			for _ in $(seq 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
			if kill -0 "$pid" 2>/dev/null; then echo "pid $pid did not stop" >&2; exit 1; fi
			echo "stopped"
		else
			echo "not running"
		fi
		;;
	status)
		if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then echo "running (pid $(cat "$PIDFILE"))"; else echo "not running"; fi
		echo "last health: $(tail -1 "$STATE_DIR/health.csv" 2>/dev/null)"
		if [[ -f "$STATE_DIR/alerts.log" ]]; then
			echo "alerts: $(wc -l < "$STATE_DIR/alerts.log")"; tail -3 "$STATE_DIR/alerts.log"
		else
			echo "alerts: 0"
		fi
		ls -1 "$STATE_DIR"/READY-* 2>/dev/null
		ls -1d "$STATE_DIR"/reports/* 2>/dev/null | tail -3
		;;
	once)
		health; parity manual; ticks; rankings manual; report "$(today)"
		cat "$(reportdir)/REPORT.md"
		;;
	run) run ;;
	*) sed -n '2,22p' "$0"; exit 2 ;;
esac
