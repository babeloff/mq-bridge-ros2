#!/usr/bin/env bash
# Memory-IPC throughput — a `static` load generator (process A) forwards
# messages to a receiver (process B) over a real Unix domain socket
# (memory: endpoint with a unix:// topic), not just an in-process channel.
# rows/sec = sum of per-batch `count=` fields the receiver's transport layer
# logs (ipc_unix.rs), over the sampling window.
#
# Deliberately does NOT use the `metrics` middleware/exporter to measure this:
# it sits on the per-message hot path and was itself once the source of a
# false near-zero reading (see ipc_receiver.yaml for why). `log_level: debug`
# already emits count/bytes per batch with no metrics crate involved — this
# script sums that instead.
#
# Prereqs: cargo build -p mq-bridge-app --no-default-features --features bench --release
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/lib.sh"

RECV_CONFIG="$HERE/ipc_receiver.yaml"
SEND_CONFIG="$HERE/ipc_sender.yaml"
RECV_UI="${RECV_UI:-127.0.0.1:9097}"
SEND_UI="${SEND_UI:-127.0.0.1:9099}"
RUN_SECONDS="${RUN_SECONDS:-15}"       # measured window, after a short warmup
SOCK_DIR="${SOCK_DIR:-/tmp/mqbipc}"    # short, privately-owned path (SUN_LEN safe)

RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
mkdir -p "$RESULTS_DIR" "$SOCK_DIR"
RECV_LOG="$RESULTS_DIR/ipc_recv.log"
SEND_LOG="$RESULTS_DIR/ipc_send.log"
CSV="$RESULTS_DIR/ipc_throughput.csv"
echo "run_seconds,processed_start,processed_end,rows_per_s" > "$CSV"
require_bin

RECV_PID="" SEND_PID=""
trap 'kill_pids "$SEND_PID" "$RECV_PID"' EXIT

# Sum of per-batch `count=` fields from the receiver's own debug log. Strips
# ANSI color codes first — tracing-subscriber emits them even to this
# file-redirected stream, which would otherwise split "count=1024" across
# escape sequences and silently break the match.
processed_count() {
  { sed -E 's/\x1b\[[0-9;]*[A-Za-z]//g' "$RECV_LOG" 2>/dev/null || true; } \
    | { grep -o 'Received batch via Unix IPC.*count=[0-9]*' || true; } \
    | { grep -o 'count=[0-9]*' || true; } | awk -F= '{s+=$2} END {print int(s)+0}'
}

# A silent reconnect loop (route error -> sleep -> fresh connect, repeat)
# looks identical to a real stall: near-zero throughput, no crash. Fail loudly
# instead of reporting a bogus number.
check_no_reconnect_churn() {
  if grep -qiE 'reconnect|publisher error|connection error' "$RECV_LOG" "$SEND_LOG" 2>/dev/null; then
    echo "ERROR: reconnect/error churn detected — throughput number would be bogus. See $RECV_LOG / $SEND_LOG" >&2
    grep -inE 'reconnect|publisher error|connection error' "$RECV_LOG" "$SEND_LOG" >&2 || true
    return 1
  fi
}

rm -f "$SOCK_DIR"/*.sock
RECV_PID="$(start_app "$RECV_CONFIG" "$RECV_LOG")"
wait_health "$RECV_UI" "$RECV_LOG"
# The receiver must be listening before the sender boots, or the sender's first
# connect finds no socket and its start is rejected.
start_consumer "$RECV_UI" ipc_recv
SEND_PID="$(start_app "$SEND_CONFIG" "$SEND_LOG")"
wait_health "$SEND_UI" "$SEND_LOG"

start_consumer "$SEND_UI" ipc_send
sleep 2   # let both routes reach steady state before sampling

start_count="$(processed_count)"
sleep "$RUN_SECONDS"
end_count="$(processed_count)"

kill_pids "$SEND_PID" "$RECV_PID"
wait "$SEND_PID" "$RECV_PID" 2>/dev/null || true
SEND_PID="" RECV_PID=""

check_no_reconnect_churn

delta=$(( end_count - start_count ))
rate=$(( delta / RUN_SECONDS ))
printf '%s,%s,%s,%s\n' "$RUN_SECONDS" "$start_count" "$end_count" "$rate" | tee -a "$CSV"
echo "done -> $CSV"
