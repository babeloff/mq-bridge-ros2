#!/usr/bin/env bash
# Scenario 2 — CDC end-to-end latency via the zero-code config path.
#
# Starts the app on cdc_latency.yaml (a postgres_cdc -> file route), starts the
# route with POST /consumer-start, then lets cdc_latency.py commit single-row
# transactions at a fixed rate and time each one from the acknowledged COMMIT to
# its line landing in the sink file. Reports p50/p95/p99 per payload size and rate.
#
# Prereqs:  ./seed.sh up, `uv` on PATH, and a build with CDC — the default build, or
#           cargo build -p mq-bridge-app --no-default-features --features bench-cdc --release
#           (`bench-cdc`, not `bench` — CDC needs the postgres logical-replication
#           endpoint, which plain `bench` leaves out.)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/seed.sh"   # sources lib.sh

CONFIG="$HERE/cdc_latency.yaml"
UI_ADDR="${UI_ADDR:-127.0.0.1:9091}"
CDC_TABLE="${CDC_TABLE:-cdc_src}"
CDC_PUB="${CDC_PUB:-mqb_pub}"
PAYLOADS="${PAYLOADS:-256 4096}"
RATES="${RATES:-200 1000}"               # commits per second (open loop)
WINDOW_SECONDS="${WINDOW_SECONDS:-20}"   # measured window per cell
SINK="/tmp/mqb_cdc_out.jsonl"            # must match cdc_latency.yaml
RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
mkdir -p "$RESULTS_DIR"
CSV="$RESULTS_DIR/cdc_latency.csv"
echo "payload_bytes,events,commits_per_s,commit_ms,ack_p50_ms,ack_p95_ms,ack_p99_ms,send_p50_ms,send_p95_ms,send_p99_ms,send_max_ms" > "$CSV"

require_bin
command -v uv >/dev/null 2>&1 || { echo "uv not found — cdc_latency.py needs it for psycopg" >&2; exit 1; }

APP_PID=""
trap 'kill_pids "$APP_PID"; rm -f "$SINK"' EXIT

wait_for_pg
seed_cdc "$CDC_TABLE" "$CDC_PUB"
: > "$SINK"
APP_PID="$(start_app "$CONFIG" "$RESULTS_DIR/cdc_app.log")"
wait_health "$UI_ADDR" "$RESULTS_DIR/cdc_app.log"
# --config loads the route but does not start it; this is the UI's Start button.
start_consumer "$UI_ADDR" cdc_lat
sleep 3

for bytes in $PAYLOADS; do
  for rate in $RATES; do
    echo "-- cdc latency ${bytes}B at ${rate} commits/s"
    uv run -q "$HERE/cdc_latency.py" --pg-url "$PG_URL" --table "$CDC_TABLE" --sink "$SINK" \
      --bytes "$bytes" --rate "$rate" --count "$((rate * WINDOW_SECONDS))" --warmup "$((rate * 2))" \
      | tee -a "$CSV"
  done
done
echo "done -> $CSV"
