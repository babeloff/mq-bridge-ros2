#!/usr/bin/env bash
# Postgres -> file, against the tools that ship *with Postgres*.
#
#   ./seed.sh up && ./seed.sh bench 1000000   # prereq: the 1M-row `bench` table
#   ./run_pg_vendor.sh                        # -> results/pg_vendor.csv
#
# Why these tools, and which comparison is fair:
#
#   psql \copy TO CSV  — LIKE-FOR-LIKE. Both sides read the same table and write
#                        the same CSV bytes (the runner diffs them). This is the
#                        honest "how do you compare to what I already run?" number.
#   pg_dump            — REFERENCE FLOOR, NOT a head-to-head. It writes a *restore
#                        format* (a COPY-block SQL script), not interchange data,
#                        and it never decodes a row: the server streams COPY text
#                        and pg_dump forwards it. It is the cost of getting bytes
#                        out of Postgres at all, so it belongs in the table as the
#                        lower bound on any extraction — not as a competitor that
#                        was beaten or lost to.
#
# Deliberately NOT compared: pg_dump's custom/directory formats (compressed,
# parallel, restore-only) and `--data-only` row counts. Publishing a ratio against
# a dump format would be comparing different work and inviting the obvious
# objection.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/lib.sh"

ROWS="${ROWS:-1000000}"
REPEATS="${REPEATS:-2}"
RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
RESULTS_CSV="${RESULTS_CSV:-$RESULTS_DIR/pg_vendor.csv}"

OUT_MQB="${OUT_MQB:-/tmp/mqb_pg_vendor.csv}"
OUT_PSQL="${OUT_PSQL:-/tmp/psql_pg_vendor.csv}"
OUT_DUMP="${OUT_DUMP:-/tmp/pg_dump_vendor.sql}"
OUT_NORMAL="${OUT_NORMAL:-/tmp/mqb_pg_vendor_normal.jsonl}"
OUT_ZSTD="${OUT_ZSTD:-/tmp/mqb_pg_vendor_normal.zst}"
OUT_LZ4="${OUT_LZ4:-/tmp/mqb_pg_vendor_normal.lz4}"
OUT_JSON="${OUT_JSON:-/tmp/mqb_pg_vendor_json.jsonl}"
OUT_TEXT="${OUT_TEXT:-/tmp/mqb_pg_vendor_text.jsonl}"
OUT_RAW="${OUT_RAW:-/tmp/mqb_pg_vendor_raw.jsonl}"
OUT_RESTORE="${OUT_RESTORE:-/tmp/mqb_pg_vendor_restored.jsonl}"
OUT_DECODED="${OUT_DECODED:-/tmp/mqb_pg_vendor_decoded.jsonl}"

# Reader tuning, overridable so the same script can produce both the
# comparable-with-the-other-scenarios number and a tuned one. The defaults match
# scenarios 5-7 (batch 1024, concurrency 1) rather than the product's own defaults
# (batch 1024, concurrency 4); see the README note on parameters.
BATCH="${BATCH:-1024}"
CONC="${CONC:-1}"

PG_TIMEOUT="${PG_TIMEOUT:-$(guard_budget 0.0004 60)}"   # ~400s at 1M rows

require_bin

# A HOST psql/pg_dump is mandatory here, unlike everywhere else in the harness.
# lib.sh's psql_q falls back to running psql *inside* the compose container, which
# would silently invalidate this scenario in two ways at once: `\copy` writes
# client-side, so the CSV would land on container disk (overlayfs) instead of the
# host's, and the client would reach the server over a container-local socket
# rather than the TCP connection mq-bridge-app uses. That is a different disk and a
# different network path — a faster number, and not a comparison.
#
# On macOS: brew install libpq (keg-only, no server, no service) and put
# $(brew --prefix libpq)/bin on PATH.
for tool in psql pg_dump; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "no host ${tool} on PATH — this scenario cannot use the container client:" >&2
    echo "  \\copy would write to container disk over a container-local socket," >&2
    echo "  while mq-bridge-app writes host disk over TCP. Not comparable." >&2
    echo "  macOS: brew install libpq && export PATH=\"\$(brew --prefix libpq)/bin:\$PATH\"" >&2
    exit 1
  }
done

wait_for_pg

have="$(psql_q -c 'SELECT count(*) FROM bench;' 2>/dev/null || echo 0)"
[[ "$have" == "$ROWS" ]] || {
  echo "the \`bench\` table has ${have} rows, expected ${ROWS} — run: ./seed.sh bench ${ROWS}" >&2
  exit 1
}

print_env_header
echo

# --- mq-bridge-app: table -> CSV file -----------------------------------------
mqb_once() {
  rm -f "$OUT_MQB"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_MQB}?format=csv&csv=%7B%22nested%22%3A%22json%22%7D" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

# --- mq-bridge-app: the same table -> its own on-disk formats ------------------
# NOT a vendor head-to-head — psql/pg_dump cannot write these. This block answers
# "what does it cost to land the data in a form mq-bridge can read back?", which is
# the interchange/backup use case rather than the CSV export one.
#
# `normal` is the default format: the whole CanonicalMessage as JSON, with a UTF-8
# payload written as a plain string and only a non-UTF-8 one base64-encoded into a
# separate `payload_base64` field. The envelope is why the file is ~2.5x the CSV.
# The three cells below measure the alternatives: `json` renders the payload as a
# JSON value, `text` as a string, `raw` writes the payload alone (no envelope, so
# no message_id). All four read back.
normal_once() {
  rm -f "$OUT_NORMAL"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_NORMAL}?format=normal" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

json_once() {
  rm -f "$OUT_JSON"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_JSON}?format=json" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

text_once() {
  rm -f "$OUT_TEXT"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_TEXT}?format=text" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

raw_once() {
  rm -f "$OUT_RAW"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_RAW}?format=raw" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

# CSV cannot be combined with compression (the endpoint rejects it), so the
# compressed cell has to be `normal` — which is the right pairing anyway, since
# compression is for the at-rest/interchange case.
zstd_once() {
  rm -f "$OUT_ZSTD"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_ZSTD}?format=normal&compression=zstd" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

# lz4: the same format, the cheaper codec. Worth measuring separately as a check on
# the ratio/CPU trade — it buys ~1.7x worse compression for a cheaper compress.
# Historical note: this cell used to show compression costing ~36% at concurrency 4,
# which read like "the compressor is the bottleneck". It was not: the member write
# path ran its encode+compress before its first await, so it held the thread that
# had woken it and stalled the route's producer (~1.9ms dead time per batch, flat
# in concurrency). Fixed by opening the file before the build; both codecs now land
# within a few percent of uncompressed.
lz4_once() {
  rm -f "$OUT_LZ4"
  "$BIN" copy \
    --from "${PG_URL}?table=bench&cursor_column=id&sslmode=disable" \
    --to "file://${OUT_LZ4}?format=normal&compression=lz4" \
    --drain --batch-size "$BATCH" --concurrency "$CONC"
}

# Row counts for the compressed cells. Counting lines in a .zst/.lz4 would be
# nonsense, so decode with the *external* CLI rather than with mq-bridge —
# verifying a writer with its own reader would hide a bug in both. This doubles as
# a check on the documented "concatenated frames decode as one stream" property.
zstd_rows() {
  local p="$1"
  [[ -f "$p" ]] || { echo 0; return; }
  zstd -dc "$p" 2>/dev/null | wc -l | tr -d ' '
}

# Verified against lz4 v1.10.0: a plain `-dc` decodes straight through concatenated
# frames and yields all $ROWS lines, so no extra flag is needed here.
lz4_rows() {
  local p="$1"
  [[ -f "$p" ]] || { echo 0; return; }
  lz4 -dc "$p" 2>/dev/null | wc -l | tr -d ' '
}

# --- psql \copy: the same table, the same CSV ---------------------------------
# The projection looks fussy, and each part of it is load-bearing for the parity
# check. mq-bridge-app's CSV sink serializes each row from a JSON object, so it
# emits columns in source order and renders booleans as `true`/`false`;
# Postgres renders them as `t`/`f`. So `active` is cast to text, which makes the two outputs
# byte-identical without changing the work either side does — only how the
# baseline presents it. `ORDER BY id` gives both a deterministic row order
# (mq-bridge-app reads via `cursor_column=id`), and HEADER matches its header line.
psql_once() {
  rm -f "$OUT_PSQL"
  psql_q -c "\\copy (SELECT id, first_name, country, amount, created_at, active::text AS active, attributes FROM bench ORDER BY id) TO '${OUT_PSQL}' WITH (FORMAT csv, HEADER true)"
}

# --- pg_dump: reference floor -------------------------------------------------
# Data only, one table, plain format — the leanest thing pg_dump can be asked to
# do, so the floor is not inflated by schema/index work.
dump_once() {
  rm -f "$OUT_DUMP"
  if command -v pg_dump >/dev/null 2>&1; then
    pg_dump "$PG_URL" --data-only --table=bench --format=plain --file="$OUT_DUMP"
  else
    docker exec -i "$PG_CONTAINER" pg_dump -U "$PGUSER" -d "$PGDATABASE" \
      --data-only --table=bench --format=plain > "$OUT_DUMP"
  fi
}

# Both CSV writers emit a header line on top of the data rows.
EXPECT_LINES=$((ROWS + 1))
bench_tool "mq-bridge-app" "$OUT_MQB" "$PG_TIMEOUT" mqb_once || exit 1
bench_tool "psql-copy" "$OUT_PSQL" "$PG_TIMEOUT" psql_once || exit 1

# A rate is only worth publishing if both sides emitted the same data. This used to
# be a byte comparison (`cmp`), which is stricter than the claim being made and
# fails for a reason that has nothing to do with correctness: a `double precision`
# holding a whole number renders as `7535.0` here (the sink serializes an f64) and
# as `7535` in Postgres. Same value, same column, different spelling.
#
# The honest fix is to compare values rather than bytes — numeric fields as
# numbers, everything else exactly. Making the check pass the other way (casting
# the baseline with to_char, or reseeding to avoid whole-valued floats) would be
# shaping the evidence to fit the claim.
echo "-- parity: mq-bridge-app CSV vs psql \\copy CSV"
# `|| parity=$?` and not a bare call: `set -e` is in force, so an unguarded failing
# check would abort the script here and the later cells would silently never run —
# which is exactly how pg_dump went missing from an earlier result set. A failed
# parity check must be loud, not fatal.
parity=0
python3 - "$OUT_MQB" "$OUT_PSQL" <<'PY' || parity=$?
import csv, sys

def same(x, y):
    if x == y:
        return True
    try:
        return float(x) == float(y)   # 7535.0 == 7535
    except ValueError:
        return False

with open(sys.argv[1], newline="") as fa, open(sys.argv[2], newline="") as fb:
    a, b = csv.reader(fa), csv.reader(fb)
    ha, hb = next(a), next(b)
    if ha != hb:
        print(f"   DIFFER — headers: {ha} vs {hb}", file=sys.stderr)
        sys.exit(1)
    n = numeric = 0
    for n, (ra, rb) in enumerate(zip(a, b), 1):
        if ra == rb:
            continue
        if len(ra) != len(rb) or not all(same(x, y) for x, y in zip(ra, rb)):
            bad = next((c for c, (x, y) in enumerate(zip(ra, rb)) if not same(x, y)), None)
            print(f"   DIFFER at row {n}, column '{ha[bad] if bad is not None else '?'}':",
                  file=sys.stderr)
            print(f"     mq-bridge-app: {ra}", file=sys.stderr)
            print(f"     psql \\copy   : {rb}", file=sys.stderr)
            sys.exit(1)
        numeric += 1
    extra = sum(1 for _ in a) + sum(1 for _ in b)
    if extra:
        print(f"   DIFFER — one file has {extra} extra row(s)", file=sys.stderr)
        sys.exit(1)
    print(f"   {n} rows equal ({numeric} differ only in numeric formatting, "
          f"e.g. 7535.0 vs 7535)")
PY
[[ $parity -eq 0 ]] || echo "   (fix the sink config before publishing the ratio)" >&2

# --- mq-bridge-app's own formats ----------------------------------------------
# One record per line, no header, so the plain row count applies here.
command -v zstd >/dev/null 2>&1 || {
  echo "no zstd on PATH — needed to verify the compressed cell (brew install zstd)" >&2
  exit 1
}
command -v lz4 >/dev/null 2>&1 || {
  echo "no lz4 on PATH — needed to verify the lz4 cell (brew install lz4)" >&2
  exit 1
}
EXPECT_LINES="$ROWS"
bench_tool "mq-bridge-app-normal" "$OUT_NORMAL" "$PG_TIMEOUT" normal_once || exit 1
bench_tool "mq-bridge-app-json" "$OUT_JSON" "$PG_TIMEOUT" json_once || exit 1
bench_tool "mq-bridge-app-text" "$OUT_TEXT" "$PG_TIMEOUT" text_once || exit 1
bench_tool "mq-bridge-app-raw" "$OUT_RAW" "$PG_TIMEOUT" raw_once || exit 1
LANDED_FN=zstd_rows bench_tool "mq-bridge-app-normal-zstd" "$OUT_ZSTD" "$PG_TIMEOUT" zstd_once || exit 1
LANDED_FN=lz4_rows bench_tool "mq-bridge-app-normal-lz4" "$OUT_LZ4" "$PG_TIMEOUT" lz4_once || exit 1

echo "-- on-disk size"
printf '   %-28s %s bytes\n' "csv" "$(wc -c < "$OUT_MQB" | tr -d ' ')"
printf '   %-28s %s bytes\n' "normal" "$(wc -c < "$OUT_NORMAL" | tr -d ' ')"
printf '   %-28s %s bytes\n' "json" "$(wc -c < "$OUT_JSON" | tr -d ' ')"
printf '   %-28s %s bytes\n' "text" "$(wc -c < "$OUT_TEXT" | tr -d ' ')"
printf '   %-28s %s bytes\n' "raw" "$(wc -c < "$OUT_RAW" | tr -d ' ')"
printf '   %-28s %s bytes (%.1fx smaller than normal)\n' "normal+zstd" \
  "$(wc -c < "$OUT_ZSTD" | tr -d ' ')" \
  "$(python3 -c "print($(wc -c < "$OUT_NORMAL")/$(wc -c < "$OUT_ZSTD"))")"
printf '   %-28s %s bytes (%.1fx smaller than normal)\n' "normal+lz4" \
  "$(wc -c < "$OUT_LZ4" | tr -d ' ')" \
  "$(python3 -c "print($(wc -c < "$OUT_NORMAL")/$(wc -c < "$OUT_LZ4"))")"

# The point of an internal format is that it comes back. Restore the compressed
# file through mq-bridge and compare it against the SAME file decoded by the
# external zstd CLI.
#
# It must not be compared against $OUT_NORMAL, which is the obvious mistake and one
# I made: that file is a separate read of Postgres, and Postgres rows carry no
# message_id, so the sink mints a fresh one per ingestion. Two independent reads
# therefore disagree on message_id by construction, which looks exactly like "the
# sink regenerates ids on write" and is not. Comparing the restore against its own
# source keeps the round trip the only variable.
echo "-- restore: zstd file -> normal, via mq-bridge-app"
rm -f "$OUT_RESTORE"
restore_t0="$(now)"
# Under the watchdog like every other cell: a wedged restore must be killed, not
# left to hang the run (run_guarded already routes stdio to /dev/null).
run_guarded "$PG_TIMEOUT" "$BIN" copy --from "file://${OUT_ZSTD}?format=normal&compression=zstd" \
  --to "file://${OUT_RESTORE}?format=normal" \
  --drain --batch-size "$BATCH" --concurrency "$CONC" || {
  echo "   restore FAILED" >&2; exit 1; }
restore_t1="$(now)"
restored="$(landed_rows "$OUT_RESTORE")"
[[ "$restored" == "$ROWS" ]] || { echo "   restored ${restored} rows, expected ${ROWS}" >&2; exit 1; }
# A plain byte comparison is the right check here, and it must include
# `message_id`: the round trip is only lossless if identity survives it.
zstd -dc "$OUT_ZSTD" > "$OUT_DECODED" 2>/dev/null
if cmp -s "$OUT_DECODED" "$OUT_RESTORE"; then
  echo "   round trip byte-identical, message_id included ($(wc -l < "$OUT_RESTORE" | tr -d ' ') records)"
else
  echo "   restore MISMATCH — the round trip is not lossless:" >&2
  python3 - "$OUT_DECODED" "$OUT_RESTORE" >&2 <<'PY' || true
import json, sys
for i, (a, b) in enumerate(zip(open(sys.argv[1]), open(sys.argv[2])), 1):
    if a == b:
        continue
    x, y = json.loads(a), json.loads(b)
    xi, yi = x.pop("message_id", None), y.pop("message_id", None)
    print(f"   first difference at record {i}"
          f"{' — message_id only' if x == y and xi != yi else ''}")
    print(f"     source  : {a.rstrip()[:160]}")
    print(f"     restored: {b.rstrip()[:160]}")
    break
PY
  exit 1
fi
python3 -c "print(f'   restore of {$ROWS} rows took {$restore_t1-$restore_t0:.3f}s')"

# The dump writes a SQL script, so its line count is not a row count; count the
# COPY-block data lines instead (every one starts with the integer id column).
# Runs through the shared bench_tool flow like every other cell — same warmup +
# repeats, watchdog, row-count validation (landed == $ROWS) and results row.
dump_rows() {
  local p="$1"
  [[ -f "$p" ]] || { echo 0; return; }
  grep -cE '^[0-9]+\t' "$p" || true
}
echo "-- pg_dump (reference floor, restore format — not a head-to-head)"
LANDED_FN=dump_rows bench_tool "pg_dump" "$OUT_DUMP" "$PG_TIMEOUT" dump_once || exit 1

rm -f "$OUT_MQB" "$OUT_PSQL" "$OUT_DUMP" "$OUT_NORMAL" "$OUT_ZSTD" "$OUT_LZ4" \
  "$OUT_JSON" "$OUT_TEXT" "$OUT_RAW" \
  "$OUT_RESTORE" "$OUT_DECODED"
echo "done -> $RESULTS_CSV"
