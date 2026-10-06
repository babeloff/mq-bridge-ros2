#!/usr/bin/env bash
# Smoke test run by CI: commits and rolls back transactions, checks what reaches Kafka.
set -euo pipefail
cd "$(dirname "$0")"

psql() { docker compose exec -T postgres psql -U app -d app -q -v ON_ERROR_STOP=1 "$@"; }
events() { ./consume.sh | jq -rR 'split(" ")[1:] | join(" ") | fromjson | .event_type' | sort -u | paste -sd, -; }
wait_for() { # wait_for <description> <expected event types>
  for _ in $(seq 1 10); do
    if [ "$(events)" = "$2" ]; then echo "ok: $1"; return; fi
    sleep 2
  done
  echo "FAILED: $1"; echo "  expected: $2"; echo "  got:      $(events)"
  docker compose logs --tail 50 mq-bridge
  exit 1
}

trap 'docker compose down -v >/dev/null 2>&1' EXIT
docker compose up -d --wait

# The relay only sees changes made after its replication slot exists.
until [ "$(echo "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'outbox_relay' AND active" | psql -At)" = "1" ]; do sleep 1; done

psql <<'SQL'
BEGIN;
INSERT INTO orders (sku, qty) VALUES ('book-42', 2);
INSERT INTO outbox (aggregate_id, event_type, payload)
VALUES ('order-1', 'order.placed', '{"order_id": 1, "sku": "book-42", "qty": 2}');
COMMIT;

BEGIN;
INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ('order-1', 'order.never_happened', '{}');
ROLLBACK;

DELETE FROM outbox;
SQL
wait_for "a committed event is relayed; a rolled-back one and the cleanup delete are not" "order.placed"

docker compose stop mq-bridge
echo "INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ('order-1', 'order.paid', '{\"order_id\": 1}');" | psql
docker compose start mq-bridge
wait_for "an event written while the relay was down arrives after a restart" "order.paid,order.placed"

keys=$(./consume.sh | cut -d' ' -f1 | sort -u | paste -sd, -)
if [ "$keys" = "order-1" ]; then
  echo "ok: the Kafka key is the aggregate id"
else
  echo "FAILED: keys were: $keys"
  exit 1
fi
