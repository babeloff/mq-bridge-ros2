#!/usr/bin/env bash
# Smoke test run by CI: starts the stack, changes rows, checks Qdrant follows.
set -euo pipefail
cd "$(dirname "$0")"

psql() { docker compose exec -T postgres psql -U app -d app -qc "$1"; }
bodies() {
  curl -sS http://localhost:6333/collections/docs/points/scroll \
    -H 'Content-Type: application/json' \
    -d '{"limit": 100, "with_payload": true}' | jq -r '[.result.points[].payload.body] | sort | join("|")'
}
wait_for() { # wait_for <description> <expected bodies>
  for _ in $(seq 1 120); do
    if [ "$(bodies 2>/dev/null)" = "$2" ]; then echo "ok: $1"; return; fi
    sleep 2
  done
  echo "FAILED: $1"; echo "  expected: $2"; echo "  got:      $(bodies)"
  docker compose logs --tail 50 mq-bridge setup
  exit 1
}

trap 'docker compose down -v >/dev/null 2>&1' EXIT
docker compose up -d

REFUND='Refunds are issued to the original payment method within 5 business days.'
SHIP='Orders ship from the Rotterdam warehouse and arrive in 2 to 4 days in the EU.'
RESET='Use the "Forgot password" link on the sign-in page to get a reset email.'
wait_for "existing rows are backfilled" "$SHIP|$REFUND|$RESET"

psql "INSERT INTO docs (title, body) VALUES ('Invoices', 'Invoices are emailed as PDF on the first day of each month.')"
psql "UPDATE docs SET body = 'Refunds take 10 business days.' WHERE title = 'Refunds'"
psql "DELETE FROM docs WHERE title = 'Shipping'"
wait_for "insert, update and delete are applied" \
  "Invoices are emailed as PDF on the first day of each month.|Refunds take 10 business days.|$RESET"

docker compose stop mq-bridge
psql "INSERT INTO docs (title, body) VALUES ('Offline', 'A row written while the bridge was stopped.')"
docker compose start mq-bridge
wait_for "changes made while the bridge was down arrive after a restart" \
  "A row written while the bridge was stopped.|Invoices are emailed as PDF on the first day of each month.|Refunds take 10 business days.|$RESET"

top=$(./search.sh "when do I get my money back" | head -1)
case "$top" in *Refunds*) echo "ok: semantic search finds the refund row" ;; *) echo "FAILED: search returned: $top"; exit 1 ;; esac
