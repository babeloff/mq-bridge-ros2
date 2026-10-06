#!/usr/bin/env bash
# Prints every event on the topic as "<key> <value>", then exits after 5 s of silence.
cd "$(dirname "$0")" || exit 1
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic order-events --from-beginning \
  --timeout-ms 5000 --property print.key=true --property key.separator=' ' 2>/dev/null </dev/null
