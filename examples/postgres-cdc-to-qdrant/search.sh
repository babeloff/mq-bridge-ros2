#!/usr/bin/env bash
# Semantic search over the synced collection. Usage: ./search.sh "how long do refunds take"
set -euo pipefail

vector=$(curl -sS --fail http://localhost:11434/v1/embeddings \
  -d "$(jq -n --arg q "$1" '{model: "all-minilm", input: $q}')" | jq -c '.data[0].embedding')

curl -sS --fail http://localhost:6333/collections/docs/points/query \
  -H 'Content-Type: application/json' \
  -d "{\"query\": $vector, \"limit\": 3, \"with_payload\": true}" |
  jq -r '.result.points[] | "\(.score | tostring | .[0:5])  \(.payload.title): \(.payload.body)"'
