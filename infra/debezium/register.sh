#!/usr/bin/env bash
# Debezium 커넥터 등록: aggregatetype='payment' 인 outbox 행 → 'payment.events' 토픽
set -euo pipefail
cd "$(dirname "$0")"
curl -fsS -X POST -H 'Content-Type: application/json' \
  --data @outbox-connector.json http://localhost:8083/connectors | jq .
