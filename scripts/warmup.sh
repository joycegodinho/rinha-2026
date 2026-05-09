#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:9999}"
READY_URL="${BASE_URL}/ready"
SCORE_URL="${BASE_URL}/fraud-score"
READY_RETRIES="${READY_RETRIES:-120}"
READY_SLEEP="${READY_SLEEP:-0.25}"
WARMUP_ROUNDS="${WARMUP_ROUNDS:-48}"

payload_a='{"id":"warmup-a","transaction":{"amount":384.88,"installments":3,"requested_at":"2026-03-11T20:23:35Z"},"customer":{"avg_amount":769.76,"tx_count_24h":3,"known_merchants":["MERC-009","MERC-001","MERC-001"]},"merchant":{"id":"MERC-001","mcc":"5912","avg_amount":298.95},"terminal":{"is_online":false,"card_present":true,"km_from_home":13.7090520965},"last_transaction":{"timestamp":"2026-03-11T14:58:35Z","km_from_current":18.8626479774}}'
payload_b='{"id":"warmup-b","transaction":{"amount":2911.41,"installments":12,"requested_at":"2026-03-19T02:17:11Z"},"customer":{"avg_amount":411.03,"tx_count_24h":8,"known_merchants":["MERC-221","MERC-010"]},"merchant":{"id":"MERC-551","mcc":"6011","avg_amount":712.22},"terminal":{"is_online":true,"card_present":false,"km_from_home":2.18},"last_transaction":{"timestamp":"2026-03-18T23:51:05Z","km_from_current":1.34}}'

echo "warming up stack via ${BASE_URL}"

for ((i = 1; i <= READY_RETRIES; i++)); do
  if curl -fsS --max-time 1 "${READY_URL}" >/dev/null; then
    break
  fi
  if [[ "${i}" -eq "${READY_RETRIES}" ]]; then
    echo "ready check failed after ${READY_RETRIES} attempts" >&2
    exit 1
  fi
  sleep "${READY_SLEEP}"
done

for ((i = 1; i <= WARMUP_ROUNDS; i++)); do
  if (( i % 2 == 0 )); then
    body="${payload_a}"
  else
    body="${payload_b}"
  fi
  curl -fsS \
    --max-time 2 \
    -H 'content-type: application/json' \
    -d "${body}" \
    "${SCORE_URL}" >/dev/null
done

echo "warmup complete"
