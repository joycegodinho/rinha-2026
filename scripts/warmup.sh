#!/bin/sh
set -eu

BASE_URL="${BASE_URL:-http://localhost:9999}"
READY_URL="${BASE_URL}/ready"
SCORE_URL="${BASE_URL}/fraud-score"
READY_RETRIES="${READY_RETRIES:-120}"
READY_SLEEP="${READY_SLEEP:-0.25}"
WARMUP_ROUNDS="${WARMUP_ROUNDS:-48}"
POST_RETRIES="${POST_RETRIES:-40}"
POST_SLEEP="${POST_SLEEP:-0.10}"

payload_a='{"id":"warmup-a","transaction":{"amount":384.88,"installments":3,"requested_at":"2026-03-11T20:23:35Z"},"customer":{"avg_amount":769.76,"tx_count_24h":3,"known_merchants":["MERC-009","MERC-001","MERC-001"]},"merchant":{"id":"MERC-001","mcc":"5912","avg_amount":298.95},"terminal":{"is_online":false,"card_present":true,"km_from_home":13.7090520965},"last_transaction":{"timestamp":"2026-03-11T14:58:35Z","km_from_current":18.8626479774}}'
payload_b='{"id":"warmup-b","transaction":{"amount":2911.41,"installments":12,"requested_at":"2026-03-19T02:17:11Z"},"customer":{"avg_amount":411.03,"tx_count_24h":8,"known_merchants":["MERC-221","MERC-010"]},"merchant":{"id":"MERC-551","mcc":"6011","avg_amount":712.22},"terminal":{"is_online":true,"card_present":false,"km_from_home":2.18},"last_transaction":{"timestamp":"2026-03-18T23:51:05Z","km_from_current":1.34}}'

echo "warming up stack via ${BASE_URL}"

try_post() {
  body="$1"
  attempt=1
  while [ "$attempt" -le "$POST_RETRIES" ]; do
    if curl -fsS       --max-time 2       -H 'content-type: application/json'       -d "$body"       "$SCORE_URL" >/dev/null; then
      return 0
    fi
    if [ "$attempt" -eq "$POST_RETRIES" ]; then
      return 1
    fi
    sleep "$POST_SLEEP"
    attempt=$((attempt + 1))
  done
  return 1
}

i=1
while [ "$i" -le "$READY_RETRIES" ]; do
  if curl -fsS --max-time 1 "$READY_URL" >/dev/null; then
    break
  fi
  if [ "$i" -eq "$READY_RETRIES" ]; then
    echo "ready check failed after ${READY_RETRIES} attempts" >&2
    exit 1
  fi
  sleep "$READY_SLEEP"
  i=$((i + 1))
done

i=1
while [ "$i" -le "$WARMUP_ROUNDS" ]; do
  if [ $((i % 2)) -eq 0 ]; then
    body="$payload_a"
  else
    body="$payload_b"
  fi
  if ! try_post "$body"; then
    echo "warmup POST failed after ${POST_RETRIES} attempts on round ${i}" >&2
    exit 52
  fi
  i=$((i + 1))
done

echo "warmup complete"
