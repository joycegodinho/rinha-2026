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
payload_c='{"id":"warmup-kd-a","transaction":{"amount":2256.89,"installments":9,"requested_at":"2029-01-15T05:52:50Z"},"customer":{"avg_amount":115.84,"tx_count_24h":11,"known_merchants":["MERC-015","MERC-019","MERC-017","MERC-005","MERC-012"]},"merchant":{"id":"MERC-084","mcc":"7802","avg_amount":39.93},"terminal":{"is_online":true,"card_present":false,"km_from_home":379.2883139521},"last_transaction":{"timestamp":"2027-09-26T15:34:18Z","km_from_current":325.2984537103}}'
payload_d='{"id":"warmup-kd-b","transaction":{"amount":2384.5,"installments":6,"requested_at":"2029-04-20T05:43:00Z"},"customer":{"avg_amount":145.07,"tx_count_24h":14,"known_merchants":["MERC-012","MERC-018","MERC-010","MERC-001","MERC-008"]},"merchant":{"id":"MERC-056","mcc":"7802","avg_amount":45.95},"terminal":{"is_online":true,"card_present":false,"km_from_home":204.4922245677},"last_transaction":{"timestamp":"2026-01-22T23:01:21Z","km_from_current":392.8103693279}}'
payload_e='{"id":"warmup-kd-c","transaction":{"amount":3646.62,"installments":7,"requested_at":"2027-12-13T01:17:19Z"},"customer":{"avg_amount":278.85,"tx_count_24h":8,"known_merchants":["MERC-019","MERC-013","MERC-001"]},"merchant":{"id":"MERC-061","mcc":"7995","avg_amount":68.05},"terminal":{"is_online":false,"card_present":true,"km_from_home":360.8480852472},"last_transaction":{"timestamp":"2027-08-23T12:13:09Z","km_from_current":335.6213644463}}'
payload_f='{"id":"warmup-kd-d","transaction":{"amount":2249.68,"installments":6,"requested_at":"2030-08-23T03:59:45Z"},"customer":{"avg_amount":155.83,"tx_count_24h":9,"known_merchants":["MERC-019","MERC-007","MERC-004","MERC-008","MERC-018"]},"merchant":{"id":"MERC-079","mcc":"7802","avg_amount":57.12},"terminal":{"is_online":true,"card_present":false,"km_from_home":343.3318304232},"last_transaction":{"timestamp":"2022-12-20T09:14:21Z","km_from_current":471.4121269694}}'
payload_g='{"id":"warmup-kd-e","transaction":{"amount":2384.42,"installments":8,"requested_at":"2027-08-18T02:26:10Z"},"customer":{"avg_amount":161.76,"tx_count_24h":11,"known_merchants":["MERC-007","MERC-005"]},"merchant":{"id":"MERC-076","mcc":"7802","avg_amount":74.48},"terminal":{"is_online":true,"card_present":false,"km_from_home":307.5128203508},"last_transaction":{"timestamp":"2022-09-29T15:45:03Z","km_from_current":310.8236789961}}'
payload_h='{"id":"warmup-kd-f","transaction":{"amount":3143.85,"installments":8,"requested_at":"2030-10-02T03:09:56Z"},"customer":{"avg_amount":50.31,"tx_count_24h":12,"known_merchants":["MERC-006","MERC-001","MERC-001","MERC-013","MERC-019"]},"merchant":{"id":"MERC-053","mcc":"7802","avg_amount":94.12},"terminal":{"is_online":true,"card_present":false,"km_from_home":298.9915713898},"last_transaction":{"timestamp":"2029-01-26T14:31:15Z","km_from_current":212.5480622082}}'

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
  case $((i % 8)) in
    1) body="$payload_a" ;;
    2) body="$payload_b" ;;
    3) body="$payload_c" ;;
    4) body="$payload_d" ;;
    5) body="$payload_e" ;;
    6) body="$payload_f" ;;
    7) body="$payload_g" ;;
    *) body="$payload_h" ;;
  esac
  if ! try_post "$body"; then
    echo "warmup POST failed after ${POST_RETRIES} attempts on round ${i}" >&2
    exit 52
  fi
  i=$((i + 1))
done

echo "warmup complete"
