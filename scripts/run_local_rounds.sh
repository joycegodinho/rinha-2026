#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.dev.yml}"
ROUNDS="${ROUNDS:-3}"
BUILD_MODE="${BUILD_MODE:-once}"   # once | always | never
KEEP_STACK="${KEEP_STACK:-0}"      # 1 keeps final stack running
TEST_DIR="$ROOT_DIR/test"
RESULTS_DIR="${RESULTS_DIR:-$TEST_DIR/local-runs}"
mkdir -p "$RESULTS_DIR"

compose() {
  docker compose -f "$ROOT_DIR/$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_warmup() {
  local cid status exit_code tries
  tries=0
  while :; do
    cid="$(compose ps -aq warmup 2>/dev/null | head -n 1 || true)"
    if [[ -n "$cid" ]]; then
      status="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)"
      exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null || true)"
      if [[ "$status" == "exited" && "$exit_code" == "0" ]]; then
        return 0
      fi
      if [[ "$status" == "exited" && "$exit_code" != "0" ]]; then
        echo "warmup falhou (exit=$exit_code)" >&2
        compose logs warmup >&2 || true
        return 1
      fi
    fi
    tries=$((tries + 1))
    if [[ "$tries" -ge 240 ]]; then
      echo "timeout esperando warmup" >&2
      compose logs warmup >&2 || true
      return 1
    fi
    sleep 1
  done
}

run_k6() {
  local round="$1"
  local log="$RESULTS_DIR/k6-round-$round.log"
  echo "  warmup ok; rodando k6 (isso costuma levar ~2min)..."
  (cd "$TEST_DIR" && k6 run test.js) | tee "$log"
  echo "  k6 terminou; log salvo em $log"
}

extract_result() {
  local path="$1"
  python3 - "$path" <<'PY2'
import json, sys
from pathlib import Path
p = Path(sys.argv[1])
data = json.loads(p.read_text())
print(data['p99'])
print(data['scoring']['breakdown']['http_errors'])
print(data['scoring']['final_score'])
PY2
}

printf 'Rodadas locais: %s\n' "$ROUNDS"
printf 'Compose: %s\n' "$COMPOSE_FILE"
printf 'Build mode: %s\n\n' "$BUILD_MODE"

for round in $(seq 1 "$ROUNDS"); do
  echo "==> Rodada $round/$ROUNDS"
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  up_args=(up -d)
  if [[ "$BUILD_MODE" == "always" || ( "$BUILD_MODE" == "once" && "$round" -eq 1 ) ]]; then
    up_args+=(--build)
  fi
  compose "${up_args[@]}" >/dev/null
  echo "  stack de pe; esperando warmup..."
  wait_for_warmup
  run_k6 "$round"
  out="$RESULTS_DIR/results-round-$round.json"
  cp "$TEST_DIR/results.json" "$out"
  mapfile -t vals < <(extract_result "$out")
  printf '  p99=%s  http_errors=%s  final_score=%s\n' "${vals[0]}" "${vals[1]}" "${vals[2]}"
done

python3 - "$RESULTS_DIR" "$ROUNDS" <<'PY3'
import json, statistics, sys
from pathlib import Path
results_dir = Path(sys.argv[1])
rounds = int(sys.argv[2])
vals = []
for i in range(1, rounds + 1):
    p = results_dir / f'results-round-{i}.json'
    data = json.loads(p.read_text())
    p99_ms = float(data['p99'].removesuffix('ms'))
    vals.append((i, p99_ms, data['scoring']['breakdown']['http_errors'], data['scoring']['final_score']))
print('\nResumo:')
for i, p99, errs, score in vals:
    print(f'  rodada {i}: p99={p99:.2f}ms  http_errors={errs}  final_score={score}')
p99s = [v[1] for v in vals]
print(f'\n  melhor p99: {min(p99s):.2f}ms')
print(f'  pior p99:   {max(p99s):.2f}ms')
print(f'  media p99:  {statistics.mean(p99s):.2f}ms')
if len(p99s) > 1:
    print(f'  desvio:     {statistics.pstdev(p99s):.2f}ms')
PY3
