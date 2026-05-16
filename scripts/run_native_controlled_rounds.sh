#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="$ROOT_DIR/test"
RESULTS_DIR="${RESULTS_DIR:-$TEST_DIR/native-controlled-runs}"
ROUNDS="${ROUNDS:-1}"
BUILD_MODE="${BUILD_MODE:-never}"   # never | once | always
KEEP_STACK="${KEEP_STACK:-1}"       # 1 keeps final stack running for inspection
CLEAN_START="${CLEAN_START:-1}"     # 1 recreates stack before each round
NATIVE_BUILD_IMAGE="${NATIVE_BUILD_IMAGE:-}" # optional image tag to build from service/Dockerfile.native

COMPOSE_FILES_VALUE="${COMPOSE_FILES:-docker-compose.yml docker-compose.native.yml}"
read -r -a COMPOSE_FILES_ARRAY <<<"$COMPOSE_FILES_VALUE"

compose() {
  local args=()
  local f
  for f in "${COMPOSE_FILES_ARRAY[@]}"; do
    args+=(-f "$ROOT_DIR/$f")
  done
  docker compose "${args[@]}" "$@"
}

cleanup() {
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$RESULTS_DIR"

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
    if [[ "$tries" -ge 300 ]]; then
      echo "timeout esperando warmup" >&2
      compose logs warmup >&2 || true
      return 1
    fi
    sleep 1
  done
}

container_for_service() {
  local service="$1"
  compose ps -q "$service" | head -n 1
}

save_cgroup_snapshot() {
  local round="$1"
  local phase="$2"
  local service cid out_dir
  out_dir="$RESULTS_DIR/cgroup-round-$round-$phase"
  mkdir -p "$out_dir"

  for service in service-1 service-2 load-balancer; do
    cid="$(container_for_service "$service")"
    if [[ -z "$cid" ]]; then
      echo "sem container para $service" >"$out_dir/$service.missing"
      continue
    fi
    {
      echo "service=$service"
      echo "container=$cid"
      echo "cpu.max=$(docker exec "$cid" sh -lc 'cat /sys/fs/cgroup/cpu.max' 2>/dev/null || true)"
      echo "--- cpu.stat"
      docker exec "$cid" sh -lc 'cat /sys/fs/cgroup/cpu.stat' 2>/dev/null || true
      echo "--- memory"
      docker exec "$cid" sh -lc 'cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/memory.events' 2>/dev/null || true
    } >"$out_dir/$service.txt"
  done
}

stat_value() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '$1 == key {print $2; found=1} END {if (!found) print 0}' "$file"
}

snapshot_delta() {
  local round="$1"
  local service="$2"
  local before="$RESULTS_DIR/cgroup-round-$round-before/$service.txt"
  local after="$RESULTS_DIR/cgroup-round-$round-after/$service.txt"
  local usage_before usage_after throttled_before throttled_after periods_before periods_after nr_before nr_after

  usage_before="$(stat_value "$before" usage_usec)"
  usage_after="$(stat_value "$after" usage_usec)"
  throttled_before="$(stat_value "$before" throttled_usec)"
  throttled_after="$(stat_value "$after" throttled_usec)"
  periods_before="$(stat_value "$before" nr_periods)"
  periods_after="$(stat_value "$after" nr_periods)"
  nr_before="$(stat_value "$before" nr_throttled)"
  nr_after="$(stat_value "$after" nr_throttled)"

  printf '%s usage_delta=%dus throttled_delta=%dus nr_throttled_delta=%d nr_periods_delta=%d\n' \
    "$service" \
    "$((usage_after - usage_before))" \
    "$((throttled_after - throttled_before))" \
    "$((nr_after - nr_before))" \
    "$((periods_after - periods_before))"
}

run_k6() {
  local round="$1"
  local log="$RESULTS_DIR/k6-round-$round.log"
  echo "  cpu.stat antes salvo; rodando k6..."
  set +e
  (cd "$TEST_DIR" && k6 run test.js) 2>&1 | tee "$log"
  local status=${PIPESTATUS[0]}
  set -e
  echo "  k6 terminou com status=$status; log salvo em $log"
  return "$status"
}

extract_result() {
  local path="$1"
  python3 - "$path" <<'PY2'
import json, sys
from pathlib import Path
p = Path(sys.argv[1])
data = json.loads(p.read_text())
print(data["p99"])
print(data["scoring"]["breakdown"]["http_errors"])
print(data["scoring"]["final_score"])
PY2
}

printf 'Rodadas controladas: %s\n' "$ROUNDS"
printf 'Compose files: %s\n' "$COMPOSE_FILES_VALUE"
printf 'Build mode: %s\n' "$BUILD_MODE"
if [[ -n "$NATIVE_BUILD_IMAGE" ]]; then
  printf 'Native image build: %s\n' "$NATIVE_BUILD_IMAGE"
fi
printf 'Resultados: %s\n\n' "$RESULTS_DIR"

for round in $(seq 1 "$ROUNDS"); do
  echo "==> Rodada $round/$ROUNDS"
  if [[ "$CLEAN_START" == "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi

  up_args=(up -d --force-recreate)
  if [[ "$BUILD_MODE" == "always" || ( "$BUILD_MODE" == "once" && "$round" -eq 1 ) ]]; then
    if [[ -n "$NATIVE_BUILD_IMAGE" ]]; then
      docker build -f "$ROOT_DIR/service/Dockerfile.native" -t "$NATIVE_BUILD_IMAGE" "$ROOT_DIR/service"
    fi
    up_args+=(--build)
  fi
  compose "${up_args[@]}" >/dev/null

  echo "  stack de pe; esperando warmup..."
  wait_for_warmup

  save_cgroup_snapshot "$round" before
  k6_status=0
  run_k6 "$round" || k6_status=$?
  save_cgroup_snapshot "$round" after

  if [[ "$k6_status" -ne 0 ]]; then
    echo "  k6 falhou na rodada $round (status=$k6_status); snapshots before/after foram salvos" >&2
    exit "$k6_status"
  fi

  out="$RESULTS_DIR/results-round-$round.json"
  cp "$TEST_DIR/results.json" "$out"
  mapfile -t vals < <(extract_result "$out")
  printf '  p99=%s  http_errors=%s  final_score=%s\n' "${vals[0]}" "${vals[1]}" "${vals[2]}"
  echo "  delta cpu.stat:"
  snapshot_delta "$round" service-1
  snapshot_delta "$round" service-2
  snapshot_delta "$round" load-balancer
done

python3 - "$RESULTS_DIR" "$ROUNDS" <<'PY3'
import json, statistics, sys
from pathlib import Path
results_dir = Path(sys.argv[1])
rounds = int(sys.argv[2])
vals = []
for i in range(1, rounds + 1):
    p = results_dir / f"results-round-{i}.json"
    data = json.loads(p.read_text())
    p99_ms = float(data["p99"].removesuffix("ms"))
    vals.append((i, p99_ms, data["scoring"]["breakdown"]["http_errors"], data["scoring"]["final_score"]))
print("\nResumo:")
for i, p99, errs, score in vals:
    print(f"  rodada {i}: p99={p99:.2f}ms  http_errors={errs}  final_score={score}")
p99s = [v[1] for v in vals]
print(f"\n  melhor p99: {min(p99s):.2f}ms")
print(f"  pior p99:   {max(p99s):.2f}ms")
print(f"  media p99:  {statistics.mean(p99s):.2f}ms")
if len(p99s) > 1:
    print(f"  desvio:     {statistics.pstdev(p99s):.2f}ms")
PY3
