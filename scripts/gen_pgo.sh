#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DIR="${ROOT_DIR}/service"
PROFILE_PATH="${SERVICE_DIR}/default.pgo"
BENCH_TIME="${BENCH_TIME:-20s}"
CACHE_DIR="${GOCACHE:-/tmp/rinha-go-cache}"

echo "generating PGO profile at ${PROFILE_PATH}"
rm -f "${PROFILE_PATH}"

cd "${SERVICE_DIR}"
GOCACHE="${CACHE_DIR}" \
DB_WARMUP=off \
go test ./handler \
  -run '^$' \
  -bench '^BenchmarkBuildAndClassifyPooledState$' \
  -benchtime="${BENCH_TIME}" \
  -count=1 \
  -cpuprofile "${PROFILE_PATH}"

ls -lh "${PROFILE_PATH}"
echo "PGO profile ready"
