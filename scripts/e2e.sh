#!/usr/bin/env bash
set -euo pipefail

repository_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repository_dir}/docker-compose.e2e.yml"
kafka_port="${SPINE_DEMO_KAFKA_PORT:-19092}"

cleanup() {
  docker compose -f "${compose_file}" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -f "${compose_file}" up -d --wait
SPINE_DEMO_E2E_BROKER="127.0.0.1:${kafka_port}" go test -tags=e2e -v ./e2e
