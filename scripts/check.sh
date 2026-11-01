#!/usr/bin/env bash
set -euo pipefail

repository_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spine_cli="${SPINE_CLI_BIN:-spine}"
cd "${repository_dir}"

for project in order-app stock-app; do
  "${spine_cli}" context --root "${repository_dir}/${project}" --json
  "${spine_cli}" contract check --root "${repository_dir}/${project}" --json
  "${spine_cli}" verify --root "${repository_dir}/${project}" --json
done

# 각 모듈을 순회해 공유 이벤트와 outbox까지 검사합니다.
for module in . order-app stock-app; do
  (cd "${module}" && go vet ./... && go test -race ./... -count=1)
done
