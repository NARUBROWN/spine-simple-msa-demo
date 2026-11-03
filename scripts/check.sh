#!/usr/bin/env bash
set -euo pipefail

repository_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spine_cli="${SPINE_CLI_BIN:-spine}"
cd "${repository_dir}"

for project in order-app stock-app; do
  # 공개 CLI의 계약 검사는 활성 go.work를 지원하지 않으므로 각 모듈의 replace로 해석합니다.
  GOWORK=off "${spine_cli}" context --root "${repository_dir}/${project}" --json
  GOWORK=off "${spine_cli}" contract check --root "${repository_dir}/${project}" --warnings-as-errors --json
  GOWORK=off "${spine_cli}" verify --root "${repository_dir}/${project}" --warnings-as-errors --json
  GOWORK=off "${spine_cli}" build --root "${repository_dir}/${project}"
done

# 각 모듈을 순회해 공유 이벤트와 outbox까지 검사합니다.
for module in . order-app stock-app; do
  (cd "${module}" && go vet ./... && go test -race ./... -count=1)
done
