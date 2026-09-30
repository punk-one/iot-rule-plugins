#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SDK_ROOT="$(cd "${REPO_ROOT}/../iot-rule-plugin-sdk" && pwd)"

PLUGINS=(
  "connectors/connect_mqtt"
  "connectors/connect_postgres"
  "connectors/connect_redis"
  "connectors/connect_s7"
  "connectors/connect_tdengine"
  "connectors/connect_timescaledb"
  "processors/detect_change"
  "processors/javascript_transform"
  "processors/process_status_event"
  "processors/redis_relationship_mapping"
  "processors/transform"
  "processors/transform_map"
  "sinks/sink_mqtt"
  "sinks/sink_postgres"
  "sinks/sink_redis"
  "sinks/sink_s7"
  "sinks/tdengine_sink"
  "sinks/timescaledb_sink"
  "sources/source_mqtt"
  "sources/source_postgres"
  "sources/source_redis"
  "sources/source_s7"
  "sources/source_timescaledb"
)

cleanup_replaces() {
  for entry in "${PLUGINS[@]}"; do
    if [[ -f "${REPO_ROOT}/${entry}/go.mod" ]]; then
      (cd "${REPO_ROOT}/${entry}" && go mod edit -dropreplace=github.com/punk-one/iot-rule-plugin-sdk 2>/dev/null || true)
    fi
  done
  (cd "${REPO_ROOT}" && go mod edit -dropreplace=github.com/punk-one/iot-rule-plugin-sdk 2>/dev/null || true)
}
trap cleanup_replaces EXIT

for entry in "${PLUGINS[@]}"; do
  plugin_dir="${REPO_ROOT}/${entry}"
  module_path="github.com/punk-one/iot-rule-plugins/${entry}"
  if [[ ! -f "${plugin_dir}/go.mod" ]]; then
    (cd "${plugin_dir}" && GOWORK=off go mod init "${module_path}")
  fi
  (cd "${plugin_dir}" && go mod edit -require=github.com/punk-one/iot-rule-plugin-sdk@v1.0.0)
  (cd "${plugin_dir}" && go mod edit -replace=github.com/punk-one/iot-rule-plugin-sdk="${SDK_ROOT}")
done

(cd "${REPO_ROOT}" && go mod edit -require=github.com/punk-one/iot-rule-plugin-sdk@v1.0.0)
(cd "${REPO_ROOT}" && go mod edit -replace=github.com/punk-one/iot-rule-plugin-sdk="${SDK_ROOT}")
(cd "${REPO_ROOT}" && go mod edit -dropreplace=github.com/taosdata/driver-go/v3 2>/dev/null || true)

for entry in "${PLUGINS[@]}"; do
  echo "tidy ${entry}"
  (cd "${REPO_ROOT}/${entry}" && go mod tidy)
done

(cd "${REPO_ROOT}" && go mod tidy)
cleanup_replaces
trap - EXIT
(cd "${REPO_ROOT}" && go work sync)
