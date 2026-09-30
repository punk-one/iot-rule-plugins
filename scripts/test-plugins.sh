#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RACE_FLAG=()
if [[ "${PLUGIN_RACE:-1}" == "1" ]]; then
  RACE_FLAG=(-race)
fi

PLUGINS=(
  connectors/connect_mqtt connectors/connect_postgres connectors/connect_redis
  connectors/connect_s7 connectors/connect_tdengine connectors/connect_timescaledb
  processors/detect_change processors/javascript_transform processors/process_status_event
  processors/redis_relationship_mapping
  processors/transform processors/transform_map
  sinks/sink_mqtt sinks/sink_postgres sinks/sink_redis sinks/sink_s7 sinks/tdengine_sink sinks/timescaledb_sink
  sources/source_mqtt sources/source_postgres sources/source_redis sources/source_s7 sources/source_timescaledb
)

for entry in "${PLUGINS[@]}"; do
  echo "testing ${entry}"
  (
    cd "${REPO_ROOT}/${entry}"
    build_output="$(mktemp)"
    trap 'rm -f "${build_output}"' EXIT
    go test "${RACE_FLAG[@]}" .
    go vet .
    go build -o "${build_output}" .
  )
done

echo "testing optional fanuc stub"
(cd "${REPO_ROOT}/fanucutil" && GOWORK=off go test "${RACE_FLAG[@]}" . && GOWORK=off go vet .)
