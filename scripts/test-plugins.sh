#!/usr/bin/env bash

set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RACE_FLAG=()
if [[ "${PLUGIN_RACE:-1}" == "1" ]]; then
  RACE_FLAG=(-race)
fi

PLUGINS=()
for category in connectors sources processors sinks; do
  for manifest in "${REPO_ROOT}/${category}"/*/plugin.json; do
    [[ -f "${manifest}" ]] || continue
    entry="${category}/$(basename "$(dirname "${manifest}")")"
    # The native connector has its own explicit host/ABI test below.
    [[ "${entry}" == "connectors/connect_fanuc" ]] && continue
    PLUGINS+=("${entry}")
  done
done
echo "Discovered ${#PLUGINS[@]} ordinary plugin modules"

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

if [[ "$(go env GOOS)" == "linux" && "$(go env CGO_ENABLED)" == "1" && ( "$(go env GOARCH)" == "amd64" || "$(go env GOARCH)" == "arm" ) ]]; then
  echo 'testing optional native FANUC connector with synthetic ABI fixture'
  (cd "${REPO_ROOT}/connectors/connect_fanuc" && GOWORK=off go test "${RACE_FLAG[@]}" . && GOWORK=off go vet .)
fi
