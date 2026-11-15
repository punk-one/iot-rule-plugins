#!/usr/bin/env bash
# Independent BACnet service and Engine in a private network namespace only.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to built Linux Engine executable}"
: "${TEST_BACNET_SERVER:?set to independently built bacnet-stack 1.7.0 bacserv}"
for tool in go ip unshare python3; do command -v "$tool" >/dev/null || { echo "missing test prerequisite: $tool" >&2; exit 1; }; done
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-bacnet-engine.XXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'connectors/connect_bacnet\nsources/source_bacnet\nsinks/sink_bacnet\nprocessors/command_guard\nsources/source_schedule\nconnectors/connect_http\nsinks/sink_http'
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
unshare -Urn -- bash -c 'set -euo pipefail; ip link set lo up; TEST_BACNET_ISOLATED=1 python3 scripts/test-bacnet-engine.py'
echo "Local test artifacts and build log: $work"
