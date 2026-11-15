#!/usr/bin/env bash
# Independent IEC104 station/Engine in a private user/network namespace.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to built Linux Engine}"
: "${TEST_IEC104_SERVER:?set to independently built lib60870 2.4.1 simple_server}"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-iec104-engine.XXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'connectors/connect_iec104\nsources/source_iec104\nconnectors/connect_http\nsinks/sink_http'
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
unshare -Urn -- bash -c 'set -euo pipefail; ip link set lo up; TEST_IEC104_ISOLATED=1 python3 scripts/test-iec104-engine.py'
echo "Local test artifacts and build log: $work"
