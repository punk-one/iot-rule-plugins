#!/usr/bin/env bash
# Loopback Manager/HTTP and a private virtual CAN namespace; no physical bus.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set TEST_ENGINE_BINARY to a built Linux Engine executable}"
for tool in go ip unshare python3; do command -v "$tool" >/dev/null || { echo "missing test prerequisite: $tool" >&2; exit 1; }; done
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-can-engine.XXXXXX")"
export TEST_CAN_FIXTURE="$work/can-peer"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'connectors/connect_can\nsources/source_can\nsinks/sink_can\nprocessors/command_guard\nsources/source_schedule\nconnectors/connect_http\nsinks/sink_http'
(
 cd "$repo/connectors/connect_can"
 go build -o "$TEST_CAN_FIXTURE" ./testdata/peer
)
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
unshare -Urn -- bash -c 'set -euo pipefail; ip link set lo up; ip link add dev testvcan0 type vcan; ip link set testvcan0 up; TEST_CAN_IFACE=testvcan0 TEST_CAN_ISOLATED=1 python3 scripts/test-can-engine.py'
echo "Local test artifacts and build log: $work"
