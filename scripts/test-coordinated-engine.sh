#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to the current Engine executable}"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-coordinated.XXXXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}" TEST_PLUGIN_DIR="$work/plugins"
cd "$repo"
PLUGINS_OVERRIDE=$'connectors/connect_modbus\nsources/source_modbus\nsinks/sink_modbus\nsources/source_manual\nconnectors/connect_http\nsinks/sink_http' PLUGINS_DST_DIR="$TEST_PLUGIN_DIR" ./build-plugins.sh > "$work/build.log" 2>&1
python3 scripts/test-coordinated-engine.py
echo "Private test output: $work"
