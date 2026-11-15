#!/usr/bin/env bash
# Local-only TCP peer, Manager and HTTP capture. No external network devices.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to built Linux Engine executable}"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-freeport-engine.XXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'connectors/connect_tcp\nsources/source_freeport\nconnectors/connect_http\nsinks/sink_http'
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
python3 scripts/test-freeport-engine.py
echo "Local test artifacts and build log: $work"
