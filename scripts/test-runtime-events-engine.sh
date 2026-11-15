#!/usr/bin/env bash
# Real Engine and deliberately failing local JS nodes; no device/network writes.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to Engine containing managed runtime-event source support}"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-runtime-events-engine.XXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'sources/source_runtime_events\nsources/source_schedule\nprocessors/javascript_transform\nconnectors/connect_http\nsinks/sink_http'
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
python3 scripts/test-runtime-events-engine.py
echo "Local test artifacts and build log: $work"
