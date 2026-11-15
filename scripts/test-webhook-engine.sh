#!/usr/bin/env bash
# Temporary local Manager and HTTP capture; signing secrets are generated per run.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEST_ENGINE_BINARY:?set to built Engine executable containing managed webhook support}"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-webhook-engine.XXXXXX")"
export TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR:-$work}"
export TEST_PLUGIN_DIR="$work/plugins" PLUGINS_DST_DIR="$work/plugins" PLUGINS_SRC_DIR="$repo"
export PLUGINS_OVERRIDE=$'sources/source_webhook\nsources/source_manual\nconnectors/connect_http\nsinks/sink_http'
cd "$repo"
./build-plugins.sh > "$work/build.log" 2>&1
python3 scripts/test-webhook-engine.py
echo "Local test artifacts and build log: $work"
