#!/usr/bin/env bash
# Runs only inside a new, unnamed user/network namespace; no physical bus.
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for tool in go ip unshare; do command -v "$tool" >/dev/null || { echo "missing test prerequisite: $tool" >&2; exit 1; }; done
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-rule-vcan.XXXXXX")"
binary="$work/can.test"
trap 'rm -f "$binary"; rmdir "$work"' EXIT
export GOOS="$(go env GOHOSTOS)" GOARCH="$(go env GOHOSTARCH)" CGO_ENABLED=1
(
 cd "$repo/connectors/connect_can"
 go test -race -c -o "$binary" .
)
unshare -Urn -- bash -c 'set -euo pipefail; ip link add dev testvcan0 type vcan; ip link set testvcan0 up; TEST_CAN_IFACE=testvcan0 "$1" -test.run TestRealVCan -test.v -test.timeout 30s' bash "$binary"
