#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/iot-buffer-fault.XXXXXXXX")"
cd "$repo/processors/buffer_node"
go test -race .
go test -c -race -o "$work/buffer.test" .
export IOT_TEST_BUFFER_FULL_DIR="$work/private-fs"
mkdir "$IOT_TEST_BUFFER_FULL_DIR"
# A private user+mount namespace owns the bounded tmpfs; the host disk is not filled.
unshare -Urm bash -c 'set -e; mount --make-rprivate /; mount -t tmpfs -o size=8m,mode=700 tmpfs "$IOT_TEST_BUFFER_FULL_DIR"; exec "$1" -test.run "^TestBufferDiskFullRollback$" -test.v' -- "$work/buffer.test"
echo "Private test output: $work"
