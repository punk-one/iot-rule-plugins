#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for arch in amd64 386;do
 mkdir -p "$root/dist/bridges/windows-$arch"
 (cd "$root/bridges/ezsocket_windows" && GOOS=windows GOARCH="$arch" CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o "$root/dist/bridges/windows-$arch/ezsocket-bridge.exe" .)
done
