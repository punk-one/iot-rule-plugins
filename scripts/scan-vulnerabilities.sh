#!/usr/bin/env bash
# Scan independent modules with an explicitly installed recent govulncheck.
set -euo pipefail
export GOWORK=off
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scanner="${GOVULNCHECK:-govulncheck}"
command -v "$scanner" >/dev/null || { echo 'Install govulncheck v1.9.0 or later, or set GOVULNCHECK.' >&2;exit 2; }
result=0
for manifest in "$root"/{connectors,sources,processors,sinks}/*/plugin.json;do
 [[ -f "$manifest" ]] || continue
 entry="$(dirname "$manifest")"
 if [[ "$entry" == */connect_fanuc && ( "$(go env GOOS)" != linux || "$(go env CGO_ENABLED)" != 1 || ! "$(go env GOARCH)" =~ ^(amd64|arm)$ ) ]];then
  echo 'Skipping native FANUC: supported Linux CGO host required';continue
 fi
 echo "Scanning ${entry#"$root"/}"
 (cd "$entry" && "$scanner" .) || result=1
done
exit "$result"
