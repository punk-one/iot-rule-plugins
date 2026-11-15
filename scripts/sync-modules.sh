#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export GOWORK=off

# Synchronize independent consumers against the published stable SDK contract.
# No sibling checkout, local replace or workspace file is used or created.
modules=("${REPO_ROOT}")
for category in connectors sources processors sinks; do
  for manifest in "${REPO_ROOT}/${category}"/*/plugin.json; do
    [[ -f "${manifest}" ]] || continue
    modules+=("$(dirname "${manifest}")")
  done
done

for module in "${modules[@]}"; do
  [[ -f "${module}/go.mod" ]] || { echo "missing independent go.mod: ${module}" >&2; exit 1; }
  echo "sync ${module#"${REPO_ROOT}"}"
  (
    cd "${module}"
    go mod edit -go=1.27.4 -require=github.com/punk-one/iot-rule-plugin-sdk@v1.0.0
    go get google.golang.org/grpc@v1.85.0 golang.org/x/net@v0.62.0 golang.org/x/crypto@v0.59.0
    go mod tidy
    replacement="$(go list -m -f '{{if .Replace}}replaced{{end}}' github.com/punk-one/iot-rule-plugin-sdk)"
    if [[ -n "${replacement}" ]]; then
      echo 'SDK replacement is not permitted in independent release modules' >&2
      exit 1
    fi
  )
done
