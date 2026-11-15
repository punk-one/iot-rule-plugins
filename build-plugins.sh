#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-${SCRIPT_DIR}}"
PLUGINS_SRC_DIR="${PLUGINS_SRC_DIR:-${REPO_ROOT}}"
PLUGINS_DST_DIR="${PLUGINS_DST_DIR:-${REPO_ROOT}/dist/plugins}"

# Discover independent local modules. Release publishing remains an explicit
# subset in release-plugins.txt; a local build does not publish all plugins.
DEFAULT_PLUGINS=()
for category in connectors sources processors sinks; do
  for manifest in "${PLUGINS_SRC_DIR}/${category}"/*/plugin.json; do
    [[ -f "${manifest}" ]] || continue
    entry="${category}/$(basename "$(dirname "${manifest}")")"
    [[ "${entry}" == "connectors/connect_fanuc" ]] && continue
    DEFAULT_PLUGINS+=("${entry}")
  done
done

PLUGINS=("${DEFAULT_PLUGINS[@]}")
if [[ -n "${PLUGINS_OVERRIDE:-}" ]]; then
  mapfile -t PLUGINS <<<"${PLUGINS_OVERRIDE}"
fi

if [[ "${INCLUDE_NATIVE_PLUGINS:-0}" == "1" ]]; then
  if [[ "$(go env GOOS)" != "linux" || "$(go env CGO_ENABLED)" != "1" ]]; then
    echo 'error: FANUC native build requires Linux and CGO' >&2
    exit 1
  fi
  case "$(go env GOARCH)" in
    amd64) ;;
    arm) [[ "$(go env GOARM)" == 7* ]] || { echo 'error: FANUC ARM requires GOARM=7' >&2; exit 1; } ;;
    *) echo 'error: no FANUC native library for requested CPU architecture' >&2; exit 1 ;;
  esac
  if [[ ! " ${PLUGINS[*]} " =~ " connectors/connect_fanuc " ]]; then PLUGINS+=("connectors/connect_fanuc");fi
fi

MANIFEST_TOOL="$(mktemp)"
RELEASE_RECORDS="$(mktemp)"
SIGNING_CONFIG_ARGS=()
if [[ -n "${PLUGIN_SIGNING_CONFIG:-}" ]];then SIGNING_CONFIG_ARGS=(--signing-config "$PLUGIN_SIGNING_CONFIG");fi
cleanup() {
  rm -f "${MANIFEST_TOOL}" "${RELEASE_RECORDS}"
}
trap cleanup EXIT

(cd "${REPO_ROOT}" && GOOS="$(go env GOHOSTOS)" GOARCH="$(go env GOHOSTARCH)" GOWORK=off go build -o "${MANIFEST_TOOL}" ./cmd/pluginmanifest)

build_plugin() {
  local entry="$1"
  local category
  local plugin_dir
  local plugin_id
  local version
  local plugin_type
  local sdk_version
  local module_path
  local actual_module
  local output_dir
  local versioned_output
  local latest_output
  local checksum
	local sbom_output
	local signature_output=""
	local signature_record=""
  local metadata=()

  category="$(dirname "${entry}")"
  plugin_dir="${PLUGINS_SRC_DIR}/${entry}"

  if [[ ! -d "${plugin_dir}" ]]; then
    echo "error: plugin directory not found: ${plugin_dir}" >&2
    return 1
  fi
  if [[ ! -f "${plugin_dir}/go.mod" ]]; then
    echo "error: independent module missing: ${plugin_dir}/go.mod" >&2
    return 1
  fi

  mapfile -t metadata < <("${MANIFEST_TOOL}" inspect "${plugin_dir}/plugin.json")
  if [[ "${#metadata[@]}" -ne 5 ]]; then
    echo "error: invalid plugin manifest: ${plugin_dir}/plugin.json" >&2
    return 1
  fi
  plugin_id="${metadata[0]}"
  version="${metadata[1]}"
  plugin_type="${metadata[2]}"
  sdk_version="${metadata[3]}"
  module_path="${metadata[4]}"
  actual_module="$(cd "${plugin_dir}" && GOWORK=off go list -m)"
  if [[ "${actual_module}" != "${module_path}" ]]; then
    echo "error: manifest module ${module_path} != go.mod module ${actual_module}" >&2
    return 1
  fi

  output_dir="${PLUGINS_DST_DIR}/${category}"
  versioned_output="${output_dir}/${plugin_id}-${version}.so"
  latest_output="${output_dir}/${plugin_id}-latest.so"
  mkdir -p "${output_dir}"

  if [[ -f "${plugin_dir}/platforms.json" ]]; then
    local target_platform="$(go env GOOS)/$(go env GOARCH)"
    if [[ "$(go env GOARCH)" == "arm" ]]; then local target_goarm="$(go env GOARM)";target_platform="${target_platform}/v${target_goarm%%,*}";fi
    local supported_platform
    supported_platform="$("${MANIFEST_TOOL}" supports-platform "${plugin_dir}/platforms.json" "${target_platform}")" || return 1
    if [[ "${supported_platform}" != "true" ]];then
      echo "skipping ${entry}: unsupported native dependency platform ${target_platform}"
      return 0
    fi
  fi

  echo "building ${entry} -> ${versioned_output}"
	(cd "${plugin_dir}" && GOWORK=off go build -trimpath -ldflags='-s -w' -o "${versioned_output}" .)

  chmod +x "${versioned_output}"
  ln -sfn "$(basename "${versioned_output}")" "${latest_output}"
  checksum="$(sha256sum "${versioned_output}" | cut -d " " -f 1)"
	sbom_output="${versioned_output}.sbom.json"
	"${MANIFEST_TOOL}" sbom "${versioned_output}" "${sbom_output}" "${plugin_id}" "${version}"
	if [[ "${PLUGIN_SIGNING_MODE:-}" == "keyless" ]]; then
		if ! command -v cosign >/dev/null 2>&1; then
			echo "error: PLUGIN_SIGNING_MODE=keyless but cosign is unavailable" >&2
			return 1
		fi
		signature_output="${versioned_output}.sigstore.json"
		cosign sign-blob --yes "${SIGNING_CONFIG_ARGS[@]}" --bundle "${signature_output}" "${versioned_output}"
		signature_record="${category}/$(basename "${signature_output}")"
	elif [[ -n "${PLUGIN_SIGNING_KEY:-}" ]]; then
		if ! command -v cosign >/dev/null 2>&1; then
			echo "error: PLUGIN_SIGNING_KEY is set but cosign is unavailable" >&2
			return 1
		fi
		signature_output="${versioned_output}.sigstore.json"
		cosign sign-blob --yes "${SIGNING_CONFIG_ARGS[@]}" --key "${PLUGIN_SIGNING_KEY}" \
			--bundle "${signature_output}" "${versioned_output}"
		signature_record="${category}/$(basename "${signature_output}")"
	elif [[ "${PLUGIN_REQUIRE_SIGNATURE:-0}" == "1" ]]; then
		echo "error: PLUGIN_REQUIRE_SIGNATURE=1 requires PLUGIN_SIGNING_MODE=keyless or PLUGIN_SIGNING_KEY" >&2
		return 1
	fi
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${plugin_id}" "${version}" "${plugin_type}" "${sdk_version}" \
		"${module_path}" "${category}/$(basename "${versioned_output}")" "${checksum}" \
		"${category}/$(basename "${sbom_output}")" "${signature_record}" "$(go env GOOS)" "$(go env GOARCH)" \
    >>"${RELEASE_RECORDS}"
}

main() {
  mkdir -p \
    "${PLUGINS_DST_DIR}/connectors" \
    "${PLUGINS_DST_DIR}/processors" \
    "${PLUGINS_DST_DIR}/sinks" \
    "${PLUGINS_DST_DIR}/sources"

  for entry in "${PLUGINS[@]}"; do
    build_plugin "${entry}"
  done

  local release_manifest="${PLUGINS_DST_DIR}/release-manifest.json"
  "${MANIFEST_TOOL}" release "${RELEASE_RECORDS}" "${release_manifest}" "${REPO_ROOT}"
  sign_release_manifest "${release_manifest}"
  echo "plugin build completed: ${PLUGINS_DST_DIR}"
}

sign_release_manifest() {
  local manifest_path="$1"
  if [[ "${PLUGIN_SIGNING_MODE:-}" == "keyless" ]]; then
    if ! command -v cosign >/dev/null 2>&1; then
      echo "error: PLUGIN_SIGNING_MODE=keyless but cosign is unavailable" >&2
      return 1
    fi
    cosign sign-blob --yes "${SIGNING_CONFIG_ARGS[@]}" --bundle "${manifest_path}.sigstore.json" "${manifest_path}"
  elif [[ -n "${PLUGIN_SIGNING_KEY:-}" ]]; then
    if ! command -v cosign >/dev/null 2>&1; then
      echo "error: PLUGIN_SIGNING_KEY is set but cosign is unavailable" >&2
      return 1
    fi
    cosign sign-blob --yes "${SIGNING_CONFIG_ARGS[@]}" --key "${PLUGIN_SIGNING_KEY}" \
      --bundle "${manifest_path}.sigstore.json" "${manifest_path}"
  elif [[ "${PLUGIN_REQUIRE_SIGNATURE:-0}" == "1" ]]; then
    echo "error: signed releases require a signature for release-manifest.json" >&2
    return 1
  fi
}

main "$@"
