# IoT Rule Engine Official Plugins public-release readiness audit

- Audit date: 2026-09-09
- Scope: 23 official plugin modules, repository tooling, manifests, SBOM/signing path, and documentation
- Release verdict: **functionally strong, release governance incomplete**

## Executive assessment

The repository now has the correct isolation model: independent modules and artifacts, no shared plugin
implementation package, one artifact process with per-rule instances, connector resource separation, and a
repository build that produces checksums and SBOMs. Public release remains blocked by SDK publication
order, signing policy, proprietary integration review, AGPL compliance guidance, and security automation.

## Findings

| ID | Severity | Finding | Required action before publishing artifacts |
| --- | --- | --- | --- |
| PLG-OS-001 | Resolved | The official plugin license was finalized on 2026-09-09. | `LICENSE` and both README languages now identify `AGPL-3.0-only`; verify every bundled dependency and vendor boundary before publishing artifacts. |
| PLG-OS-002 | P1 | All modules require SDK `v1.0.0`, but the future GitHub SDK tag must be published first. | Publish, sign, and verify the SDK release before plugin CI or artifacts. |
| PLG-OS-003 | P1 | Release signing is optional and current CI uploads unsigned plugin artifacts. | Require cosign signatures in protected release jobs and publish verification instructions. |
| PLG-OS-004 | P1 | FANUC integration may depend on separately supplied vendor libraries and terms. | Confirm redistribution and runtime licensing rights; keep proprietary binaries out of Git and release archives. |
| PLG-OS-005 | P1 | JavaScript CPU execution is time bounded, but the plugin process has no per-runtime hard memory ceiling. | Apply OS/container memory limits and add hostile-script/OOM recovery tests before enabling untrusted authors. |
| PLG-OS-006 | P1 | CI lacks dependency review, secret scanning, CodeQL/static analysis, and license-policy enforcement across 25 Go modules. | Add repository-wide automated security and license checks. |
| PLG-OS-007 | P2 | The remote points to a local legacy clone and the worktree contains a large uncommitted module migration. | Create reviewable commits and configure the public remote only after clean-clone verification. |
| PLG-OS-008 | P2 | Protocol integration testing still depends largely on mocks or local infrastructure. | Add opt-in hardware/service matrices and publish tested device/database versions without embedding private endpoints. |

## Verified strengths

- Official plugins are independent modules and produce independent versioned artifacts.
- Repository contracts validate manifest identity, module path, SDK version, plugin type, and entry model.
- CI tests every module and builds release artifacts plus CycloneDX SBOMs.
- S7 connector design covers demand-aware session reuse, serialized access, latest-only scheduling, and
  reconnect circuit breaking.
- JavaScript execution uses isolated runtimes, validated JSON output, execution timeouts, and no native I/O.
- No known deployment credential literal or secret file type was found in the working tree scan.
- `dist/` and plugin binaries are ignored.

## Required release gates

1. Verify copyright ownership, dependency compatibility, vendor-library boundaries, and AGPL compliance guidance.
2. Consume a public, immutable SDK `v1.0.0` tag from a clean CI environment.
3. Add dependency, secret, static-security, and license-policy checks across every module.
4. Require signed artifacts, checksum verification, SBOM publication, and build provenance.
5. Validate FANUC redistribution boundaries and document externally supplied runtime dependencies.
6. Run protocol smoke, disconnect, timeout, stop, and long-duration tests against an explicitly authorized lab.
7. Publish only from a clean, signed release commit; never upload a locally mutated `dist/` directory.

## Verification snapshot

The 2026-09-09 working tree passed the root contract tests and vet checks, followed by
`scripts/test-plugins.sh` across all 23 official plugin modules; the optional FANUC stub path also passed.
These results validate source-level behavior only and do not replace authorized hardware-integration or
signed-artifact release tests.

## Compatibility reminder

Plugin schemas may evolve additively, but deployed rule configurations must remain readable throughout the
v1 support window. A plugin requiring a breaking schema change should use a new plugin ID or an explicit,
tested migration rather than silently reinterpreting existing fields.
