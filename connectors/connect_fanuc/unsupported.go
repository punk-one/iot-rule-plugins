//go:build !linux || !cgo || (!amd64 && !arm)

package main

// Deliberately fail unsupported builds. No usable-looking stub artifact is produced.
var _ = FANUC_requires_Linux_CGO_and_an_amd64_or_ARMv7_vendor_library
