package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNativePlatformFilterDoesNotInventARM64Compatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "platforms.json")
	os.WriteFile(path, []byte(`{"schema_version":1,"platforms":["linux/amd64","linux/arm/v7"],"requires_cgo":true}`), 0600)
	for _, target := range []string{"linux/amd64", "linux/arm/v7"} {
		if supported, err := supportsPlatform(path, target); err != nil || !supported {
			t.Fatal(target, err)
		}
	}
	if supported, err := supportsPlatform(path, "linux/arm64"); err != nil || supported {
		t.Fatal("ARMv7 cannot imply ARM64")
	}
	os.WriteFile(path, []byte(`{"schema_version":1,"platforms":["linux/amd64","linux/amd64"]}`), 0600)
	if _, err := supportsPlatform(path, "linux/amd64"); err == nil {
		t.Fatal("duplicate platforms accepted")
	}
}
