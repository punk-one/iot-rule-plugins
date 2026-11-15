package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateKeyContentDetectionAndPermissions(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{{"key.key", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})}, {"pkcs8.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})}, {"private.der", pkcs8}} {
		path := filepath.Join(t.TempDir(), test.name)
		os.WriteFile(path, test.data, 0600)
		loaded, err := privateKey(path)
		if err != nil || loaded.N.Cmp(key.N) != 0 {
			t.Fatal(test.name, err)
		}
		os.Chmod(path, 0644)
		if _, err = privateKey(path); err == nil {
			t.Fatal("world-readable key accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "oversized.key")
	os.WriteFile(path, make([]byte, 65537), 0600)
	if _, err := privateKey(path); err == nil {
		t.Fatal("unbounded key file accepted")
	}
}
