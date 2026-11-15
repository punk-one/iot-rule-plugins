package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"os"
)

func boundedFile(path string, max int64, private bool) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open configured certificate/key")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > max || (private && info.Mode().Perm()&0077 != 0) {
		return nil, errors.New("certificate/key must be a bounded regular file; private key must exclude group/other access")
	}
	raw, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil || int64(len(raw)) > max {
		return nil, errors.New("certificate/key exceeds size limit")
	}
	return raw, nil
}
func privateKey(path string) (*rsa.PrivateKey, error) {
	raw, err := boundedFile(path, 65536, true)
	if err != nil {
		return nil, err
	}
	if block, rest := pem.Decode(raw); block != nil {
		if len(rest) > 0 {
			for _, b := range rest {
				if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
					return nil, errors.New("unexpected trailing private key content")
				}
			}
		}
		raw = block.Bytes
	}
	key, err := x509.ParsePKCS1PrivateKey(raw)
	if err != nil {
		value, parseErr := x509.ParsePKCS8PrivateKey(raw)
		if parseErr != nil {
			return nil, errors.New("private key must be unencrypted RSA PKCS1 or PKCS8")
		}
		var ok bool
		key, ok = value.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("OPC UA Basic256Sha256 requires an RSA private key")
		}
	}
	if key.N.BitLen() < 2048 || key.N.BitLen() > 8192 || key.Validate() != nil {
		return nil, errors.New("invalid RSA private key or unsupported key size")
	}
	return key, nil
}
