package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
)

var platformPattern = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+(?:/v[0-9]+)?$`)

func supportsPlatform(path, target string) (bool, error) {
	if !platformPattern.MatchString(target) {
		return false, errors.New("invalid target platform")
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(raw) > 65536 {
		return false, errors.New("platform descriptor exceeds limit")
	}
	var descriptor struct {
		Version   int      `json:"schema_version"`
		Platforms []string `json:"platforms"`
	}
	if json.Unmarshal(raw, &descriptor) != nil || descriptor.Version != 1 || len(descriptor.Platforms) < 1 || len(descriptor.Platforms) > 32 {
		return false, errors.New("invalid platform descriptor")
	}
	seen := map[string]bool{}
	supported := false
	for _, value := range descriptor.Platforms {
		if !platformPattern.MatchString(value) || seen[value] {
			return false, errors.New("invalid or duplicate supported platform")
		}
		seen[value] = true
		supported = supported || value == target
	}
	return supported, nil
}
