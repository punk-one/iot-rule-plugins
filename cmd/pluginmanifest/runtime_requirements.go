package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

type runtimeRequirements struct {
	SchemaVersion     int      `json:"schema_version"`
	ConnectorFamilies []string `json:"connector_families,omitempty"`
	NativeLibraries   []string `json:"native_libraries,omitempty"`
	CompanionServices []string `json:"companion_services,omitempty"`
}

func validateRequirements(r *runtimeRequirements) error {
	if r == nil {
		return nil
	}
	if r.SchemaVersion != 1 {
		return errors.New("unsupported runtime requirements version")
	}
	valid := regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)
	for _, list := range [][]string{r.ConnectorFamilies, r.NativeLibraries, r.CompanionServices} {
		if len(list) > 16 {
			return errors.New("too many runtime requirements")
		}
		seen := map[string]bool{}
		for _, id := range list {
			if !valid.MatchString(id) || seen[id] {
				return errors.New("invalid or duplicate runtime requirement")
			}
			seen[id] = true
		}
	}
	return nil
}
func requirementsFor(repo, id string) (*runtimeRequirements, error) {
	for _, category := range []string{"connectors", "sources", "processors", "sinks"} {
		files, _ := filepath.Glob(filepath.Join(repo, category, "*", "plugin.json"))
		for _, file := range files {
			raw, err := os.ReadFile(file)
			if err != nil {
				return nil, err
			}
			var m manifest
			if err = json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			if m.ID == id {
				return m.Requirements, validateRequirements(m.Requirements)
			}
		}
	}
	return nil, nil
}

func (r *runtimeRequirements) UnmarshalJSON(raw []byte) error {
	type plain runtimeRequirements
	var value plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return err
	}
	*r = runtimeRequirements(value)
	return nil
}
