package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDetectChangeSchemaOmitsUnusedInputFilter(t *testing.T) {
	info := (&ChangeDetectPlugin{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", schema["properties"])
	}
	if _, exists := properties["input"]; exists {
		t.Fatalf("expected detect_change schema to omit unused input section, got %#v", properties["input"])
	}
}

func TestDetectChangeSchemaDescribesStateAndProcessFields(t *testing.T) {
	info := (&ChangeDetectPlugin{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", schema["properties"])
	}

	stateSchema, ok := properties["state"].(map[string]interface{})
	if !ok {
		t.Fatalf("state schema missing or invalid: %#v", properties["state"])
	}
	assertDetectChangeSchemaDescriptions(t, stateSchema, []string{"key_by", "ttl", "max_keys", "evict"})

	processSchema, ok := properties["process"].(map[string]interface{})
	if !ok {
		t.Fatalf("process schema missing or invalid: %#v", properties["process"])
	}
	processProps, ok := processSchema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.properties missing or invalid: %#v", processSchema["properties"])
	}
	detectSchema, ok := processProps["detect"].(map[string]interface{})
	if !ok {
		t.Fatalf("detect schema missing or invalid: %#v", processProps["detect"])
	}
	assertDetectChangeSchemaDescriptions(t, detectSchema, []string{"type", "value", "field", "fields", "direction"})

	outputSchema, ok := properties["output"].(map[string]interface{})
	if !ok {
		t.Fatalf("output schema missing or invalid: %#v", properties["output"])
	}
	assertDetectChangeSchemaDescriptions(t, outputSchema, []string{"emit", "emit_first", "annotate"})
}

func TestDetectChangeAnnotationDefaultsToEnabled(t *testing.T) {
	var cfg Config
	if !cfg.annotateEnabled() {
		t.Fatal("annotation must remain enabled by default for backward compatibility")
	}
	disabled := false
	cfg.Output.Annotate = &disabled
	if cfg.annotateEnabled() {
		t.Fatal("explicit annotate=false must preserve the downstream payload contract")
	}
}

func assertDetectChangeSchemaDescriptions(t *testing.T, parent map[string]interface{}, keys []string) {
	t.Helper()

	properties, ok := parent["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", parent["properties"])
	}

	for _, key := range keys {
		propSchema, ok := properties[key].(map[string]interface{})
		if !ok {
			t.Fatalf("schema for %s missing or invalid: %#v", key, properties[key])
		}
		description, _ := propSchema["description"].(string)
		if strings.TrimSpace(description) == "" {
			t.Fatalf("expected non-empty description for %s, got %#v", key, propSchema["description"])
		}
	}
}
