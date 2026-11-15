package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"testing"
)

func TestRangeClassifiesUnroundedEngineeringValue(t *testing.T) {
	precision := 1
	maximum := 10.0
	point := PointDef{Name: "temperature", ValueType: "Float64", Precision: &precision, Range: &PointRange{Maximum: &maximum, Policy: rangePolicyUseDefault, DefaultValue: 1.25}}
	value, keep, quality, err := applyPointPolicy(10.04, point)
	if err != nil || !keep || quality != "defaulted" || value != 1.3 {
		t.Fatal(value, keep, quality, err)
	}
	value, keep, quality, err = applyPointPolicy(9.96, point)
	if err != nil || !keep || quality != "" || value != 10.0 {
		t.Fatal(value, keep, quality, err)
	}
}
func TestLargeFinitePrecisionDoesNotOverflow(t *testing.T) {
	precision := 15
	large := math.MaxFloat64 / 2
	value, _, _, err := applyPointPolicy(large, PointDef{Name: "large", ValueType: "Float64", Precision: &precision})
	if err != nil || value != large {
		t.Fatal(value, err)
	}
	scale := math.MaxFloat64
	if _, _, _, err = applyPointPolicy(2.0, PointDef{Name: "overflow", ValueType: "Float64", Scale: &scale}); err == nil {
		t.Fatal("scale overflow accepted")
	}
}
func TestCanonicalQualityExistsWithoutOptionalMetadata(t *testing.T) {
	maximum := 10.0
	precision := 1
	raw, _ := json.Marshal(map[string]any{"values": map[string]any{"temperature": 10.04}})
	client := &sourceTestConnector{resp: payloadResponse(raw), status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	out := &sourceTestEmitter{}
	source := &S7Source{}
	source.cfg = S7SourceConfig{ConnectorRef: "resource", Output: outputConf{DeviceCode: "machine-test", IncludeMeta: false}, Points: []PointDef{{Name: "temperature", Address: "DB1.DBD0", ValueType: "Float32", Precision: &precision, Range: &PointRange{Maximum: &maximum, Policy: rangePolicyUseDefault, DefaultValue: 0.0}}}}
	source.ctx = sourceRuntimeContext{connector: client, emitter: out}
	source.setStatus(sdk.ResourceStatusHealthy)
	if _, err := source.poll(); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 1 {
		t.Fatal("telemetry missing")
	}
	event := out.events[0]
	quality := event.Payload["quality"].(map[string]string)
	if quality["temperature"] != "bad:range_default" {
		t.Fatal(event.Payload)
	}
	if _, exists := event.Payload["point_quality"]; exists {
		t.Fatal("legacy optional metadata unexpectedly enabled")
	}
}

func TestEngineeringScaleOffsetBeforeRangeAndPrecision(t *testing.T) {
	scale := 0.1
	max := 10.0
	precision := 0
	value, keep, quality, err := applyPointPolicy(99.0, PointDef{Name: "x", ValueType: "Float64", Scale: &scale, Offset: 0.2, Precision: &precision, Range: &PointRange{Maximum: &max, Policy: rangePolicyClamp}})
	if err != nil || !keep || value != 10.0 || quality != "clamped" {
		t.Fatal(value, keep, quality, err)
	}
}
