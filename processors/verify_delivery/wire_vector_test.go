package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestFrozenCrossImplementationWireVector(t *testing.T) {
	raw, err := os.ReadFile("testdata/delivery-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Secret   string   `json:"secret"`
		Envelope envelope `json:"envelope"`
		Receipt  receipt  `json:"receipt"`
	}
	if json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("invalid fixture")
	}
	header := fixture.Envelope.Delivery
	expected := header.Signature
	header.Signature = ""
	if actual := sign(fixture.Secret, "delivery-v1", header); actual != expected {
		t.Fatal("delivery signature wire drift", actual)
	}
	if identity(header) != header.ID {
		t.Fatal("identity wire drift")
	}
	_, digest, err := canonicalPayload(fixture.Envelope.Payload)
	if err != nil || digest != header.PayloadHash {
		t.Fatal("canonical payload hash wire drift")
	}
	receipt := fixture.Receipt
	expected = receipt.Signature
	receipt.Signature = ""
	if actual := sign(fixture.Secret, "commit-receipt-v1", receipt); actual != expected {
		t.Fatal("commit signature wire drift", actual)
	}
}
