package main

import (
	"testing"
)

func TestBoundedFormatsAndLargeJSONNumbers(t *testing.T) {
	p := &plugin{cfg: config{Format: "json", Encoding: "utf8", MaxBytes: 1024, MaxRecords: 10}}
	value, err := p.parse(`{"counter":9007199254740993}`)
	if err != nil || value.(map[string]any)["counter"] != "9007199254740993" {
		t.Fatalf("%v %v", value, err)
	}
	p.cfg.Format = "csv"
	p.cfg.Separator = ","
	p.cfg.Header = true
	value, err = p.parse("id,name\n1,example\n")
	if err != nil || len(value.([]any)) != 1 {
		t.Fatalf("CSV %v %v", value, err)
	}
	if _, err = p.parse("id,id\n1,2\n"); err == nil {
		t.Fatal("duplicate CSV header accepted")
	}
	p.cfg.Format = "xml"
	value, err = p.parse("<root>a<child/>b</root>")
	if err != nil {
		t.Fatal(err)
	}
	content := value.(map[string]any)["content"].([]any)
	if len(content) != 3 || content[0] != "a" || content[2] != "b" {
		t.Fatal("XML mixed content order changed")
	}
	for _, raw := range []string{"<!DOCTYPE root><root/>", "<a/><b/>", "<a>"} {
		if _, err = p.parse(raw); err == nil {
			t.Fatalf("invalid XML accepted: %s", raw)
		}
	}
}
