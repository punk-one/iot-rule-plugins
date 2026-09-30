package fanucutil

import (
	"encoding/json"
	"testing"
)

func TestEncodeMacroValueAutoDecimals(t *testing.T) {
	raw, decimals, err := EncodeMacroValue(12.345, nil)
	if err != nil {
		t.Fatalf("encode macro value: %v", err)
	}
	if raw != 12345 || decimals != 3 {
		t.Fatalf("expected 12345/3, got %d/%d", raw, decimals)
	}
}

func TestEncodeMacroValueHonorsDecimalPlaces(t *testing.T) {
	places := 2
	raw, decimals, err := EncodeMacroValue(json.Number("12.345"), &places)
	if err != nil {
		t.Fatalf("encode macro value with places: %v", err)
	}
	if raw != 1235 || decimals != 2 {
		t.Fatalf("expected rounded 1235/2, got %d/%d", raw, decimals)
	}
}

func TestEncodeMacroValueRejectsOverPrecision(t *testing.T) {
	if _, _, err := EncodeMacroValue("1234567890.12", nil); err == nil {
		t.Fatal("expected precision error")
	}
}

func TestDeduplicateIntsPreservesOrder(t *testing.T) {
	got := DeduplicateInts([]int{3, 1, 3, 2, 1, 4})
	want := []int{3, 1, 2, 4}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}
