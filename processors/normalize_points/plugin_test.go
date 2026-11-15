package main

import (
	"math"
	"testing"
)

func TestBadQualityCannotBeMaskedByDefaults(t *testing.T) {
	lo := float64(0)
	hi := float64(100)
	scale := float64(.1)
	precision := 2
	p := pointPolicy{Min: &lo, Max: &hi, Scale: &scale, Precision: &precision, Default: 1.5, OutOfRange: "default"}
	v, q := normalize(2000, "good", p)
	if v != 1.5 || q != "bad:range" {
		t.Fatalf("%v %s", v, q)
	}
	v, q = normalize(10, "bad:stale", p)
	if q != "bad:stale" || v != 1.5 {
		t.Fatal("bad quality masked")
	}
	v, q = normalize(123, "good", p)
	if v != 12.3 || q != "good" {
		t.Fatalf("%v %s", v, q)
	}
	if _, q = normalize(uint64(9007199254740993), "good", p); q != "bad:number" {
		t.Fatal("large integer rounded")
	}
	if _, q = normalize(math.Inf(1), "good", p); q != "bad:number" {
		t.Fatal("non-finite accepted")
	}
}
