package main

import (
	"math"
	"testing"
)

func TestTypedKeysDoNotShareBucket(t *testing.T) {
	a, _ := scalarKey("1")
	b, _ := scalarKey(float64(1))
	c, _ := scalarKey(true)
	if a == b || b == c || a == c {
		t.Fatal("key collision")
	}
	if _, err := scalarKey(math.NaN()); err == nil {
		t.Fatal("NaN accepted")
	}
}
