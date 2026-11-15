package main

import (
	"testing"
	"time"
)

func TestTypedPartitionRetainsDisplayKey(t *testing.T) {
	p := &plugin{cfg: config{MaxKeys: 2, MaxCount: 10, WindowMS: 1000}, groups: map[string]*aggregate{}}
	p.addDisplay("string:1", "1", 2, time.Now())
	p.addDisplay("float64:1", "1", 5, time.Now())
	g := p.drain(time.Now(), true)
	if len(g) != 2 || g[0].count != 1 || g[1].count != 1 || g[0].key != "1" || g[1].key != "1" {
		t.Fatalf("groups=%+v", g)
	}
}
