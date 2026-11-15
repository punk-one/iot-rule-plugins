package main

import (
	"math"
	"testing"
	"time"
)

func TestWindowCountTimeoutAndCapacity(t *testing.T) {
	p := &plugin{cfg: config{MaxCount: 2, WindowMS: 100, MaxKeys: 1}, groups: map[string]*aggregate{}}
	now := time.Now()
	if g, err := p.add("a", 2, now); err != nil || g != nil {
		t.Fatal("unexpected flush")
	}
	if _, err := p.add("b", 1, now); err == nil {
		t.Fatal("capacity ignored")
	}
	g, err := p.add("a", 4, now)
	if err != nil || g == nil || len(g) != 1 || g[0].sum != 6 || g[0].min != 2 || g[0].max != 4 {
		t.Fatalf("%+v %v", g, err)
	}
	p.add("a", 3, now)
	if len(p.drain(now.Add(99*time.Millisecond), false)) != 0 {
		t.Fatal("window flushed early")
	}
	if len(p.drain(now.Add(100*time.Millisecond), false)) != 1 {
		t.Fatal("window not flushed")
	}
	if _, err = p.add("a", math.Inf(1), now); err == nil {
		t.Fatal("non-finite accepted")
	}
}

func TestEventCannotExtendExpiredWindow(t *testing.T) {
	p := &plugin{cfg: config{MaxCount: 10, WindowMS: 100, MaxKeys: 1}, groups: map[string]*aggregate{}}
	now := time.Now()
	p.add("a", 2, now)
	flushed, err := p.add("a", 4, now.Add(100*time.Millisecond))
	if err != nil || len(flushed) != 1 || flushed[0].sum != 2 || p.groups["a"].sum != 4 {
		t.Fatal("event crossed processing-time boundary")
	}
}
