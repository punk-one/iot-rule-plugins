package main

import (
	"testing"
	"time"
)

func TestTokenBucketRefillAndBoundedKeys(t *testing.T) {
	p := &plugin{cfg: config{Rate: 2, Burst: 1, MaxKeys: 1, IdleTTLMS: 1000}, buckets: map[string]*bucket{}}
	now := time.Now()
	if ok, err := p.allow("a", now); !ok || err != nil {
		t.Fatal("first token denied")
	}
	if ok, _ := p.allow("a", now); ok {
		t.Fatal("burst limit ignored")
	}
	if ok, _ := p.allow("a", now.Add(500*time.Millisecond)); !ok {
		t.Fatal("refill missing")
	}
	if _, err := p.allow("b", now); err == nil {
		t.Fatal("unbounded key table")
	}
	if ok, err := p.allow("b", now.Add(2*time.Second)); !ok || err != nil {
		t.Fatal("idle key not reclaimed")
	}
}
