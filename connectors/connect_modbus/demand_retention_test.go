package main

import (
	"testing"
)

func TestDistantDemandDoesNotRetainStaleNodeTokens(t *testing.T) {
	plc := newSessionPLC(t)
	c := sessionConnector(t, plc, nil)
	if err := demandRead(c, "changed-source", 100); err != nil {
		t.Fatal(err)
	}
	if !demandStats(t, c).Active {
		t.Fatal("near demand missing")
	}
	if err := demandRead(c, "changed-source", 20000); err != nil {
		t.Fatal(err)
	}
	s := demandStats(t, c)
	if s.Active || s.Demands != 0 {
		t.Fatal("distant demand retained an inactive lease", s)
	}
}
