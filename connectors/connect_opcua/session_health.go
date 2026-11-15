package main

import (
	"context"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"time"
)

// The upstream client can pause its publish loop without forwarding EOF/session
// loss as a notification. Inspect state and verify active-session demand so a
// Source cannot silently remain connected to a dead subscription forever.
func (r *resource) sessionHealthy() bool {
	if state, ok := r.client.(interface{ State() opcua.ConnState }); ok && state.State() != opcua.Connected {
		return false
	}
	if len(r.streams) == 0 || time.Since(r.lastProbe) < 5*time.Second {
		return true
	}
	r.lastProbe = time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(min(r.cfg.TimeoutMS, 1000))*time.Millisecond)
	defer cancel()
	response, err := r.client.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: ua.NewNumericNodeID(0, 2258), AttributeID: ua.AttributeIDValue}}, TimestampsToReturn: ua.TimestampsToReturnNeither})
	return err == nil && response != nil && len(response.Results) == 1 && response.Results[0] != nil && uint32(response.Results[0].Status)&0xc0000000 == 0 && response.Results[0].Value != nil
}
