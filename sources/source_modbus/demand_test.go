package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type releaseClient struct {
	sdk.ConnectorClient
	requests []sdk.ConnectorRequest
}

func (c *releaseClient) Read(q sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.requests = append(c.requests, q)
	return sdk.ConnectorResponse{}, nil
}

type releaseRuntime struct {
	sdk.RuntimeContext
	client sdk.ConnectorClient
}

func (r releaseRuntime) Connector() sdk.ConnectorClient { return r.client }
func TestStopReleasesOnlyRuntimeBoundDemand(t *testing.T) {
	c := &releaseClient{}
	p := &plugin{cfg: config{TimeoutMS: 3000}, ctx: releaseRuntime{client: c}}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if len(c.requests) != 1 || c.requests[0].Target != "release_demand" || c.requests[0].TimeoutMS != 1000 || c.requests[0].ResourceToken != "" {
		t.Fatal("release must let the engine inject this node's token", c.requests)
	}
}
