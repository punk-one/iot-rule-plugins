package main

import (
	"context"
	"errors"
	"github.com/gopcua/opcua/ua"
	"strconv"
	"strings"
)

// Called under the resource session gate. Namespace indexes belong to one
// server/session generation and are cleared when that session is retired.
func (r *resource) resolvePoints(ctx context.Context, points []point) ([]point, error) {
	needed := false
	for _, p := range points {
		if p.NamespaceURI != "" {
			needed = true
		}
	}
	if !needed {
		return points, nil
	}
	if r.namespaces == nil {
		response, err := r.client.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: ua.NewNumericNodeID(0, 2255), AttributeID: ua.AttributeIDValue}}, TimestampsToReturn: ua.TimestampsToReturnNeither})
		if err != nil || response == nil || len(response.Results) != 1 || response.Results[0] == nil || response.Results[0].Status != ua.StatusOK || response.Results[0].Value == nil {
			return nil, errors.New("cannot resolve NamespaceArray")
		}
		names, ok := response.Results[0].Value.Value().([]string)
		if !ok || len(names) > 65536 {
			return nil, errors.New("invalid NamespaceArray")
		}
		namespaces := map[string]uint16{}
		for i, name := range names {
			if name == "" {
				return nil, errors.New("empty namespace URI")
			}
			if _, duplicate := namespaces[name]; duplicate {
				return nil, errors.New("duplicate namespace URI")
			}
			namespaces[name] = uint16(i)
		}
		r.namespaces = namespaces
	}
	out := append([]point(nil), points...)
	for i, p := range out {
		if p.NamespaceURI == "" {
			continue
		}
		index, ok := r.namespaces[p.NamespaceURI]
		if !ok {
			return nil, errors.New("configured namespace URI is absent from server")
		}
		node, err := ua.ParseNodeID(p.NodeID)
		if err != nil || node.Namespace() != 0 {
			return nil, errors.New("namespace_uri requires an unqualified/namespace-zero node_id")
		}
		identifier := strings.TrimPrefix(node.String(), "ns=0;")
		out[i].NodeID = "ns=" + strconv.Itoa(int(index)) + ";" + identifier
	}
	return out, nil
}
