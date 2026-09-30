package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/punk-one/gos7"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestWithClientDeviceGateWaitHonorsRequestTimeout(t *testing.T) {
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		return nil, func() error { return nil }, nil
	})
	resource := &s7Resource{
		resource:    sdk.ConnectorResource{ID: "locked"},
		cfg:         s7ConnectionConfig{TimeoutSec: 5},
		nextDemands: make(map[string]time.Time),
	}
	resource.operationMu.Lock()
	started := time.Now()
	err := connector.withClient(resource, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestBatchRead, TimeoutMS: 10}, func(gos7.Client) error { return nil })
	resource.operationMu.Unlock()
	if err == nil {
		t.Fatal("expected device gate timeout")
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("device gate timeout took too long: %s", elapsed)
	}
}

func TestConnectorInfoContainsSchema(t *testing.T) {
	info := NewConnector().Info()
	if info.Type != sdk.PluginTypeConnector {
		t.Fatalf("expected connector type, got %s", info.Type)
	}
	if strings.TrimSpace(info.Capabilities.ConfigSchema) == "" {
		t.Fatal("expected config schema")
	}
}

func TestCreateResourceAndParseAddresses(t *testing.T) {
	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:            "s7-1",
		PluginID:      "connect-s7",
		SessionPolicy: s7SessionPolicySplitRW,
		Connection: mustJSON(t, map[string]any{
			"host": "192.168.1.10",
			"port": 102,
		}),
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}
	defer func() { _ = connector.DestroyResource(handle) }()

	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestMetadata,
		Target: "parse_addresses",
		Payload: mustJSON(t, map[string]any{
			"points": []map[string]any{
				{"name": "running", "address": "DB1.DBX0.0", "value_type": "Bool"},
				{"name": "temp", "address": "MW10", "value_type": "Int16"},
			},
		}),
	})
	if err != nil {
		t.Fatalf("parse addresses: %v", err)
	}

	var payload struct {
		Points []map[string]any `json:"points"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(payload.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(payload.Points))
	}
	if payload.Points[0]["db_number"] != float64(1) {
		t.Fatalf("unexpected parsed point: %+v", payload.Points[0])
	}
	if payload.Points[1]["byte_size"] != float64(2) {
		t.Fatalf("unexpected parsed point byte size: %+v", payload.Points[1])
	}
}

func TestPerOperationConnectionIsAlwaysClosed(t *testing.T) {
	var opened atomic.Int32
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error {
			closed.Add(1)
			return nil
		}, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-short-lived", PluginID: "connect-s7",
		SessionPolicy: sdk.ConnectorSessionPolicyPerOperation,
		Connection:    mustJSON(t, map[string]any{"host": "192.0.2.1"}),
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}
	resource, err := connector.getResource(handle)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := connector.withClient(resource, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestBatchRead}, func(gos7.Client) error { return nil }); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	if opened.Load() != 2 || closed.Load() != 2 {
		t.Fatalf("connections opened=%d closed=%d, want 2/2", opened.Load(), closed.Load())
	}
}

func TestZeroReuseHorizonEnforcesOneSessionPerBatch(t *testing.T) {
	var opened atomic.Int32
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-zero-horizon", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.2", "sessionReuseHorizonMillis": 0}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 100}),
	}
	for i := 0; i < 2; i++ {
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if opened.Load() != 2 || closed.Load() != 2 {
		t.Fatalf("connections opened=%d closed=%d, want 2/2", opened.Load(), closed.Load())
	}
}

func TestLegacyS7SessionPoliciesNormalizeToScheduledReuse(t *testing.T) {
	for _, policy := range []string{"", s7SessionPolicySharedDuplex, s7SessionPolicySplitRW, sdk.ConnectorSessionPolicyScheduledReuse} {
		if got := normalizeSessionPolicy(policy); got != sdk.ConnectorSessionPolicyScheduledReuse {
			t.Fatalf("normalize %q = %q", policy, got)
		}
	}
	if got := normalizeSessionPolicy(sdk.ConnectorSessionPolicyPerOperation); got != sdk.ConnectorSessionPolicyPerOperation {
		t.Fatalf("explicit per_operation normalized to %q", got)
	}
}

func TestScheduledReuseKeepsOneSessionForHighFrequencyDemand(t *testing.T) {
	var opened atomic.Int32
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-fast", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.10"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 100}),
	}
	for i := 0; i < 600; i++ {
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	if opened.Load() != 1 || closed.Load() != 0 {
		t.Fatalf("600 high-frequency batches opened=%d closed=%d, want 1/0 before stop", opened.Load(), closed.Load())
	}
	if err := connector.DestroyResource(handle); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("closed=%d, want 1 after stop", closed.Load())
	}
}

func TestScheduledReuseKeepsOneSessionFor500msDemand(t *testing.T) {
	var opened atomic.Int32
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-500ms", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.12"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-500ms", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 500}),
	}
	for i := 0; i < 10; i++ {
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if opened.Load() != 1 || closed.Load() != 0 {
		t.Fatalf("500ms demand opened=%d closed=%d, want 1/0", opened.Load(), closed.Load())
	}
	if err := connector.DestroyResource(handle); err != nil {
		t.Fatal(err)
	}
}

func TestScheduledReuseSerializesConcurrentBatches(t *testing.T) {
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		return nil, func() error { return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-serial", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.11"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 100}),
	}
	var active atomic.Int32
	var maxActive atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := connector.withClient(resource, req, func(gos7.Client) error {
				current := active.Add(1)
				for {
					previous := maxActive.Load()
					if current <= previous || maxActive.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				return nil
			}); err != nil {
				t.Errorf("batch failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxActive.Load() != 1 {
		t.Fatalf("overlapping batches=%d, want 1", maxActive.Load())
	}
}

func TestS7PackageSessionLimitIsBounded(t *testing.T) {
	var opened atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error { return nil }, nil
	})
	for i := 0; i < maxConcurrentS7Sessions; i++ {
		handle, err := connector.CreateResource(sdk.ConnectorResource{
			ID: fmt.Sprintf("s7-slot-%d", i), PluginID: "connect-s7",
			Connection: mustJSON(t, map[string]any{"host": fmt.Sprintf("192.0.2.%d", 50+i)}),
		})
		if err != nil {
			t.Fatal(err)
		}
		resource, _ := connector.getResource(handle)
		req := sdk.ConnectorRequest{
			ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
			Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 100}),
		}
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-slot-overflow", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.99"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	err = connector.withClient(resource, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestBatchRead, TimeoutMS: 5}, func(gos7.Client) error { return nil })
	if err == nil || !sdk.IsRetryable(err) {
		t.Fatalf("expected retryable session-limit error, got %v", err)
	}
	if opened.Load() != maxConcurrentS7Sessions {
		t.Fatalf("opened=%d, want bounded at %d", opened.Load(), maxConcurrentS7Sessions)
	}
	if err := connector.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestScheduledReuseClosesEachLowFrequencyBatch(t *testing.T) {
	var opened atomic.Int32
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-slow", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.20"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-slow", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 20000}),
	}
	for i := 0; i < 3; i++ {
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	if opened.Load() != 3 || closed.Load() != 3 {
		t.Fatalf("connections opened=%d closed=%d, want 3/3", opened.Load(), closed.Load())
	}
}

func TestScheduledReuseClosesAfterIdleHorizon(t *testing.T) {
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-idle", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{
			"host": "192.0.2.30", "sessionReuseHorizonMillis": 20,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 10}),
	}
	if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if closed.Load() != 1 {
		t.Fatalf("closed=%d, want 1 after idle horizon", closed.Load())
	}
}

func TestReleaseDemandClosesSessionImmediately(t *testing.T) {
	var closed atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		return nil, func() error { closed.Add(1); return nil }, nil
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-release", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{"host": "192.0.2.31"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{
		ResourceToken: "source-fast", Kind: sdk.ConnectorRequestBatchRead,
		Payload: mustJSON(t, map[string]any{"next_demand_in_ms": 100}),
	}
	if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := connector.releaseDemand(resource, "source-fast"); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("closed=%d, want immediate close after release", closed.Load())
	}
}

func TestS7CircuitBreakerSuppressesHighFrequencyDialStorm(t *testing.T) {
	var opened atomic.Int32
	connector := newS7Connector(func(s7ConnectionConfig) (gos7.Client, func() error, error) {
		opened.Add(1)
		return nil, nil, errors.New("PLC offline")
	})
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "s7-offline", PluginID: "connect-s7",
		Connection: mustJSON(t, map[string]any{
			"host": "192.0.2.40", "failureThreshold": 2, "retryBackoffMillis": 1000,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	req := sdk.ConnectorRequest{Kind: sdk.ConnectorRequestBatchRead}
	for i := 0; i < 3; i++ {
		if err := connector.withClient(resource, req, func(gos7.Client) error { return nil }); err == nil {
			t.Fatalf("operation %d unexpectedly succeeded", i)
		}
	}
	if opened.Load() != 2 {
		t.Fatalf("dial attempts=%d, want 2 before circuit suppresses calls", opened.Load())
	}
}

func TestCreateResourceValidatesHost(t *testing.T) {
	connector := NewConnector()
	_, err := connector.CreateResource(sdk.ConnectorResource{
		ID:         "s7-invalid",
		PluginID:   "connect-s7",
		Connection: mustJSON(t, map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "connection.host is required") {
		t.Fatalf("expected host validation error, got %v", err)
	}
}

func TestS7EncodeValueUsesConfiguredStringLengths(t *testing.T) {
	var helper gos7.Helper
	info := &s7DBInfo{Amount: 1}

	buf, err := s7EncodeValue(helper, info, "String", 20, "hello")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	if len(buf) != 22 {
		t.Fatalf("expected string buffer len 22, got %d", len(buf))
	}
	if got := helper.GetStringAt(buf, 0); got != "hello" {
		t.Fatalf("expected encoded string hello, got %q", got)
	}

	wbuf, err := s7EncodeValue(helper, info, "WString", 8, "配方A")
	if err != nil {
		t.Fatalf("encode wstring: %v", err)
	}
	if len(wbuf) != 20 {
		t.Fatalf("expected wstring buffer len 20, got %d", len(wbuf))
	}
	if got := helper.GetWStringAt(wbuf, 0); got != "配方A" {
		t.Fatalf("expected encoded wstring 配方A, got %q", got)
	}
}

func TestS7ExtractValueSupportsByteWordAndDWordAliases(t *testing.T) {
	var helper gos7.Helper
	info := &s7DBInfo{Amount: 1}

	byteValue, err := s7ExtractValue(helper, []byte{42}, info, "Byte")
	if err != nil || byteValue != uint8(42) {
		t.Fatalf("extract Byte: value=%v err=%v", byteValue, err)
	}

	wordBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(wordBuf, 65000)
	wordValue, err := s7ExtractValue(helper, wordBuf, info, "Word")
	if err != nil || wordValue != uint16(65000) {
		t.Fatalf("extract Word: value=%v err=%v", wordValue, err)
	}

	dwordBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(dwordBuf, 4000000000)
	dwordValue, err := s7ExtractValue(helper, dwordBuf, info, "DWord")
	if err != nil || dwordValue != uint32(4000000000) {
		t.Fatalf("extract DWord: value=%v err=%v", dwordValue, err)
	}
}

func TestS7WrapConnectError(t *testing.T) {
	err := s7WrapConnectError("127.0.0.1:10102", "op", errors.New(s7InvalidPDUText))
	if err == nil {
		t.Fatal("expected wrapped error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "connect_type=op") {
		t.Fatalf("expected connect_type in error, got %q", msg)
	}
	if !strings.Contains(msg, "standard S7 port is 102") {
		t.Fatalf("expected port hint in error, got %q", msg)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}
