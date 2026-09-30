package main

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sourceTestConnector struct {
	resp   sdk.ConnectorResponse
	status sdk.ResourceStatusEvent
	reqs   []sdk.ConnectorRequest
}

type blockingSourceConnector struct {
	sourceTestConnector
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingSourceConnector) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	payload, _ := json.Marshal(map[string]any{"values": map[string]any{"running": true}})
	return sdk.ConnectorResponse{Payload: payload}, nil
}

func (c *sourceTestConnector) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reqs = append(c.reqs, req)
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, sdk.ErrNotImplemented
}
func (c *sourceTestConnector) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}
func (c *sourceTestConnector) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reqs = append(c.reqs, req)
	return c.resp, nil
}

type sourceTestEmitter struct{ events []sdk.Event }

func (e *sourceTestEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sourceTestEmitter) Ack(eventID string) error            { return nil }
func (e *sourceTestEmitter) PublishAck(ack sdk.AckMessage) error { return nil }
func (e *sourceTestEmitter) EmitTo(label string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sourceTestEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type noopLogger struct{}

func (noopLogger) Debug(msg string, fields ...sdk.Field) {}
func (noopLogger) Info(msg string, fields ...sdk.Field)  {}
func (noopLogger) Warn(msg string, fields ...sdk.Field)  {}
func (noopLogger) Error(msg string, fields ...sdk.Field) {}
func (noopLogger) LogBatch(level sdk.LogLevel, messages []string, fields []map[string]interface{}) {
}

type noopMetrics struct{}

func (noopMetrics) IncCounter(name string, labels map[string]string)             {}
func (noopMetrics) Observe(name string, value float64, labels map[string]string) {}

type noopHealth struct{}

func (noopHealth) Report(report sdk.HealthReport) error                         { return nil }
func (noopHealth) Healthy(msg string, details map[string]string) error          { return nil }
func (noopHealth) Degraded(code, msg string, details map[string]string) error   { return nil }
func (noopHealth) Recovering(code, msg string, details map[string]string) error { return nil }
func (noopHealth) Unhealthy(code, msg string, details map[string]string) error  { return nil }
func (noopHealth) Heartbeat(msg string, details map[string]string) error        { return nil }
func (noopHealth) Stopped(msg string, details map[string]string) error          { return nil }

type sourceRuntimeContext struct {
	connector sdk.ConnectorClient
	emitter   sdk.Emitter
	events    <-chan sdk.ResourceStatusEvent
}

func (c sourceRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sourceRuntimeContext) NodeID() string                                 { return "source-node" }
func (c sourceRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c sourceRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c sourceRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c sourceRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c sourceRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sourceRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return c.events }

func TestSourceConfigRequiresConnectorRef(t *testing.T) {
	var source S7Source
	err := source.validateConfig([]byte(`{"output":{"device_code":"dev1"},"points":[{"address":"DB1.DBX0.0","name":"x","value_type":"Bool"}]}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestSourceConfigSchemaIsValidJSON(t *testing.T) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(s7SourceConfigSchema), &schema); err != nil {
		t.Fatalf("source-s7 config schema is invalid JSON: %v", err)
	}
}

func TestSourcePollUsesConnectorAndPublishesEvent(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"values": map[string]any{
			"running": true,
			"temp":    23.5,
		},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: payload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "s7-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &S7Source{
		cfg: S7SourceConfig{
			ConnectorRef: "s7-1",
			Output:       outputConf{DeviceCode: "dev-1", IncludeMeta: true},
			Points: []PointDef{
				{Address: "DB1.DBX0.0", Name: "running", ValueType: "Bool"},
			},
		},
		ctx:    sourceRuntimeContext{connector: connector, emitter: emitter},
		stopCh: make(chan struct{}),
	}
	source.setStatus(sdk.ResourceStatusHealthy)

	congested, err := source.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if congested {
		t.Fatal("did not expect congested")
	}
	if len(connector.reqs) != 1 {
		t.Fatalf("expected one connector request, got %d", len(connector.reqs))
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(emitter.events))
	}
	got := emitter.events[0].Payload["data"].(map[string]interface{})
	if got["running"] != true {
		t.Fatalf("unexpected payload: %+v", emitter.events[0].Payload)
	}
}

func TestSourceStopReleasesScheduledDemand(t *testing.T) {
	connector := &sourceTestConnector{}
	source := &S7Source{
		ctx:    sourceRuntimeContext{connector: connector, emitter: &sourceTestEmitter{}},
		stopCh: make(chan struct{}),
	}
	if err := source.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := source.Stop(); err != nil {
		t.Fatal(err)
	}
	if len(connector.reqs) != 1 || connector.reqs[0].Target != "release_demand" {
		t.Fatalf("unexpected stop requests: %+v", connector.reqs)
	}
}

func TestSourceScanTimeoutUsesCycleBudget(t *testing.T) {
	source := &S7Source{}
	source.cfg.interval = 100 * time.Millisecond
	if got := source.scanTimeoutMS(); got != 80 {
		t.Fatalf("timeout=%dms, want 80ms", got)
	}
	source.cfg.interval = 20 * time.Millisecond
	if got := source.scanTimeoutMS(); got != 50 {
		t.Fatalf("minimum timeout=%dms, want 50ms", got)
	}
}

func TestSourceStopWaitsForInFlightPollAndPreventsPublish(t *testing.T) {
	connector := &blockingSourceConnector{started: make(chan struct{}), release: make(chan struct{})}
	emitter := &sourceTestEmitter{}
	source := &S7Source{cfg: S7SourceConfig{
		ConnectorRef: "s7-1",
		Output:       outputConf{DeviceCode: "dev-1"},
		Points:       []PointDef{{Address: "DB1.DBX0.0", Name: "running", ValueType: "Bool"}},
		interval:     time.Millisecond,
	}}
	if err := source.Start(sourceRuntimeContext{connector: connector, emitter: emitter}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.started:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- source.Stop() }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while the poll was still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(connector.release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if len(emitter.events) != 0 {
		t.Fatalf("published %d events after stop began", len(emitter.events))
	}
}

func TestSourceNextDelayBacksOffOnDisconnected(t *testing.T) {
	source := &S7Source{}
	source.cfg.Polling.IntervalMs = 1000
	source.cfg.Polling.MaxBackoffMs = 10000
	source.setStatus(sdk.ResourceStatusDisconnected)

	delay := source.nextDelay(1)
	if delay < 8*time.Second {
		t.Fatalf("expected disconnected backoff, got %s", delay)
	}
}

func TestSourcePollRetriesDisconnectedOnDemandConnector(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"values": map[string]any{"running": true}})
	connector := &sourceTestConnector{
		resp:   sdk.ConnectorResponse{Payload: payload},
		status: sdk.ResourceStatusEvent{ResourceID: "s7-1", Status: sdk.ResourceStatusDisconnected},
	}
	emitter := &sourceTestEmitter{}
	source := &S7Source{
		cfg: S7SourceConfig{
			ConnectorRef: "s7-1",
			Output:       outputConf{DeviceCode: "dev-1"},
			Points:       []PointDef{{Address: "DB1.DBX0.0", Name: "running", ValueType: "Bool"}},
		},
		ctx:    sourceRuntimeContext{connector: connector, emitter: emitter},
		stopCh: make(chan struct{}),
	}
	source.setStatus(sdk.ResourceStatusDisconnected)

	if _, err := source.poll(); err != nil {
		t.Fatalf("disconnected on-demand resource should retry a real read: %v", err)
	}
	if len(connector.reqs) != 1 || len(emitter.events) != 1 {
		t.Fatalf("requests=%d events=%d, want 1/1", len(connector.reqs), len(emitter.events))
	}
}

func TestSourceConfigSupportsMultiplePointsIntervalPrecisionAndRange(t *testing.T) {
	var source S7Source
	err := source.validateConfig([]byte(`{
		"connector_ref":"s7-cnc18",
		"output":{"device_code":"cnc18"},
		"polling":{"interval":"5s","batch_size":19,"max_backoff_ms":1000},
		"points":[
			{"name":"total_output","address":"DB24.DBD440","value_type":"Uint32","read_write":"RW","range":{"minimum":0,"maximum":1000000,"policy":"use_default","default_value":0}},
			{"name":"temperature","address":"DB1.DBD0","value_type":"Float32","scale":0.1,"precision":2}
		]
	}`))
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if len(source.cfg.Points) != 2 {
		t.Fatalf("expected two points, got %d", len(source.cfg.Points))
	}
	if source.cfg.Points[0].ValueType != "Uint32" || source.cfg.Points[0].ReadWrite != "RW" {
		t.Fatalf("unexpected normalized point: %+v", source.cfg.Points[0])
	}
	if source.cfg.interval != 5*time.Second {
		t.Fatalf("expected 5s interval, got %s", source.cfg.interval)
	}
	if source.cfg.Polling.MaxBackoffMs != 5000 {
		t.Fatalf("max backoff must not be shorter than polling interval, got %d", source.cfg.Polling.MaxBackoffMs)
	}
}

func TestSourceConfigRejectsDuplicatePointNames(t *testing.T) {
	var source S7Source
	err := source.validateConfig([]byte(`{
		"connector_ref":"s7-1","output":{"device_code":"cnc18"},
		"points":[
			{"name":"x","address":"DB1.DBW0","value_type":"Int16"},
			{"name":"x","address":"DB1.DBW2","value_type":"Int16"}
		]
	}`))
	if err == nil || !strings.Contains(err.Error(), "must be unique") {
		t.Fatalf("expected duplicate-name error, got %v", err)
	}
}

func TestApplyPointPolicyScalePrecisionAndFallback(t *testing.T) {
	scale := 0.1
	precision := 2
	minimum, maximum := 0.0, 10.0
	point := PointDef{
		Name: "pressure", ValueType: "Float32", Scale: &scale, Precision: &precision,
		Range: &PointRange{Minimum: &minimum, Maximum: &maximum, Policy: rangePolicyUseDefault, DefaultValue: 1.25},
	}

	value, keep, quality, err := applyPointPolicy(123.456, point)
	if err != nil || !keep || quality != "defaulted" || value != 1.25 {
		t.Fatalf("unexpected fallback result value=%v keep=%v quality=%q err=%v", value, keep, quality, err)
	}

	value, keep, quality, err = applyPointPolicy(31.46, point)
	if err != nil || !keep || quality != "" || math.Abs(value.(float64)-3.15) > 0.000001 {
		t.Fatalf("unexpected transform result value=%v keep=%v quality=%q err=%v", value, keep, quality, err)
	}
}

func TestApplyPointPolicyClampAndDrop(t *testing.T) {
	minimum, maximum := 0.0, 100.0
	clampPoint := PointDef{Name: "x", ValueType: "Float64", Range: &PointRange{Minimum: &minimum, Maximum: &maximum, Policy: rangePolicyClamp}}
	value, keep, quality, err := applyPointPolicy(120.0, clampPoint)
	if err != nil || !keep || quality != "clamped" || value != maximum {
		t.Fatalf("unexpected clamp result value=%v keep=%v quality=%q err=%v", value, keep, quality, err)
	}

	dropPoint := PointDef{Name: "x", ValueType: "Float64", Range: &PointRange{Minimum: &minimum, Maximum: &maximum, Policy: rangePolicyDrop}}
	_, keep, quality, err = applyPointPolicy(-1.0, dropPoint)
	if err != nil || keep || quality != "dropped" {
		t.Fatalf("unexpected drop result keep=%v quality=%q err=%v", keep, quality, err)
	}
}

func TestSourcePollSendsProtocolOnlyPointsAndAppliesPolicies(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"values": map[string]any{"temperature": 234.56}})
	connector := &sourceTestConnector{
		resp:   payloadResponse(payload),
		status: sdk.ResourceStatusEvent{ResourceID: "s7-1", Status: sdk.ResourceStatusHealthy},
	}
	emitter := &sourceTestEmitter{}
	scale := 0.1
	precision := 1
	source := &S7Source{
		cfg: S7SourceConfig{
			ConnectorRef: "s7-1", Output: outputConf{DeviceCode: "cnc18"},
			Points: []PointDef{{Address: "DB1.DBD0", Name: "temperature", ValueType: "Float32", Scale: &scale, Precision: &precision}},
		},
		ctx: sourceRuntimeContext{connector: connector, emitter: emitter}, stopCh: make(chan struct{}),
	}
	source.setStatus(sdk.ResourceStatusHealthy)
	if _, err := source.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	var request sourceReadPayload
	if err := json.Unmarshal(connector.reqs[0].Payload, &request); err != nil {
		t.Fatalf("decode connector request: %v", err)
	}
	requestJSON := string(connector.reqs[0].Payload)
	if strings.Contains(requestJSON, "scale") || strings.Contains(requestJSON, "precision") || strings.Contains(requestJSON, "range") {
		t.Fatalf("connector request leaked source policy: %s", requestJSON)
	}
	if request.NextDemandInMS != 1000 {
		t.Fatalf("next demand hint=%dms, want 1000ms", request.NextDemandInMS)
	}
	data := emitter.events[0].Payload["data"].(map[string]interface{})
	if got := data["temperature"]; got != 23.5 {
		t.Fatalf("expected transformed value 23.5, got %v", got)
	}
	if len(emitter.events[0].Payload) != 3 {
		t.Fatalf("expected exact device_code/data/time payload, got %+v", emitter.events[0].Payload)
	}
}

func TestAdvanceNextDueDropsExpiredScanWindows(t *testing.T) {
	start := time.Unix(100, 0)
	next, dropped := advanceNextDue(start, start.Add(350*time.Millisecond), 100*time.Millisecond)
	if dropped != 3 {
		t.Fatalf("dropped=%d, want 3", dropped)
	}
	if want := start.Add(400 * time.Millisecond); !next.Equal(want) {
		t.Fatalf("next=%s, want %s", next, want)
	}
}

func payloadResponse(payload []byte) sdk.ConnectorResponse {
	return sdk.ConnectorResponse{Payload: payload}
}
