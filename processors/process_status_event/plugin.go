package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const statusEventConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "state_key_prefix": {
      "type": "string",
      "default": "iot:device:event_state:"
    },
    "output": {
      "type": "object",
      "properties": {
        "redis_label": {"type": "string", "default": "redis"},
        "event_label": {"type": "string", "default": "events"},
        "redis_key_prefix": {"type": "string", "default": "iot:device:status:"}
      },
      "additionalProperties": false
    },
    "state": {
      "type": "object",
      "properties": {
        "device_code_field": {"type": "string", "default": "payload.device_code"},
        "time_field": {"type": "string", "default": "payload.time"},
        "online_field": {"type": "string", "default": "payload.data.online"},
        "connection_state_field": {"type": "string", "default": "payload.data.connection_state"}
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

type StatusConfig struct {
	StateKeyPrefix string `json:"state_key_prefix,omitempty"`
	Output         struct {
		RedisLabel     string `json:"redis_label,omitempty"`
		EventLabel     string `json:"event_label,omitempty"`
		RedisKeyPrefix string `json:"redis_key_prefix,omitempty"`
	} `json:"output,omitempty"`
	State struct {
		DeviceCodeField      string `json:"device_code_field,omitempty"`
		TimeField            string `json:"time_field,omitempty"`
		OnlineField          string `json:"online_field,omitempty"`
		ConnectionStateField string `json:"connection_state_field,omitempty"`
	} `json:"state,omitempty"`
}

type StatusEventPlugin struct {
	sdk.BasePlugin
	cfg      StatusConfig
	ctx      sdk.RuntimeContext
	stateMgr sdk.StateManager
}

type statusDeviceState struct {
	Active             map[string]statusActiveEvent `json:"active"`
	LastEventAt        int64                        `json:"last_event_at,omitempty"`
	LastStatusChangeAt int64                        `json:"last_status_change_at,omitempty"`
}

type statusActiveEvent struct {
	EventInstanceID  string `json:"event_instance_id"`
	EventKey         string `json:"event_key"`
	EventIdentifier  string `json:"event_identifier"`
	EventName        string `json:"event_name"`
	Category         string `json:"category"`
	Severity         string `json:"severity"`
	Message          string `json:"message"`
	RuleCode         string `json:"rule_code,omitempty"`
	MetricIdentifier string `json:"metric_identifier,omitempty"`
	Source           string `json:"source"`
	StartedAt        int64  `json:"started_at"`
	LastSeenAt       int64  `json:"last_seen_at"`
	PayloadJSON      string `json:"payload_json,omitempty"`
	Signature        string `json:"signature"`
}

type statusDesiredEvent struct {
	EventKey         string
	EventIdentifier  string
	EventName        string
	Category         string
	Severity         string
	Message          string
	RuleCode         string
	MetricIdentifier string
	Source           string
	Payload          map[string]interface{}
	Signature        string
}

type statusEmittedEventRow struct {
	DeviceCode string
	OccurredAt time.Time
	Data       map[string]interface{}
}

func (p *StatusEventPlugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "process-status-event",
		Name:        "Process Status Event",
		Version:     "1.0.0",
		Description: "Derive device lifecycle events from snake_case status messages and emit Redis/TimescaleDB payloads",
		Type:        sdk.PluginTypeProcessor,
		Category:    "stateful",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  2,
			SupportBatch: false,
			SupportAck:   false,
			ConfigSchema: statusEventConfigSchema,
		},
	}
}

func (p *StatusEventPlugin) Init(cfg sdk.PluginConfig) error {
	if err := json.Unmarshal(cfg.Raw, &p.cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if p.cfg.StateKeyPrefix == "" {
		p.cfg.StateKeyPrefix = "iot:device:event_state:"
	}
	if p.cfg.Output.RedisLabel == "" {
		p.cfg.Output.RedisLabel = "redis"
	}
	if p.cfg.Output.EventLabel == "" {
		p.cfg.Output.EventLabel = "events"
	}
	if p.cfg.Output.RedisKeyPrefix == "" {
		p.cfg.Output.RedisKeyPrefix = "iot:device:status:"
	}
	if p.cfg.State.DeviceCodeField == "" {
		p.cfg.State.DeviceCodeField = "payload.device_code"
	}
	if p.cfg.State.TimeField == "" {
		p.cfg.State.TimeField = "payload.time"
	}
	if p.cfg.State.OnlineField == "" {
		p.cfg.State.OnlineField = "payload.data.online"
	}
	if p.cfg.State.ConnectionStateField == "" {
		p.cfg.State.ConnectionStateField = "payload.data.connection_state"
	}
	return nil
}

func (p *StatusEventPlugin) Start(ctx sdk.RuntimeContext) error {
	type stateContext interface{ State() sdk.StateManager }
	impl, ok := ctx.(stateContext)
	if !ok || impl.State() == nil {
		return fmt.Errorf("state manager not available")
	}
	p.ctx = ctx
	p.stateMgr = impl.State()
	ctx.Logger().Info("process-status-event started",
		sdk.Field{Key: "rule_id", Value: ctx.RuleID()},
		sdk.Field{Key: "node_id", Value: ctx.NodeID()})
	return nil
}

func (p *StatusEventPlugin) Stop() error {
	return nil
}

func (p *StatusEventPlugin) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *StatusEventPlugin) OnEvent(e sdk.Event) error {
	deviceCode := strings.TrimSpace(statusAsString(statusGetPathValue(statusEventContext(e), p.cfg.State.DeviceCodeField)))
	if deviceCode == "" {
		return nil
	}
	occurredAt, occurredMS := statusResolveEventTime(e, p.cfg.State.TimeField)
	online, _ := statusAsBool(statusGetPathValue(statusEventContext(e), p.cfg.State.OnlineField))
	connectionState := strings.ToLower(strings.TrimSpace(statusAsString(statusGetPathValue(statusEventContext(e), p.cfg.State.ConnectionStateField))))
	offline := !online || connectionState == "disconnected"

	current, err := p.loadState(deviceCode)
	if err != nil {
		return err
	}

	desired := statusDesiredEvent{
		EventKey:         "status.offline",
		EventIdentifier:  "device_offline",
		EventName:        "设备离线",
		Category:         "status",
		Severity:         "alarm",
		Message:          statusTernaryString(offline, "device offline", "device back online"),
		MetricIdentifier: "online",
		Source:           "status_report",
		Payload: map[string]interface{}{
			"online":           online,
			"connection_state": connectionState,
			"raw":              e.Payload,
		},
		Signature: statusStableSignature(map[string]interface{}{
			"online":           online,
			"connection_state": connectionState,
		}),
	}

	current.ensureActive()
	existing, exists := current.Active[desired.EventKey]
	rows := make([]statusEmittedEventRow, 0, 1)
	changed := false

	switch {
	case offline && !exists:
		active, row := statusStartEvent(deviceCode, occurredAt, occurredMS, desired)
		current.Active[desired.EventKey] = active
		current.LastEventAt = occurredMS
		current.LastStatusChangeAt = occurredMS
		rows = append(rows, row)
		changed = true
	case offline && exists && existing.Signature != desired.Signature:
		active, row := statusUpdateEvent(deviceCode, occurredAt, occurredMS, existing, desired)
		current.Active[desired.EventKey] = active
		current.LastEventAt = occurredMS
		current.LastStatusChangeAt = occurredMS
		rows = append(rows, row)
		changed = true
	case !offline && exists:
		delete(current.Active, desired.EventKey)
		current.LastEventAt = occurredMS
		current.LastStatusChangeAt = occurredMS
		rows = append(rows, statusEndEvent(deviceCode, occurredAt, occurredMS, existing, desired.Message, desired.Payload))
		changed = true
	}

	if !changed {
		return nil
	}
	if err := p.saveState(deviceCode, current); err != nil {
		return err
	}
	if err := p.emitRedisProjection(deviceCode, current, occurredMS); err != nil {
		return err
	}
	return p.emitEventRows(rows, e)
}

func (p *StatusEventPlugin) emitRedisProjection(deviceCode string, state statusDeviceState, updatedAt int64) error {
	payload := map[string]interface{}{
		"key": p.cfg.Output.RedisKeyPrefix + deviceCode,
		"fields": map[string]interface{}{
			"device_code":           deviceCode,
			"active_events":         statusActiveEventsProjection(state.Active, ""),
			"alarms":                statusActiveEventsProjection(state.Active, "alarm"),
			"faults":                statusActiveEventsProjection(state.Active, "fault"),
			"last_event_at":         state.LastEventAt,
			"last_status_change_at": state.LastStatusChangeAt,
			"updated_at":            updatedAt,
		},
	}
	out := sdk.NewEvent(payload, nil)
	out.Timestamp = statusUnixMilli(updatedAt)
	out.RuleID = p.ctx.RuleID()
	out.Source = p.ctx.NodeID()
	return p.ctx.Emitter().EmitTo(p.cfg.Output.RedisLabel, out)
}

func (p *StatusEventPlugin) emitEventRows(rows []statusEmittedEventRow, in sdk.Event) error {
	if len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if traceID := sdk.GetTraceID(in); traceID != "" {
			row.Data["trace_id"] = traceID
		}
		out := sdk.NewEvent(map[string]interface{}{
			"device_code": row.DeviceCode,
			"data":        row.Data,
		}, statusCloneMetadata(in.Metadata))
		out.Timestamp = row.OccurredAt
		out.RuleID = p.ctx.RuleID()
		out.Source = p.ctx.NodeID()
		if traceID := sdk.GetTraceID(in); traceID != "" {
			sdk.SetTraceID(&out, traceID)
		}
		if err := p.ctx.Emitter().EmitTo(p.cfg.Output.EventLabel, out); err != nil {
			return err
		}
	}
	return nil
}

func (p *StatusEventPlugin) stateKey(deviceCode string) string {
	return p.cfg.StateKeyPrefix + deviceCode
}

func (p *StatusEventPlugin) loadState(deviceCode string) (statusDeviceState, error) {
	state := statusDeviceState{Active: map[string]statusActiveEvent{}}
	if err := p.stateMgr.GetState(p.stateKey(deviceCode), &state); err != nil {
		return state, nil
	}
	state.ensureActive()
	return state, nil
}

func (p *StatusEventPlugin) saveState(deviceCode string, state statusDeviceState) error {
	state.ensureActive()
	return p.stateMgr.SetState(p.stateKey(deviceCode), state)
}

func (s *statusDeviceState) ensureActive() {
	if s.Active == nil {
		s.Active = map[string]statusActiveEvent{}
	}
}

func statusStartEvent(deviceCode string, occurredAt time.Time, occurredMS int64, next statusDesiredEvent) (statusActiveEvent, statusEmittedEventRow) {
	instanceID := "evt_" + uuid.NewString()
	payloadJSON := statusMarshalJSON(next.Payload)
	active := statusActiveEvent{
		EventInstanceID:  instanceID,
		EventKey:         next.EventKey,
		EventIdentifier:  next.EventIdentifier,
		EventName:        next.EventName,
		Category:         next.Category,
		Severity:         next.Severity,
		Message:          next.Message,
		RuleCode:         next.RuleCode,
		MetricIdentifier: next.MetricIdentifier,
		Source:           next.Source,
		StartedAt:        occurredMS,
		LastSeenAt:       occurredMS,
		PayloadJSON:      payloadJSON,
		Signature:        next.Signature,
	}
	return active, statusBuildEventRow(deviceCode, occurredAt, active, "start", "active", payloadJSON, 0, nil)
}

func statusUpdateEvent(deviceCode string, occurredAt time.Time, occurredMS int64, current statusActiveEvent, next statusDesiredEvent) (statusActiveEvent, statusEmittedEventRow) {
	current.EventIdentifier = next.EventIdentifier
	current.EventName = next.EventName
	current.Category = next.Category
	current.Severity = next.Severity
	current.Message = next.Message
	current.RuleCode = next.RuleCode
	current.MetricIdentifier = next.MetricIdentifier
	current.Source = next.Source
	current.LastSeenAt = occurredMS
	current.PayloadJSON = statusMarshalJSON(next.Payload)
	current.Signature = next.Signature
	return current, statusBuildEventRow(deviceCode, occurredAt, current, "update", "active", current.PayloadJSON, 0, nil)
}

func statusEndEvent(deviceCode string, occurredAt time.Time, occurredMS int64, current statusActiveEvent, message string, payload map[string]interface{}) statusEmittedEventRow {
	duration := occurredMS - current.StartedAt
	if duration < 0 {
		duration = 0
	}
	current.Message = message
	payloadJSON := statusMarshalJSON(payload)
	return statusBuildEventRow(deviceCode, occurredAt, current, "end", "closed", payloadJSON, duration, &occurredAt)
}

func statusBuildEventRow(deviceCode string, occurredAt time.Time, current statusActiveEvent, phase string, status string, payloadJSON string, durationMS int64, endedAt *time.Time) statusEmittedEventRow {
	data := map[string]interface{}{
		"event_instance_id": current.EventInstanceID,
		"event_key":         current.EventKey,
		"event_identifier":  current.EventIdentifier,
		"event_name":        current.EventName,
		"category":          current.Category,
		"severity":          current.Severity,
		"phase":             phase,
		"status":            status,
		"message":           current.Message,
		"started_at":        statusUnixMilli(current.StartedAt),
		"rule_code":         current.RuleCode,
		"metric_identifier": current.MetricIdentifier,
		"payload":           payloadJSON,
		"source":            current.Source,
		"trace_id":          "",
	}
	if endedAt != nil {
		data["ended_at"] = *endedAt
		data["duration_ms"] = durationMS
	}
	return statusEmittedEventRow{DeviceCode: deviceCode, OccurredAt: occurredAt, Data: data}
}

func statusActiveEventsProjection(active map[string]statusActiveEvent, category string) []map[string]interface{} {
	keys := make([]string, 0, len(active))
	for key, item := range active {
		if category != "" && item.Category != category {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		item := active[key]
		projection := map[string]interface{}{
			"event_key":         item.EventKey,
			"event_identifier":  item.EventIdentifier,
			"event_name":        item.EventName,
			"event_instance_id": item.EventInstanceID,
			"category":          item.Category,
			"severity":          item.Severity,
			"message":           item.Message,
			"rule_code":         item.RuleCode,
			"metric_identifier": item.MetricIdentifier,
			"started_at":        item.StartedAt,
			"last_seen_at":      item.LastSeenAt,
		}
		if item.PayloadJSON != "" {
			projection["payload"] = item.PayloadJSON
		}
		out = append(out, projection)
	}
	return out
}

func statusEventContext(e sdk.Event) map[string]interface{} {
	meta := map[string]interface{}{}
	for key, value := range e.Metadata {
		meta[key] = value
	}
	return map[string]interface{}{
		"payload":  e.Payload,
		"metadata": meta,
		"meta":     meta,
	}
}

func statusGetPathValue(root map[string]interface{}, path string) interface{} {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	parts := strings.Split(path, ".")
	var current interface{} = root
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]interface{}:
			current = typed[part]
		case map[string]string:
			current = typed[part]
		default:
			return nil
		}
	}
	return current
}

func statusResolveEventTime(e sdk.Event, path string) (time.Time, int64) {
	if raw := statusGetPathValue(statusEventContext(e), path); raw != nil {
		if ts, ms, ok := statusParseTimestamp(raw); ok {
			return ts, ms
		}
	}
	ts := e.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return ts.UTC(), ts.UTC().UnixMilli()
}

func statusParseTimestamp(value interface{}) (time.Time, int64, bool) {
	switch typed := value.(type) {
	case float64:
		ms := int64(typed)
		return statusUnixMilli(ms), ms, true
	case int64:
		return statusUnixMilli(typed), typed, true
	case int:
		ms := int64(typed)
		return statusUnixMilli(ms), ms, true
	case json.Number:
		if v, err := typed.Int64(); err == nil {
			return statusUnixMilli(v), v, true
		}
	case string:
		typed = strings.TrimSpace(typed)
		if typed == "" {
			return time.Time{}, 0, false
		}
		if v, err := strconv.ParseInt(typed, 10, 64); err == nil {
			return statusUnixMilli(v), v, true
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed.UTC(), parsed.UTC().UnixMilli(), true
			}
		}
	}
	return time.Time{}, 0, false
}

func statusUnixMilli(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

func statusAsString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", value)
	}
}

func statusAsBool(value interface{}) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case float64:
		return typed != 0, true
	case int:
		return typed != 0, true
	case int64:
		return typed != 0, true
	case json.Number:
		if v, err := typed.Int64(); err == nil {
			return v != 0, true
		}
	case string:
		s := strings.TrimSpace(strings.ToLower(typed))
		switch s {
		case "true", "1", "yes", "y", "on", "connected":
			return true, true
		case "false", "0", "no", "n", "off", "disconnected":
			return false, true
		}
	}
	return false, false
}

func statusStableSignature(value interface{}) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(raw)
}

func statusMarshalJSON(value interface{}) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func statusCloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func statusTernaryString(cond bool, left string, right string) string {
	if cond {
		return left
	}
	return right
}
