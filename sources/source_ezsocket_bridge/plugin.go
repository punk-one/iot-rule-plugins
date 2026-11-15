package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`

	IntervalMS int `json:"interval_ms"`
	TimeoutMS  int `json:"timeout_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg  config
	ctx  sdk.RuntimeContext
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
	etag string
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-ezsocket-bridge", Name: "EZSocket Bridge Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Poll an authenticated Windows EZSocket read bridge and validate its versioned telemetry envelope", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"http"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{IntervalMS: 5000, TimeoutMS: 5000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(p.cfg.DeviceCode) || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid HTTP polling config"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.etag = ""
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.wg.Wait()
	return nil
}
func (p *plugin) loop() {
	defer p.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
	next := time.Now()
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		if err := p.poll(); err != nil {
			failures++
			p.ctx.Metrics().IncCounter("poll_failures", nil)
		} else {
			failures = 0
		}
		next = next.Add(interval)
		now := time.Now()
		if !next.After(now) {
			skipped := now.Sub(next)/interval + 1
			next = next.Add(skipped * interval)
			p.ctx.Metrics().Observe("dropped_scans", float64(skipped), nil)
		}
		if failures > 0 {
			retry := now.Add(time.Duration(1<<min(failures-1, 5)) * time.Second)
			if next.Before(retry) {
				next = retry
			}
		}
		timer.Reset(time.Until(next))
	}
}
func (p *plugin) poll() error {
	headers := map[string]string{}
	if p.etag != "" {
		headers["If-None-Match"] = p.etag
	}
	raw, _ := json.Marshal(map[string]any{"method": "GET", "path": "/v1/devices/" + url.PathEscape(p.cfg.DeviceCode) + "/telemetry", "headers": headers})
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "request", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return err
	}
	var decoded struct {
		Status      int    `json:"status"`
		Body        string `json:"body_base64"`
		ContentType string `json:"content_type"`
		ETag        string `json:"etag"`
	}
	if err = json.Unmarshal(response.Payload, &decoded); err != nil {
		return err
	}
	if decoded.Status == 304 {
		return nil
	}
	if decoded.Status < 200 || decoded.Status >= 300 {
		return errors.New("HTTP polling non-success status")
	}
	data, err := decodeBody(decoded.Body, decoded.ContentType)
	if err != nil {
		return err
	}
	if data["schema_version"] != float64(1) || data["device_code"] != p.cfg.DeviceCode {
		return errors.New("bridge schema/device identity mismatch")
	}
	if _, ok := data["data"].(map[string]any); !ok {
		return errors.New("bridge data object required")
	}
	if _, ok := data["quality"].(map[string]any); !ok {
		return errors.New("bridge quality object required")
	}
	stamp, ok := data["time"].(float64)
	if !ok || math.IsNaN(stamp) || math.IsInf(stamp, 0) || stamp <= 0 || stamp > 9007199254740991 || math.Trunc(stamp) != stamp {
		return errors.New("bridge timestamp must be integer milliseconds")
	}
	event := sdk.NewEvent(data, map[string]string{"protocol": "ezsocket-bridge"})
	sdk.SetTraceID(&event, event.ID)
	if err = p.ctx.Emitter().Publish(event); err != nil {
		return err
	}
	p.etag = decoded.ETag
	return nil
}
func decodeBody(encoded, contentType string) (map[string]any, error) {
	body, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("invalid body encoding")
	}
	if strings.Contains(strings.ToLower(contentType), "json") {
		var value any
		if value, err = decodeSafeJSON(body); err != nil {
			return nil, errors.New("invalid JSON body")
		}
		if object, ok := value.(map[string]any); ok {
			return object, nil
		}
		return map[string]any{"value": value}, nil
	}
	return map[string]any{"value": string(body)}, nil
}

func decodeSafeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid JSON body")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return safeJSONValue(value, 0)
}
func safeJSONValue(value any, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting limit exceeded")
	}
	switch v := value.(type) {
	case json.Number:
		n, err := v.Float64()
		if err != nil || math.Abs(n) > 9007199254740991 {
			return string(v), nil
		}
		return n, nil
	case []any:
		if len(v) > 4096 {
			return nil, errors.New("JSON array item limit exceeded")
		}
		for i, x := range v {
			next, err := safeJSONValue(x, depth+1)
			if err != nil {
				return nil, err
			}
			v[i] = next
		}
		return v, nil
	case map[string]any:
		if len(v) > 4096 {
			return nil, errors.New("JSON object field limit exceeded")
		}
		for k, x := range v {
			next, err := safeJSONValue(x, depth+1)
			if err != nil {
				return nil, err
			}
			v[k] = next
		}
		return v, nil
	default:
		return value, nil
	}
}
