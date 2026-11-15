package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	ConnectorRef     string `json:"connector_ref"`
	Query            string `json:"query"`
	DeviceCode       string `json:"device_code"`
	DeviceColumn     string `json:"device_code_column"`
	TimeColumn       string `json:"time_column"`
	Mode             string `json:"mode"`
	WatermarkColumn  string `json:"watermark_column"`
	InitialWatermark string `json:"initial_watermark"`
	CursorNamespace  string `json:"cursor_namespace"`
	StateDir         string `json:"state_dir"`
	IntervalMS       int    `json:"interval_ms"`
	TimeoutMS        int    `json:"timeout_ms"`
	PageSize         int    `json:"page_size"`
}
type plugin struct {
	sdk.BasePlugin
	cfg   config
	ctx   sdk.RuntimeContext
	db    *bolt.DB
	scope string
	stop  chan struct{}
	once  sync.Once
	wg    sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-relational-mssql", Name: "SQL Server Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded snapshot or opt-in ordered-integer incremental query with persistent cursor", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mssql"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Mode: "snapshot", DeviceColumn: "device_code", InitialWatermark: "0", CursorNamespace: "v1", StateDir: "./state/mssql-source", IntervalMS: 5000, TimeoutMS: 5000, PageSize: 100}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || strings.TrimSpace(p.cfg.Query) == "" || p.cfg.IntervalMS < 100 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.PageSize < 1 || p.cfg.PageSize > 10000 || (p.cfg.DeviceCode == "" && p.cfg.DeviceColumn == "") {
		return sdk.FatalError(errors.New("invalid SQL source settings"))
	}
	switch p.cfg.Mode {
	case "snapshot":
	case "incremental":
		if p.cfg.WatermarkColumn == "" || p.cfg.StateDir == "" || p.cfg.CursorNamespace == "" || !strings.Contains(strings.ToLower(p.cfg.Query), "@watermark") {
			return sdk.FatalError(errors.New("incremental mode requires cursor settings and @watermark parameter"))
		}
		if _, err := strconv.ParseInt(p.cfg.InitialWatermark, 10, 64); err != nil {
			return sdk.FatalError(errors.New("initial watermark must be a signed 64-bit integer string"))
		}
	default:
		return sdk.FatalError(errors.New("mode must be snapshot or incremental"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	if p.cfg.Mode == "incremental" {
		response, err := ctx.Connector().Read(sdk.ConnectorRequest{Target: "identity", TimeoutMS: p.cfg.TimeoutMS})
		if err != nil {
			return errors.New("cannot determine database identity for cursor scope")
		}
		var identity struct {
			Identity string `json:"identity"`
		}
		if json.Unmarshal(response.Payload, &identity) != nil || identity.Identity == "" {
			return errors.New("invalid database identity")
		}
		scope := sha256.Sum256([]byte(ctx.RuleID() + "\x00" + ctx.NodeID() + "\x00" + identity.Identity + "\x00" + p.cfg.Query + "\x00" + p.cfg.WatermarkColumn + "\x00" + p.cfg.CursorNamespace))
		p.scope = hex.EncodeToString(scope[:])
		if err = os.MkdirAll(p.cfg.StateDir, 0700); err != nil {
			return err
		}
		p.db, err = bolt.Open(filepath.Join(p.cfg.StateDir, p.scope[:32]+".db"), 0600, &bolt.Options{Timeout: time.Second})
		if err != nil {
			return errors.New("cannot open cursor state")
		}
		if err = p.db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("cursor")); return err }); err != nil {
			p.db.Close()
			return err
		}
	}
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	var result error
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
		p.wg.Wait()
		if p.db != nil {
			result = p.db.Close()
		}
	})
	return result
}
func (p *plugin) loop() {
	defer p.wg.Done()
	delay := time.Duration(0)
	failures := 0
	for {
		timer := time.NewTimer(delay)
		select {
		case <-p.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		started := time.Now()
		if err := p.poll(); err != nil {
			failures++
			p.ctx.Metrics().IncCounter("sql_poll_failures", nil)
		} else {
			failures = 0
		}
		delay = time.Duration(p.cfg.IntervalMS)*time.Millisecond - time.Since(started)
		if delay < 0 {
			delay = time.Duration(p.cfg.IntervalMS) * time.Millisecond
			p.ctx.Metrics().IncCounter("dropped_scans", nil)
		}
		if failures > 0 {
			backoff := time.Duration(1<<min(failures-1, 5)) * time.Second
			if delay < backoff {
				delay = backoff
			}
		}
	}
}
func cursorValue(value any) (int64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case json.Number:
		return v.Int64()
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 9007199254740991 || v != math.Trunc(v) {
			return 0, errors.New("unsafe cursor numeric value")
		}
		return int64(v), nil
	case int64:
		return v, nil
	}
	return 0, errors.New("cursor must be integer or decimal integer string")
}
func (p *plugin) cursor() (int64, error) {
	if p.cfg.Mode == "snapshot" {
		return 0, nil
	}
	value, err := strconv.ParseInt(p.cfg.InitialWatermark, 10, 64)
	if err != nil {
		return 0, err
	}
	if p.db == nil {
		return value, nil
	}
	err = p.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("cursor")).Get([]byte("value"))
		if raw != nil {
			var err error
			value, err = strconv.ParseInt(string(raw), 10, 64)
			return err
		}
		return nil
	})
	return value, err
}
func (p *plugin) poll() error {
	last, err := p.cursor()
	if err != nil {
		return err
	}
	params := map[string]any{"page_size": p.cfg.PageSize}
	if p.cfg.Mode == "incremental" {
		params["watermark"] = strconv.FormatInt(last, 10)
	}
	raw, _ := json.Marshal(map[string]any{"query": p.cfg.Query, "params": params, "max_rows": p.cfg.PageSize})
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "query", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return err
	}
	var decoded struct {
		Rows []map[string]any `json:"rows"`
	}
	if json.Unmarshal(response.Payload, &decoded) != nil || len(decoded.Rows) > p.cfg.PageSize {
		return errors.New("invalid SQL row batch")
	}
	type prepared struct {
		event  sdk.Event
		cursor int64
	}
	events := make([]prepared, 0, len(decoded.Rows))
	previous := last
	for _, row := range decoded.Rows {
		watermark := int64(0)
		if p.cfg.Mode == "incremental" {
			watermark, err = cursorValue(row[p.cfg.WatermarkColumn])
			if err != nil || watermark <= previous {
				return errors.New("incremental query must return strictly ascending unique watermark values above the saved cursor")
			}
			previous = watermark
		}
		device := p.cfg.DeviceCode
		if value, ok := row[p.cfg.DeviceColumn].(string); ok && value != "" {
			device = value
		}
		if device == "" {
			return errors.New("row has no device_code")
		}
		stamp := time.Now().UnixMilli()
		if p.cfg.TimeColumn != "" {
			value, exists := row[p.cfg.TimeColumn]
			if !exists {
				return errors.New("configured time column missing")
			}
			if text, ok := value.(string); ok {
				parsed, e := time.Parse(time.RFC3339Nano, text)
				if e != nil {
					return errors.New("time column must be RFC3339 or integer milliseconds")
				}
				stamp = parsed.UnixMilli()
			} else {
				stamp, err = cursorValue(value)
				if err != nil {
					return errors.New("invalid time column")
				}
			}
		}
		data := map[string]any{}
		for key, value := range row {
			if key != p.cfg.DeviceColumn && key != p.cfg.TimeColumn {
				data[key] = value
			}
		}
		event := sdk.NewEvent(map[string]any{"device_code": device, "data": data, "time": stamp}, nil)
		event.Timestamp = time.UnixMilli(stamp).UTC()
		if p.cfg.Mode == "incremental" {
			id := sha256.Sum256([]byte(p.scope + "\x00" + strconv.FormatInt(watermark, 10)))
			event.ID = "evt_sql_" + hex.EncodeToString(id[:])
		}
		sdk.SetTraceID(&event, event.ID)
		events = append(events, prepared{event: event, cursor: watermark})
	}
	for _, item := range events {
		if err = p.ctx.Emitter().Publish(item.event); err != nil {
			return err
		}
		if p.db != nil {
			if err = p.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("cursor")).Put([]byte("value"), []byte(strconv.FormatInt(item.cursor, 10)))
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
