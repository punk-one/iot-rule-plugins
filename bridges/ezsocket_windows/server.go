package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type readPoint struct {
	Method    string `json:"method"`
	Arguments []any  `json:"arguments"`
}
type device struct {
	ProgID        string               `json:"progid"`
	OpenMethod    string               `json:"open_method"`
	OpenArguments []any                `json:"open_arguments"`
	Points        map[string]readPoint `json:"points"`
}
type config struct {
	Listen    string            `json:"listen"`
	TLSCert   string            `json:"tls_cert"`
	TLSKey    string            `json:"tls_key"`
	TimeoutMS int               `json:"timeout_ms"`
	Devices   map[string]device `json:"devices"`
}
type job struct {
	ctx    context.Context
	device device
	result chan result
}
type result struct {
	data    map[string]any
	quality map[string]any
	err     error
}
type bridge struct {
	cfg   config
	token [32]byte
	jobs  chan job
	stop  context.Context
	read  func(device) result
}

var aliasPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var methodPattern = regexp.MustCompile(`^(Get|Read)[A-Za-z0-9_]{1,80}$`)

func validateArgs(values []any) error {
	for _, v := range values {
		switch n := v.(type) {
		case string:
			if len(n) > 1024 {
				return errors.New("COM argument string too large")
			}
		case bool:
		case float64:
			if math.IsNaN(n) || math.IsInf(n, 0) || math.Abs(n) > 2147483647 {
				return errors.New("COM numeric argument outside bounded range")
			}
		default:
			return errors.New("COM arguments must be strings, booleans or bounded numbers")
		}
	}
	return nil
}
func validate(cfg config) error {
	if cfg.Listen == "" || cfg.TimeoutMS < 100 || cfg.TimeoutMS > 30000 || len(cfg.Devices) < 1 || len(cfg.Devices) > 64 {
		return errors.New("invalid bridge limits")
	}
	for code, d := range cfg.Devices {
		if !aliasPattern.MatchString(code) || d.ProgID == "" || len(d.ProgID) > 256 || d.OpenMethod != "Open" || len(d.OpenArguments) > 8 || len(d.Points) < 1 || len(d.Points) > 128 {
			return errors.New("explicit verified COM ProgID/Open arguments and bounded points required")
		}
		if validateArgs(d.OpenArguments) != nil {
			return errors.New("invalid COM Open arguments")
		}
		for name, p := range d.Points {
			if validateArgs(p.Arguments) != nil || !aliasPattern.MatchString(name) || !methodPattern.MatchString(p.Method) || len(p.Arguments) > 8 {
				return errors.New("read aliases must map to bounded Get/Read methods")
			}
		}
	}
	return nil
}
func newBridge(cfg config, token string, read func(device) result, ctx context.Context) (*bridge, error) {
	if validate(cfg) != nil || len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("valid config and BRIDGE_TOKEN of 32..256 characters required")
	}
	b := &bridge{cfg: cfg, token: sha256.Sum256([]byte(token)), jobs: make(chan job, 4), stop: ctx, read: read}
	go b.worker()
	return b, nil
}
func (b *bridge) worker() {
	for {
		select {
		case <-b.stop.Done():
			return
		case request := <-b.jobs:
			if request.ctx.Err() != nil {
				continue
			}
			out := b.read(request.device)
			select {
			case request.result <- out:
			case <-request.ctx.Done():
			case <-b.stop.Done():
				return
			}
		}
	}
}
func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	token := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(token[:], b.token[:]) != 1 {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"read-only endpoint"}`, http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "devices" || parts[3] != "telemetry" {
		http.NotFound(w, r)
		return
	}
	code := parts[2]
	d, ok := b.cfg.Devices[code]
	if !ok {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(b.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	request := job{ctx: ctx, device: d, result: make(chan result, 1)}
	select {
	case <-b.stop.Done():
		http.Error(w, `{"error":"bridge stopping"}`, 503)
		return
	case b.jobs <- request:
	default:
		http.Error(w, `{"error":"device queue full"}`, 503)
		return
	}
	select {
	case <-ctx.Done():
		http.Error(w, `{"error":"COM batch deadline exceeded; supervisor may need to restart bridge"}`, 504)
	case <-b.stop.Done():
		http.Error(w, `{"error":"bridge stopping"}`, 503)
	case out := <-request.result:
		if out.err != nil {
			http.Error(w, `{"error":"COM connection/read failed"}`, 502)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "device_code": code, "data": out.data, "quality": out.quality, "time": time.Now().UnixMilli()})
	}
}
