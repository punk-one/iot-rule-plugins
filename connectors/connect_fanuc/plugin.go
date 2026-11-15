package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

type pmcRange struct {
	Area  int `json:"area"`
	Start int `json:"start"`
	End   int `json:"end"`
}
type config struct {
	ReadPMCRanges []pmcRange `json:"readable_pmc_ranges"`
	Host          string     `json:"host"`
	Port          int        `json:"port"`
	TimeoutMS     int        `json:"timeout_ms"`
	Path          int        `json:"cnc_path"`
	Library       string     `json:"library_path"`
	SHA256        string     `json:"library_sha256"`
	ABI           string     `json:"abi"`
	WriteMacros   []int      `json:"writable_macros"`
}
type point struct {
	PMCArea  int    `json:"pmc_area,omitempty"`
	Name     string `json:"name"`
	Query    string `json:"query"`
	Number   int    `json:"number,omitempty"`
	Value    any    `json:"value,omitempty"`
	Decimals int    `json:"decimals,omitempty"`
}
type batch struct {
	Points  []point `json:"points"`
	Expires *int64  `json:"expires_at,omitempty"`
}
type resource struct {
	cfg    config
	ctx    context.Context
	cancel context.CancelFunc
	gate   chan struct{}
}
type connector struct {
	mu            sync.Mutex
	libraryMu     sync.Mutex
	active        sync.WaitGroup
	resources     map[string]*resource
	closed        bool
	slots         chan struct{}
	library       *nativeLibrary
	libraryPath   string
	libraryDigest string
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, slots: make(chan struct{}, 8)}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-fanuc", Name: "FANUC FOCAS Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Native FOCAS demand sessions with explicit ABI, architecture and operator-owned library", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "fanuc", Label: "FANUC FOCAS", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation"}, DisplayFields: []string{"host", "port", "cnc_path", "abi"}}}}
}
func validateLibrary(cfg config) error {
	if !filepath.IsAbs(cfg.Library) || len(cfg.SHA256) != 64 || cfg.ABI != "focas32_compat" {
		return errors.New("absolute library_path, SHA256 and explicit focas32_compat ABI required")
	}
	expected, err := hex.DecodeString(cfg.SHA256)
	if err != nil {
		return errors.New("invalid library SHA256")
	}
	file, err := os.Open(cfg.Library)
	if err != nil {
		return errors.New("cannot open FOCAS library")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 67108864 {
		return errors.New("FOCAS library must be a regular file within 64 MiB")
	}
	digest := sha256.New()
	copied, err := io.Copy(digest, io.LimitReader(file, 67108865))
	if err != nil || copied > 67108864 || !bytes.Equal(digest.Sum(nil), expected) {
		return errors.New("FOCAS library digest mismatch or size limit")
	}
	binary, err := elf.Open(cfg.Library)
	if err != nil {
		return errors.New("native library must be ELF")
	}
	defer binary.Close()
	if binary.Type != elf.ET_DYN || runtime.GOARCH == "amd64" && (binary.Machine != elf.EM_X86_64 || binary.Class != elf.ELFCLASS64) || runtime.GOARCH == "arm" && (binary.Machine != elf.EM_ARM || binary.Class != elf.ELFCLASS32) {
		return errors.New("FOCAS library CPU architecture mismatch")
	}
	return nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 8193, TimeoutMS: 3000, Path: 1}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || strings.ContainsRune(cfg.Host, 0) || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 1000 || cfg.TimeoutMS > 10000 || cfg.Path < 1 || cfg.Path > 16 || len(cfg.WriteMacros) > 128 || len(cfg.ReadPMCRanges) > 64 {
		return "", errors.New("invalid FANUC resource")
	}
	if err := validateLibrary(cfg); err != nil {
		return "", err
	}
	for _, v := range cfg.ReadPMCRanges {
		if v.Area < 0 || v.Area > 12 || v.Start < 0 || v.End < v.Start || v.End > 32767 {
			return "", errors.New("invalid readable PMC byte range")
		}
	}
	seen := map[int]bool{}
	for _, number := range cfg.WriteMacros {
		if number < 1 || number > 9999 || seen[number] {
			return "", errors.New("invalid writable macro allowlist")
		}
		seen[number] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("resource exists or connector stopped")
	}
	if c.libraryPath != "" && (c.libraryPath != cfg.Library || c.libraryDigest != cfg.SHA256) {
		cancel()
		return "", errors.New("one FOCAS library generation per plugin process; restart plugin process before replacing native library generation")
	}
	c.libraryPath = cfg.Library
	c.libraryDigest = cfg.SHA256
	c.resources[spec.ID] = &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	return spec.ID, nil
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		r.gate <- struct{}{}
		<-r.gate
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.closed = true
	resources := c.resources
	c.resources = map[string]*resource{}
	c.mu.Unlock()
	for _, r := range resources {
		r.cancel()
	}
	for _, r := range resources {
		r.gate <- struct{}{}
		<-r.gate
	}
	c.active.Wait()
	c.libraryMu.Lock()
	defer c.libraryMu.Unlock()
	if c.library != nil {
		c.library.close()
		c.library = nil
	}
	return nil
}
func (c *connector) native(cfg config) (*nativeLibrary, error) {
	c.libraryMu.Lock()
	defer c.libraryMu.Unlock()
	if c.library != nil {
		return c.library, nil
	}
	if err := validateLibrary(cfg); err != nil {
		return nil, err
	}
	library, err := openNative(cfg.Library)
	if err != nil {
		return nil, err
	}
	c.library = library
	return library, nil
}

var decimalPattern = regexp.MustCompile(`^[+-]?[0-9]{1,16}(?:\.[0-9]{1,16})?$`)

func macroValue(value any, decimals int) (int32, error) {
	if decimals < 0 || decimals > 9 {
		return 0, errors.New("macro decimals must be 0..9")
	}
	text := ""
	switch v := value.(type) {
	case string:
		text = v
	case json.Number:
		text = string(v)
	default:
		return 0, errors.New("macro write requires exact decimal string or JSON number")
	}
	if len(text) > 64 || !decimalPattern.MatchString(text) {
		return 0, errors.New("macro decimal too long")
	}
	number, ok := new(big.Rat).SetString(text)
	if !ok {
		return 0, errors.New("invalid decimal macro value")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	number.Mul(number, new(big.Rat).SetInt(scale))
	if !number.IsInt() || !number.Num().IsInt64() {
		return 0, errors.New("macro value has unsupported precision")
	}
	v := number.Num().Int64()
	if v < -999999999 || v > 999999999 {
		return 0, errors.New("macro value outside 9-digit FOCAS range")
	}
	return int32(v), nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.Lock()
	r := c.resources[h]
	if r == nil || c.closed {
		c.mu.Unlock()
		return sdk.ConnectorResponse{}, errors.New("resource unavailable or stopped")
	}
	c.active.Add(1)
	c.mu.Unlock()
	defer c.active.Done()
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	if !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported FANUC operation")
	}
	var b batch
	decoder := json.NewDecoder(bytes.NewReader(req.Payload))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if len(req.Payload) > 131072 || decoder.Decode(&b) != nil || len(b.Points) < 1 || len(b.Points) > 64 {
		return sdk.ConnectorResponse{}, errors.New("requires 1..64 FANUC queries")
	}
	names := map[string]bool{}
	rawValues := make([]int32, len(b.Points))
	for i, p := range b.Points {
		if p.Name == "" || names[p.Name] {
			return sdk.ConnectorResponse{}, errors.New("duplicate or empty point")
		}
		names[p.Name] = true
		switch p.Query {
		case "status", "program", "feed", "spindle", "alarm", "positions":
			if write {
				return sdk.ConnectorResponse{}, errors.New("only allowlisted macro writes are exposed")
			}
		case "tool_management", "pmc_byte":
			if write {
				return sdk.ConnectorResponse{}, errors.New("optional queries are read-only")
			}
			if p.Query == "tool_management" && (p.Number < 1 || p.Number > 32767) {
				return sdk.ConnectorResponse{}, errors.New("tool index must be 1..32767")
			}
			if p.Query == "pmc_byte" && !pmcReadable(r.cfg, p) {
				return sdk.ConnectorResponse{}, errors.New("PMC byte not in resource readable allowlist")
			}
		case "macro":
			if p.Number < 1 || p.Number > 9999 {
				return sdk.ConnectorResponse{}, errors.New("macro number must be 1..9999")
			}
		default:
			return sdk.ConnectorResponse{}, errors.New("unsupported FANUC query")
		}
		if write {
			allowed := false
			for _, number := range r.cfg.WriteMacros {
				allowed = allowed || number == p.Number
			}
			if !allowed {
				return sdk.ConnectorResponse{}, errors.New("macro is not in resource writable allowlist")
			}
			var err error
			rawValues[i], err = macroValue(p.Value, p.Decimals)
			if err != nil {
				return sdk.ConnectorResponse{}, err
			}
		}
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	if write && b.Expires == nil {
		return sdk.ConnectorResponse{}, errors.New("native write requires absolute expires_at")
	}
	if b.Expires != nil {
		if *b.Expires <= time.Now().UnixMilli() {
			return sdk.ConnectorResponse{}, errors.New("command expired")
		}
		var done context.CancelFunc
		ctx, done = context.WithDeadline(ctx, time.UnixMilli(*b.Expires))
		defer done()
	}
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case r.gate <- struct{}{}:
	}
	defer func() { <-r.gate }()
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case c.slots <- struct{}{}:
	}
	defer func() { <-c.slots }()
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	native, err := c.native(r.cfg)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer native.exitThread()
	handle, err := native.connect(r.cfg, ctx)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	defer native.free(handle)
	values := map[string]any{}
	quality := map[string]any{}
	for i, p := range b.Points {
		if ctx.Err() != nil {
			return sdk.ConnectorResponse{}, errors.New("FANUC batch deadline reached; remaining operations not attempted")
		}
		if write {
			if err = native.writeMacro(handle, p.Number, rawValues[i], p.Decimals); err != nil {
				return sdk.ConnectorResponse{}, errors.New("FANUC macro write outcome unknown; do not transparently retry")
			}
		} else {
			value, err := native.read(handle, p)
			if err != nil {
				values[p.Name] = nil
				quality[p.Name] = "bad:native_read"
			} else {
				values[p.Name] = value
				quality[p.Name] = "good"
			}
		}
	}
	result := map[string]any{"values": values, "quality": quality}
	if write {
		result = map[string]any{"written": len(b.Points)}
	}
	raw, err := json.Marshal(result)
	return sdk.ConnectorResponse{Payload: raw}, err
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED", Message: "native file validated; device verified on actual operation", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("FANUC polling only")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("FANUC polling only")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error   { return errors.New("no stream") }
func (c *connector) NackStream(string, sdk.StreamNackRequest) error { return errors.New("no stream") }
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("no stream")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }

func pmcReadable(cfg config, p point) bool {
	if p.PMCArea < 0 || p.PMCArea > 12 || p.Number < 0 || p.Number > 32767 {
		return false
	}
	for _, v := range cfg.ReadPMCRanges {
		if v.Area == p.PMCArea && p.Number >= v.Start && p.Number <= v.End {
			return true
		}
	}
	return false
}
