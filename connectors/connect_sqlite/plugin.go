package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	_ "modernc.org/sqlite"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	Directory      string `json:"directory"`
	File           string `json:"file"`
	TimeoutMS      int    `json:"timeout_ms"`
	MaxConnections int    `json:"max_connections"`
	MaxRows        int    `json:"max_rows"`
	AllowWrite     bool   `json:"allow_write"`
}

type statement struct {
	Query   string         `json:"query"`
	Params  map[string]any `json:"params"`
	MaxRows int            `json:"max_rows,omitempty"`
}
type resource struct {
	cfg    config
	db     *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
}
type connector struct {
	mu        sync.RWMutex
	resources map[string]*resource
	open      func(string, string) (*sql.DB, error)
	stopped   bool
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, open: sql.Open}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-sqlite", Name: "SQLite Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "TLS-verified SQLite pool with named parameters, bounded rows and transactional writes", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "sqlite", Label: "SQLite", Capabilities: []string{"read", "write", "shared_duplex", "session:pooled"}, DisplayFields: []string{"directory", "file", "allow_write"}}}}
}
func connectionString(cfg config) string {
	absolute, _ := filepath.Abs(filepath.Join(cfg.Directory, cfg.File))
	u := &url.URL{Scheme: "file", Path: absolute}
	q := url.Values{}
	if cfg.AllowWrite {
		q.Set("mode", "rwc")
		q.Add("_pragma", "journal_mode(WAL)")
		q.Add("_pragma", "synchronous(FULL)")
	} else {
		q.Set("mode", "ro")
	}
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout("+strconv.Itoa(cfg.TimeoutMS)+")")
	u.RawQuery = q.Encode()
	return u.String()
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := config{Directory: "./data/sqlite", File: "telemetry.db", TimeoutMS: 5000, MaxConnections: 1, MaxRows: 1000}
	if err := json.Unmarshal(r.Connection, &cfg); err != nil {
		return "", errors.New("invalid SQLite config")
	}
	if r.ID == "" || cfg.Directory == "" || cfg.File == "" || filepath.Base(cfg.File) != cfg.File || cfg.File == "." || cfg.File == ".." || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxConnections != 1 || cfg.MaxRows < 1 || cfg.MaxRows > 10000 {
		return "", errors.New("invalid SQLite settings; one connection per resource is required")
	}
	if cfg.AllowWrite {
		if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
			return "", errors.New("cannot create SQLite directory")
		}
	}
	if info, err := os.Lstat(filepath.Join(cfg.Directory, cfg.File)); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("SQLite database file must not be a symlink")
	}
	db, err := c.open("sqlite", connectionString(cfg))
	if err != nil {
		return "", errors.New("cannot create SQLite pool")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		cancel()
		db.Close()
		return "", errors.New("connector stopped")
	}
	if _, exists := c.resources[r.ID]; exists {
		cancel()
		db.Close()
		return "", errors.New("resource exists")
	}
	c.resources[r.ID] = &resource{cfg: cfg, db: db, ctx: ctx, cancel: cancel}
	return r.ID, nil
}

func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		return r.db.Close()
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.stopped = true
	rs := make([]*resource, 0, len(c.resources))
	for k, r := range c.resources {
		r.cancel()
		rs = append(rs, r)
		delete(c.resources, k)
	}
	c.mu.Unlock()
	for _, r := range rs {
		_ = r.db.Close()
	}
	return nil
}

var parameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func arguments(params map[string]any) ([]any, error) {
	if len(params) > 128 {
		return nil, errors.New("too many SQL parameters")
	}
	names := make([]string, 0, len(params))
	for k := range params {
		if !parameterName.MatchString(k) {
			return nil, errors.New("invalid named parameter")
		}
		names = append(names, k)
	}
	sort.Strings(names)
	args := make([]any, 0, len(names))
	for _, k := range names {
		v := params[k]
		switch x := v.(type) {
		case nil, bool:
		case string:
			if len(x) > 65536 {
				return nil, errors.New("SQL parameter too large")
			}
		case float64:
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return nil, errors.New("invalid SQL numeric parameter")
			}
		case json.Number:
			if n, err := x.Int64(); err == nil {
				v = n
			} else {
				if !strings.ContainsAny(string(x), ".eE") {
					return nil, errors.New("integer SQL parameter out of int64 range; use explicit decimal string")
				}
				n, err := x.Float64()
				if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
					return nil, errors.New("invalid SQL numeric parameter")
				}
				v = n
			}
		default:
			return nil, errors.New("SQL parameters must be JSON scalars")
		}
		args = append(args, sql.Named(k, v))
	}
	return args, nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if req.Target == "identity" && req.Kind == sdk.ConnectorRequestRead {
		ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
		defer cancel()
		if err := r.db.PingContext(ctx); err != nil {
			return sdk.ConnectorResponse{}, errors.New("SQLite database unavailable")
		}
		path, _ := filepath.Abs(filepath.Join(r.cfg.Directory, r.cfg.File))
		info, err := os.Stat(path)
		if err != nil {
			return sdk.ConnectorResponse{}, errors.New("cannot identify SQLite file")
		}
		system, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return sdk.ConnectorResponse{}, errors.New("SQLite file identity unavailable")
		}
		fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", path, system.Dev, system.Ino)))
		raw, _ := json.Marshal(map[string]string{"identity": hex.EncodeToString(fingerprint[:])})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}
	if len(req.Payload) > 1048576 {
		return sdk.ConnectorResponse{}, errors.New("SQL request too large")
	}
	var spec statement
	decoder := json.NewDecoder(strings.NewReader(string(req.Payload)))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil || strings.TrimSpace(spec.Query) == "" {
		return sdk.ConnectorResponse{}, errors.New("invalid statement request")
	}
	args, err := arguments(spec.Params)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	timeout := time.Duration(r.cfg.TimeoutMS) * time.Millisecond
	if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond < timeout {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	var result any
	switch req.Kind {
	case sdk.ConnectorRequestRead, sdk.ConnectorRequestBatchRead:
		limit := r.cfg.MaxRows
		if spec.MaxRows > 0 && spec.MaxRows < limit {
			limit = spec.MaxRows
		}
		result, err = queryRows(ctx, r.db, spec.Query, args, limit)
	case sdk.ConnectorRequestWrite, sdk.ConnectorRequestBatchWrite:
		if !r.cfg.AllowWrite {
			return sdk.ConnectorResponse{}, errors.New("writes disabled on this resource")
		}
		tx, e := r.db.BeginTx(ctx, nil)
		if e != nil {
			return sdk.ConnectorResponse{}, errors.New("begin transaction failed")
		}
		defer tx.Rollback()
		res, e := tx.ExecContext(ctx, spec.Query, args...)
		if e != nil {
			return sdk.ConnectorResponse{}, errors.New("statement failed and transaction rolled back")
		}
		affected, _ := res.RowsAffected()
		if e = tx.Commit(); e != nil {
			return sdk.ConnectorResponse{}, errors.New("commit result unknown; do not retry automatically")
		}
		result = map[string]any{"committed": true, "rows_affected": affected}
	default:
		return sdk.ConnectorResponse{}, errors.New("unsupported SQL request kind")
	}
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("SQL result is not JSON serializable")
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}
func queryRows(ctx context.Context, db *sql.DB, query string, args []any, limit int) (any, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.New("SQL query failed")
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.New("SQL columns unavailable")
	}
	if len(columns) > 256 {
		return nil, errors.New("SQL column limit exceeded")
	}
	seen := map[string]bool{}
	for _, name := range columns {
		if seen[name] {
			return nil, errors.New("duplicate SQL column names require explicit aliases")
		}
		seen[name] = true
	}
	types, _ := rows.ColumnTypes()
	result := make([]map[string]any, 0)
	size := 0
	for rows.Next() {
		if len(result) >= limit {
			return nil, errors.New("SQL row limit exceeded; use bounded/paginated SQL")
		}
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, errors.New("SQL row scan failed")
		}
		row := map[string]any{}
		for i, name := range columns {
			value := values[i]
			switch v := value.(type) {
			case []byte:
				typeName := ""
				if len(types) > i {
					typeName = strings.ToUpper(types[i].DatabaseTypeName())
				}
				switch typeName {
				case "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT", "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
					value = string(v)
				default:
					value = map[string]any{"encoding": "base64", "value": base64.StdEncoding.EncodeToString(v)}
				}
			case int64:
				if v > 9007199254740991 || v < -9007199254740991 {
					value = strconv.FormatInt(v, 10)
				}
			case time.Time:
				value = v.UTC().Format(time.RFC3339Nano)
			}
			row[name] = value
		}
		encoded, e := json.Marshal(row)
		if e != nil {
			return nil, errors.New("invalid SQL value")
		}
		size += len(encoded)
		if size > 8388608 {
			return nil, errors.New("SQL response byte limit exceeded")
		}
		result = append(result, row)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("SQL query interrupted")
	}
	return map[string]any{"rows": result, "row_count": len(result)}, nil
}
func (c *connector) Probe(h string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	defer cancel()
	status := sdk.ResourceStatusHealthy
	code := "CONNECTED"
	if err := r.db.PingContext(ctx); err != nil {
		status = sdk.ResourceStatusDisconnected
		code = "CONNECT_FAILED"
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: code, ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("SQL streaming not supported")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("SQL streaming not supported")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) NackStream(string, sdk.StreamNackRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
