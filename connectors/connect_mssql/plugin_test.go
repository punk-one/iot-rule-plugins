package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net/url"
	"testing"
)

type fixtureConnector struct{ failCommit bool }

func (f fixtureConnector) Connect(context.Context) (driver.Conn, error) {
	return fixtureConn{fail: f.failCommit}, nil
}
func (f fixtureConnector) Driver() driver.Driver { return fixtureDriver{} }

type fixtureDriver struct{}

func (fixtureDriver) Open(string) (driver.Conn, error) { return fixtureConn{}, nil }

type fixtureConn struct{ fail bool }

func (fixtureConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (fixtureConn) Close() error                        { return nil }
func (f fixtureConn) Begin() (driver.Tx, error)         { return fixtureTx{fail: f.fail}, nil }
func (fixtureConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (fixtureConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fixtureRows{}, nil
}

type fixtureTx struct{ fail bool }

func (f fixtureTx) Commit() error {
	if f.fail {
		return errors.New("commit acknowledgement lost")
	}
	return nil
}
func (fixtureTx) Rollback() error { return nil }

type fixtureRows struct{ index int }

func (fixtureRows) Columns() []string { return []string{"id", "label"} }
func (fixtureRows) Close() error      { return nil }
func (r *fixtureRows) Next(dest []driver.Value) error {
	if r.index >= 2 {
		return io.EOF
	}
	dest[0] = int64(9007199254740993) + int64(r.index)
	dest[1] = "synthetic"
	r.index++
	return nil
}
func TestSQLTLSAndParameterBinding(t *testing.T) {
	dsn := connectionString(config{Host: "db.invalid", Port: 1433, Database: "test", Username: "user", Password: "test-only@&", TimeoutMS: 1000})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("encrypt") != "true" || u.Query().Get("TrustServerCertificate") != "false" {
		t.Fatal("TLS verification not enforced")
	}
	args, err := arguments(map[string]any{"id": json.Number("9007199254740993"), "text": "x';drop table t;--"})
	if err != nil {
		t.Fatal(err)
	}
	if args[0].(sql.NamedArg).Value != int64(9007199254740993) {
		t.Fatal("integer parameter rounded")
	}
	if _, err := arguments(map[string]any{"not;safe": 1}); err == nil {
		t.Fatal("invalid parameter name accepted")
	}
}
func TestSQLRowsBoundAndCommitBoundary(t *testing.T) {
	for _, fail := range []bool{false, true} {
		c := NewConnector().(*connector)
		c.open = func(string, string) (*sql.DB, error) { return sql.OpenDB(fixtureConnector{failCommit: fail}), nil }
		if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"host":"db.invalid","database":"test","username":"user","allow_write":true}`)}); err != nil {
			t.Fatal(err)
		}
		response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"query":"SELECT id,label FROM events","params":{},"max_rows":2}`)})
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Rows []map[string]any `json:"rows"`
		}
		json.Unmarshal(response.Payload, &result)
		if result.Rows[0]["id"] != "9007199254740993" {
			t.Fatal("large integer rounded")
		}
		if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"query":"SELECT id,label FROM events","max_rows":1}`)}); err == nil {
			t.Fatal("unbounded query accepted")
		}
		_, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"query":"INSERT INTO events(id) VALUES (@id)","params":{"id":1}}`)})
		if fail && err == nil {
			t.Fatal("unknown commit reported success")
		}
		if !fail && err != nil {
			t.Fatal(err)
		}
		c.Stop()
	}
}
