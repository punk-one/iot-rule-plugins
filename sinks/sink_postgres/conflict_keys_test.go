package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const idempotentSinkConfig = `{"connector_ref":"pg","output":{"table":"sample","conflict_key_columns":["Device_ID","sample_id"]}}`

func TestConflictKeysValidateBeforeConnectorIO(t *testing.T) {
	for _, input := range []string{`["device_id","Device_ID"]`, `["metadata.device_id"]`, `["bad key"]`} {
		config := `{"connector_ref":"pg","output":{"table":"sample","conflict_key_columns":` + input + `}}`
		if err := new(PostgresSink).validateConfig([]byte(config)); err == nil {
			t.Errorf("invalid conflict column configuration accepted: %s", input)
		}
	}
	connector := &pgConnector{}
	sink := &PostgresSink{ctx: pgRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(idempotentSinkConfig)); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []map[string]any{
		{"device_id": 1, "reading": 10},
		{"device_id": 1, "sample_id": nil, "reading": 10},
	} {
		if err := sink.OnEvent(sdk.Event{Payload: payload}); err == nil {
			t.Fatal("event without a usable conflict key accepted")
		}
	}
	if len(connector.writes) != 0 || len(connector.reads) != 0 {
		t.Fatal("missing or null key reached the connector")
	}
}

func TestPostgresConflictClauseForSingleAndBatch(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		t.Run(mode, func(t *testing.T) {
			writes := conflictWriteRequests(t, mode)
			for _, req := range writes {
				if !strings.HasSuffix(req.SQL, ` ON CONFLICT ("device_id", "sample_id") DO NOTHING`) {
					t.Fatalf("unexpected conflict target: %s", req.SQL)
				}
			}
		})
	}
	sink := new(PostgresSink)
	if err := sink.validateConfig([]byte(idempotentSinkConfig)); err != nil {
		t.Fatal(err)
	}
	if err := sink.validateConfig([]byte(`{"connector_ref":"pg","output":{"table":"sample"}}`)); err != nil {
		t.Fatal(err)
	}
	if sink.conflictClause() != "" {
		t.Fatal("omitting conflict keys should preserve plain INSERT")
	}
}

func conflictWriteRequests(t *testing.T, mode string) []sqlWritePayload {
	t.Helper()
	connector := &pgConnector{}
	sink := &PostgresSink{ctx: pgRuntimeContext{connector: connector}}
	config := idempotentSinkConfig
	if mode == "batch" {
		config = strings.Replace(config, `"table":"sample",`, `"table":"sample","write_mode":"batch","batch":{"max_rows":2},`, 1)
	}
	if err := sink.validateConfig([]byte(config)); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []map[string]any{
		{"device_id": 1, "sample_id": 7, "reading": 9},
		{"Device_ID": 1, "sample_id": 7, "reading": 99},
	} {
		if err := sink.OnEvent(sdk.Event{Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	expected := 2
	if mode == "batch" {
		expected = 1
	}
	if len(connector.writes) != expected {
		t.Fatalf("got %d write requests, want %d", len(connector.writes), expected)
	}
	var writes []sqlWritePayload
	for _, write := range connector.writes {
		var req sqlWritePayload
		decoder := json.NewDecoder(bytes.NewReader(write.Payload))
		decoder.UseNumber()
		if err := decoder.Decode(&req); err != nil {
			t.Fatal(err)
		}
		writes = append(writes, req)
	}
	return writes
}

func TestPostgresReplayKeepsOriginalRowInDisposableDatabase(t *testing.T) {
	dsn, psql := os.Getenv("IOT_TEST_POSTGRES_DSN"), os.Getenv("IOT_TEST_POSTGRES_PSQL")
	if dsn == "" || psql == "" {
		t.Skip("set IOT_TEST_POSTGRES_DSN and IOT_TEST_POSTGRES_PSQL for an isolated PostgreSQL fixture")
	}
	for _, mode := range []string{"single", "batch"} {
		t.Run(mode, func(t *testing.T) {
			var input strings.Builder
			input.WriteString("CREATE TEMP TABLE sample(device_id integer NOT NULL, sample_id integer NOT NULL, reading integer, PRIMARY KEY(device_id, sample_id));\n")
			for i, req := range conflictWriteRequests(t, mode) {
				var args []string
				for _, arg := range req.Args {
					numeric, ok := arg.(json.Number)
					if !ok {
						t.Fatalf("unexpected test argument type %T", arg)
					}
					value, err := numeric.Int64()
					if err != nil {
						t.Fatal(err)
					}
					args = append(args, strconv.FormatInt(value, 10))
				}
				fmt.Fprintf(&input, "PREPARE stmt%d AS %s;\nEXECUTE stmt%d (%s);\n", i, req.SQL, i, strings.Join(args, ","))
			}
			input.WriteString("SELECT count(*)::text || '|' || max(reading)::text FROM sample;\n")
			cmd := exec.Command(psql, "-XqAt", "-v", "ON_ERROR_STOP=1", "--dbname", dsn)
			cmd.Stdin = strings.NewReader(input.String())
			result, err := cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(result)) != "1|9" {
				t.Fatalf("expected one unchanged row, got %q, err=%v", result, err)
			}
		})
	}
}
