package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sqlReadPayload struct {
	SQL     string        `json:"sql"`
	Args    []interface{} `json:"args"`
	MaxRows int           `json:"max_rows"`
}

type sqlWritePayload struct {
	SQL        string              `json:"sql"`
	Args       []interface{}       `json:"args,omitempty"`
	Statements []sqlWriteStatement `json:"statements,omitempty"`
}

type sqlWriteStatement struct {
	SQL  string        `json:"sql"`
	Args []interface{} `json:"args,omitempty"`
}

type sqlRowsResponse struct {
	Rows []map[string]interface{} `json:"rows"`
}

type jsonColumnValue string

func (p *PostgresSink) connectorRead(payload sqlReadPayload) ([]map[string]interface{}, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	resp, err := p.ctx.Connector().Read(sdk.ConnectorRequest{
		Target:  "sql",
		Payload: raw,
	})
	if err != nil {
		return nil, err
	}
	var decoded sqlRowsResponse
	if err := json.Unmarshal(resp.Payload, &decoded); err != nil {
		return nil, err
	}
	return decoded.Rows, nil
}

func (p *PostgresSink) connectorWrite(payload sqlWritePayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = p.ctx.Connector().Write(sdk.ConnectorRequest{
		Target:  "sql",
		Payload: raw,
	})
	return err
}

func (p *PostgresSink) ensureColumnsForRows(tableName string, rows []map[string]interface{}) error {
	if !p.autoCreateCols || len(rows) == 0 {
		return nil
	}

	tableColumns, err := p.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to load table columns: %w", err)
	}

	for _, column := range unionColumnNames(rows) {
		if tableColumns[column] {
			continue
		}
		sample, ok := firstNonNilValue(rows, column)
		if !ok {
			sample = ""
		}
		if err := p.ensureColumn(tableName, column, sample); err != nil {
			return err
		}
		tableColumns[column] = true
	}
	return nil
}

func firstNonNilValue(rows []map[string]interface{}, column string) (interface{}, bool) {
	for _, row := range rows {
		if value, ok := row[column]; ok && value != nil {
			return value, true
		}
	}
	return nil, false
}

func (p *PostgresSink) ensureColumn(tableName, colName string, sample interface{}) error {
	colType := inferPGType(sample)
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", tableName, colName, colType)
	if err := p.connectorWrite(sqlWritePayload{SQL: ddl}); err != nil {
		return err
	}
	delete(p.tableColumns, tableName)
	return nil
}

func inferPGType(v interface{}) string {
	switch typed := v.(type) {
	case nil:
		return "TEXT"
	case bool:
		return "BOOLEAN"
	case float64, float32:
		return "DOUBLE PRECISION"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "BIGINT"
	case jsonColumnValue:
		return "JSONB"
	case time.Time:
		return "TIMESTAMPTZ"
	case string:
		if _, err := time.Parse(time.RFC3339Nano, typed); err == nil {
			return "TIMESTAMPTZ"
		}
		return "TEXT"
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array:
			return "JSONB"
		default:
			return "TEXT"
		}
	}
}

func (p *PostgresSink) getTableColumns(tableName string) (map[string]bool, error) {
	if cols, ok := p.tableColumns[tableName]; ok {
		return cols, nil
	}
	rows, err := p.connectorRead(sqlReadPayload{
		SQL:     `SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`,
		Args:    []interface{}{tableName},
		MaxRows: 1000,
	})
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool, len(rows))
	for _, row := range rows {
		if value, ok := row["column_name"].(string); ok {
			cols[value] = true
		}
	}
	p.tableColumns[tableName] = cols
	return cols, nil
}

func normalizeColumnValue(value interface{}) (interface{}, error) {
	switch typed := value.(type) {
	case nil, bool, string, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, time.Time:
		return typed, nil
	case jsonColumnValue:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array:
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			return jsonColumnValue(raw), nil
		default:
			return value, nil
		}
	}
}

func sqlArgValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case jsonColumnValue:
		return string(typed)
	default:
		return value
	}
}

func estimateRowSize(row map[string]interface{}) int {
	raw, err := json.Marshal(row)
	if err != nil {
		return len(row) * 32
	}
	return len(raw)
}

func unionColumnNames(rows []map[string]interface{}) []string {
	columns := make(map[string]struct{})
	for _, row := range rows {
		for column := range row {
			columns[column] = struct{}{}
		}
	}
	names := make([]string, 0, len(columns))
	for column := range columns {
		names = append(names, column)
	}
	sort.Strings(names)
	return names
}

func sortedColumnNames(row map[string]interface{}) []string {
	names := make([]string, 0, len(row))
	for column := range row {
		names = append(names, column)
	}
	sort.Strings(names)
	return names
}

func resolveTableName(template string, metadata map[string]string) string {
	re := regexp.MustCompile(`\$\{metadata\.(\w+)\}`)
	return re.ReplaceAllStringFunc(template, func(match string) string {
		key := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "${metadata.")
		if val, ok := metadata[key]; ok {
			return val
		}
		return match
	})
}
