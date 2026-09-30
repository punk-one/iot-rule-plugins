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

func (p *TDengineSink) connectorRead(payload sqlReadPayload) ([]map[string]interface{}, error) {
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

func (p *TDengineSink) connectorWrite(payload sqlWritePayload) error {
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

func (p *TDengineSink) ensureColumnsForRows(tableName string, rows []map[string]interface{}) error {
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

func (p *TDengineSink) ensureColumn(tableName, colName string, sample interface{}) error {
	// TDengine does not support ADD COLUMN IF NOT EXISTS, so check first.
	existingCols, err := p.getTableColumns(tableName)
	if err == nil && existingCols[colName] {
		return nil
	}

	colType := inferTDengineType(sample)
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, colName, colType)
	if err := p.connectorWrite(sqlWritePayload{SQL: ddl}); err != nil {
		return err
	}
	delete(p.tableColumns, tableName)
	return nil
}

func inferTDengineType(v interface{}) string {
	switch typed := v.(type) {
	case nil:
		return "NCHAR(255)"
	case bool:
		return "BOOL"
	case float64, float32:
		return "DOUBLE"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "BIGINT"
	case jsonColumnValue:
		return "NCHAR(4096)"
	case time.Time:
		return "TIMESTAMP"
	case string:
		if _, err := time.Parse(time.RFC3339Nano, typed); err == nil {
			return "TIMESTAMP"
		}
		return "NCHAR(255)"
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array:
			return "NCHAR(4096)"
		default:
			return "NCHAR(255)"
		}
	}
}

func (p *TDengineSink) getTableColumns(tableName string) (map[string]bool, error) {
	if cols, ok := p.tableColumns[tableName]; ok {
		return cols, nil
	}
	rows, err := p.connectorRead(sqlReadPayload{
		SQL:     fmt.Sprintf("DESCRIBE %s", tableName),
		Args:    nil,
		MaxRows: 1000,
	})
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool, len(rows))
	for _, row := range rows {
		if value, ok := row["Field"].(string); ok {
			cols[value] = true
		} else if value, ok := row["field"].(string); ok {
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

// formatSQLValue formats a value for direct embedding in a TDengine SQL statement.
// The taosRestful driver does naive string interpolation of ? placeholders,
// which leaves string values unquoted. Embedding values directly avoids this.
func formatSQLValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "true"
		}
		return "false"
	case jsonColumnValue:
		s := string(val)
		return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "\\'") + "'"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05.000") + "'"
	case float64:
		return fmt.Sprintf("%v", val)
	case float32:
		return fmt.Sprintf("%v", val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", val)
	default:
		s := fmt.Sprintf("%v", val)
		return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
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

func resolveChildTableName(template string, row map[string]interface{}) string {
	re := regexp.MustCompile(`\$\{(\w+)\}`)
	return re.ReplaceAllStringFunc(template, func(match string) string {
		key := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "${")
		if val, ok := row[key]; ok {
			switch v := val.(type) {
			case string:
				return v
			default:
				return fmt.Sprintf("%v", v)
			}
		}
		return match
	})
}

func (p *TDengineSink) buildInsertSQL(tableName, childTable string, row map[string]interface{}) string {
	columnNames := sortedColumnNames(row)

	if childTable == "" {
		values := make([]string, 0, len(columnNames))
		for _, col := range columnNames {
			values = append(values, formatSQLValue(sqlArgValue(row[col])))
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName, stringsJoin(columnNames, ", "), stringsJoin(values, ", "))
		return query
	}

	tagNames := make([]string, 0)
	tagValues := make([]string, 0)
	colNames := make([]string, 0)
	colValues := make([]string, 0)

	for _, col := range columnNames {
		if p.tagFieldSet[col] {
			tagNames = append(tagNames, col)
			tagValues = append(tagValues, formatSQLValue(sqlArgValue(row[col])))
		} else {
			colNames = append(colNames, col)
			colValues = append(colValues, formatSQLValue(sqlArgValue(row[col])))
		}
	}

	// When no tag fields are configured, use simple INSERT without USING/TAGS.
	if len(tagNames) == 0 {
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			childTable, stringsJoin(colNames, ", "), stringsJoin(colValues, ", "))
		return query
	}

	query := fmt.Sprintf("INSERT INTO %s USING %s TAGS (%s) (%s) VALUES (%s)",
		childTable, tableName, stringsJoin(tagValues, ", "), stringsJoin(colNames, ", "), stringsJoin(colValues, ", "))

	return query
}

func applyColumnMapping(payload map[string]interface{}, columnMapping []ColumnMappingItem) map[string]interface{} {
	if len(columnMapping) == 0 {
		return payload
	}
	lookup := make(map[string]string, len(columnMapping))
	for _, item := range columnMapping {
		if item.Field != "" {
			lookup[item.Field] = item.Column
		}
	}
	mapped := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		if targetColumn, ok := lookup[key]; ok {
			mapped[targetColumn] = value
		} else {
			mapped[key] = value
		}
	}
	return mapped
}
