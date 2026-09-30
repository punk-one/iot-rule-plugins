package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sqlReadPayload struct {
	SQL     string `json:"sql"`
	Args    []any  `json:"args"`
	MaxRows int    `json:"max_rows"`
}

type sqlWritePayload struct {
	SQL        string              `json:"sql"`
	Args       []any               `json:"args"`
	Statements []sqlWriteStatement `json:"statements"`
}

type sqlWriteStatement struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args"`
}

const (
	defaultSQLMaxRows       = 100
	maximumSQLMaxRows       = 10000
	maximumSQLColumns       = 256
	maximumSQLResponseBytes = 4 << 20
)

func (c *timescaledbConnector) executeMetadata(resource *timescaledbResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "info":
		return buildJSONResponse(map[string]any{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
			"driver":      "postgres",
			"database":    "timescaledb",
			"session": map[string]any{
				"warmup_policy":  resource.resource.WarmupPolicy,
				"session_policy": resource.resource.SessionPolicy,
			},
			"supported_targets": []string{
				"metadata/info",
				"metadata/ping",
				"read/sql",
				"write/sql",
			},
		}, map[string]string{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
		})
	case "ping":
		db, err := c.openDB(resource)
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 3*time.Second))
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("ping database: %w", err)
		}
		return buildJSONResponse(map[string]any{
			"ok":          true,
			"resource_id": resource.resource.ID,
		}, map[string]string{
			"status": "healthy",
		})
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported metadata target %q", req.Target)
	}
}

func (c *timescaledbConnector) probe(resource *timescaledbResource, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	db, err := c.openDB(resource)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 3*time.Second))
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return sdk.ResourceStatusEvent{}, fmt.Errorf("ping database: %w", err)
	}

	row := db.QueryRowContext(ctx, "SHOW transaction_read_only")
	var readOnly string
	if err := row.Scan(&readOnly); err == nil && (readOnly == "on" || readOnly == "true") {
		return sdk.ResourceStatusEvent{
			ResourceID:         resource.resource.ID,
			ProviderPluginID:   c.Info().ID,
			Status:             sdk.ResourceStatusReadonly,
			ReasonCode:         "READ_ONLY",
			Message:            "database is read-only",
			ObservedAtUnixNano: time.Now().UnixNano(),
		}, nil
	}

	return sdk.ResourceStatusEvent{
		ResourceID:         resource.resource.ID,
		ProviderPluginID:   c.Info().ID,
		Status:             sdk.ResourceStatusHealthy,
		ReasonCode:         "READY",
		Message:            "database reachable",
		ObservedAtUnixNano: time.Now().UnixNano(),
	}, nil
}

func (c *timescaledbConnector) executeRead(resource *timescaledbResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload sqlReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse read payload: %w", err)
	}
	if payload.SQL == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("read.sql is required")
	}
	rowLimit, err := normalizeSQLRowLimit(payload.MaxRows)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	db, err := c.openDB(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()

	rows, err := db.QueryContext(ctx, payload.SQL, payload.Args...)
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("execute read sql: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("load columns: %w", err)
	}
	if len(columns) > maximumSQLColumns {
		return sdk.ConnectorResponse{}, fmt.Errorf("query returned %d columns; maximum is %d", len(columns), maximumSQLColumns)
	}

	result := make([]map[string]any, 0)
	responseBytes := 0
	truncated := false
	for rows.Next() {
		if len(result) >= rowLimit {
			truncated = true
			break
		}

		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("scan row: %w", err)
		}

		row := make(map[string]any, len(columns))
		for i, column := range columns {
			normalized := normalizeSQLValue(values[i])
			responseBytes += len(column) + estimateSQLValueBytes(normalized)
			if responseBytes > maximumSQLResponseBytes {
				return sdk.ConnectorResponse{}, fmt.Errorf("query response exceeds %d bytes", maximumSQLResponseBytes)
			}
			row[column] = normalized
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("iterate rows: %w", err)
	}

	return buildJSONResponse(map[string]any{
		"rows":      result,
		"count":     len(result),
		"truncated": truncated,
	}, map[string]string{
		"resource_id": resource.resource.ID,
	})
}

func normalizeSQLRowLimit(requested int) (int, error) {
	if requested <= 0 {
		return defaultSQLMaxRows, nil
	}
	if requested > maximumSQLMaxRows {
		return 0, fmt.Errorf("max_rows must not exceed %d", maximumSQLMaxRows)
	}
	return requested, nil
}

func estimateSQLValueBytes(value any) int {
	switch typed := value.(type) {
	case string:
		return len(typed)
	case []byte:
		return len(typed)
	case nil:
		return 4
	default:
		return 32
	}
}

func (c *timescaledbConnector) executeWrite(resource *timescaledbResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload sqlWritePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse write payload: %w", err)
	}

	db, err := c.openDB(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()

	if len(payload.Statements) == 0 {
		if payload.SQL == "" {
			return sdk.ConnectorResponse{}, fmt.Errorf("write.sql or write.statements is required")
		}
		result, err := db.ExecContext(ctx, payload.SQL, payload.Args...)
		if err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("execute write sql: %w", err)
		}
		return buildWriteResponse(result)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("begin transaction: %w", err)
	}

	var rowsAffected int64
	for _, stmt := range payload.Statements {
		if stmt.SQL == "" {
			_ = tx.Rollback()
			return sdk.ConnectorResponse{}, fmt.Errorf("statement.sql is required")
		}
		result, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...)
		if err != nil {
			_ = tx.Rollback()
			return sdk.ConnectorResponse{}, fmt.Errorf("execute statement: %w", err)
		}
		if count, err := result.RowsAffected(); err == nil {
			rowsAffected += count
		}
	}
	if err := tx.Commit(); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("commit transaction: %w", err)
	}

	return buildJSONResponse(map[string]any{
		"rows_affected": rowsAffected,
	}, nil)
}

func buildWriteResponse(result sql.Result) (sdk.ConnectorResponse, error) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}
	return buildJSONResponse(map[string]any{
		"rows_affected": rowsAffected,
	}, nil)
}

func normalizeSQLValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return value
	}
}
