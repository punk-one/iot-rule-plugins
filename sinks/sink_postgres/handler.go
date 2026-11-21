package main

import (
	"errors"
	"fmt"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (p *PostgresSink) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return postgresSinkFatal(fmt.Errorf("RuntimeContext not initialized"))
	}
	p.reportConnected()

	tableName, row, err := p.buildRowData(e)
	if err != nil || row == nil {
		return err
	}

	if p.writeMode == writeModeBatch {
		return p.enqueueRow(tableName, row)
	}
	return p.writeSingleRow(tableName, row)
}

func (p *PostgresSink) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresSink) buildRowData(e sdk.Event) (string, map[string]interface{}, error) {
	tableName := resolveTableName(p.tableTemplate, e.Metadata)
	if len(e.Payload) == 0 {
		return tableName, nil, nil
	}

	if !validSQLTableName(tableName) {
		return "", nil, sdk.SkippableError(errors.New("invalid SQL table name"))
	}

	row := make(map[string]interface{}, len(e.Payload))
	for key, value := range e.Payload {
		normalized, err := normalizeColumnValue(value)
		if err != nil {
			return tableName, nil, sdk.SkippableError(fmt.Errorf("normalize payload field %q: %w", key, err))
		}
		row[key] = normalized
	}
	if err := validateSQLColumns(row); err != nil {
		return tableName, nil, sdk.SkippableError(err)
	}
	if err := normalizeConflictKeyValues(row, p.cfg.Output.ConflictKeyColumns); err != nil {
		return tableName, nil, sdk.SkippableError(err)
	}
	return tableName, row, nil
}

func (p *PostgresSink) conflictClause() string {
	if len(p.cfg.Output.ConflictKeyColumns) == 0 {
		return ""
	}
	return ` ON CONFLICT ("` + strings.Join(p.cfg.Output.ConflictKeyColumns, `", "`) + `") DO NOTHING`
}

func (p *PostgresSink) writeSingleRow(tableName string, row map[string]interface{}) error {
	if err := p.ensureColumnsForRows(tableName, []map[string]interface{}{row}); err != nil {
		p.reportWriteFailure(err, "single")
		return postgresSinkRetryable(err)
	}

	columnNames := sortedColumnNames(row)
	placeholders := make([]string, len(columnNames))
	args := make([]interface{}, 0, len(columnNames))
	for idx, col := range columnNames {
		placeholders[idx] = fmt.Sprintf("$%d", idx+1)
		args = append(args, sqlArgValue(row[col]))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, strings.Join(columnNames, ", "), strings.Join(placeholders, ", ")) + p.conflictClause()
	if err := p.connectorWrite(sqlWritePayload{SQL: query, Args: args}); err != nil {
		p.reportWriteFailure(err, "single")
		return postgresSinkRetryable(err)
	}

	p.ctx.Metrics().IncCounter("postgres_sink.inserts", map[string]string{"table": tableName})
	p.reportWriteSuccess(1, "single")
	p.reportHeartbeat(1, "single")
	return nil
}

func (p *PostgresSink) enqueueRow(tableName string, row map[string]interface{}) error {
	rowBytes := estimateRowSize(row)

	var rowsToFlush []map[string]interface{}
	p.bufferMu.Lock()
	if p.pendingRows == nil {
		p.pendingRows = make(map[string][]map[string]interface{})
	}
	if p.pendingBytes == nil {
		p.pendingBytes = make(map[string]int)
	}
	p.pendingRows[tableName] = append(p.pendingRows[tableName], row)
	p.pendingBytes[tableName] += rowBytes
	if len(p.pendingRows[tableName]) >= p.batchConfig.MaxRows || p.pendingBytes[tableName] >= p.batchConfig.MaxBytes {
		rowsToFlush = p.pendingRows[tableName]
		delete(p.pendingRows, tableName)
		delete(p.pendingBytes, tableName)
	}
	p.bufferMu.Unlock()

	if len(rowsToFlush) == 0 {
		return nil
	}
	return p.flushRows(tableName, rowsToFlush)
}

func (p *PostgresSink) flushLoop() {
	defer close(p.flushDoneCh)
	for {
		select {
		case <-p.stopFlushCh:
			return
		case <-p.flushTicker.C:
			if err := p.flushAllBuffered(); err != nil && p.ctx != nil {
				p.ctx.Logger().Warn("postgres sink timed flush failed", sdk.Field{Key: "error", Value: err.Error()})
			}
		}
	}
}

func (p *PostgresSink) flushAllBuffered() error {
	p.bufferMu.Lock()
	if len(p.pendingRows) == 0 {
		p.bufferMu.Unlock()
		return nil
	}

	pendingRows := p.pendingRows
	p.pendingRows = make(map[string][]map[string]interface{})
	p.pendingBytes = make(map[string]int)
	p.bufferMu.Unlock()

	var failures []error
	for tableName, rows := range pendingRows {
		if err := p.flushRows(tableName, rows); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (p *PostgresSink) flushRows(tableName string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) == 1 {
		return p.writeSingleRow(tableName, rows[0])
	}

	if err := p.ensureColumnsForRows(tableName, rows); err != nil {
		p.reportWriteFailure(err, "batch")
		return postgresSinkRetryable(err)
	}

	columnNames := unionColumnNames(rows)
	placeholderGroups := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*len(columnNames))
	argIndex := 1
	for _, row := range rows {
		placeholders := make([]string, len(columnNames))
		for idx, col := range columnNames {
			placeholders[idx] = fmt.Sprintf("$%d", argIndex)
			argIndex++
			args = append(args, sqlArgValue(row[col]))
		}
		placeholderGroups = append(placeholderGroups, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName, strings.Join(columnNames, ", "), strings.Join(placeholderGroups, ", ")) + p.conflictClause()
	if err := p.connectorWrite(sqlWritePayload{SQL: query, Args: args}); err != nil {
		// A transport error can follow a successful database commit.
		p.reportWriteFailure(err, "batch")
		return postgresSinkRetryable(err)
	}
	p.ctx.Metrics().IncCounter("postgres_sink.inserts", map[string]string{"table": tableName})
	p.reportWriteSuccess(len(rows), "batch")
	p.reportHeartbeat(len(rows), "batch")
	return nil
}

func (p *PostgresSink) watchResourceEvents() {
	ch := p.ctx.ResourceEvents()
	if ch == nil {
		return
	}
	for event := range ch {
		p.onResourceStatus(event)
	}
}

func (p *PostgresSink) onResourceStatus(event sdk.ResourceStatusEvent) {
	p.reportStatus(event)
}
