package main

import (
	"fmt"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type queuedRow struct {
	childTable string
	data       map[string]interface{}
}

func (p *TDengineSink) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return tdengineSinkFatal(fmt.Errorf("RuntimeContext not initialized"))
	}
	p.reportConnected()

	tableName, childTable, row, err := p.buildRowData(e)
	if err != nil || row == nil {
		return err
	}

	if p.writeMode == writeModeBatch {
		return p.enqueueRow(tableName, childTable, row)
	}
	return p.writeSingleRow(tableName, childTable, row)
}

func (p *TDengineSink) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *TDengineSink) buildRowData(e sdk.Event) (string, string, map[string]interface{}, error) {
	tableName := resolveTableName(p.tableTemplate, e.Metadata)
	if len(e.Payload) == 0 {
		return tableName, "", nil, nil
	}

	row := make(map[string]interface{}, len(e.Payload))
	for key, value := range e.Payload {
		normalized, err := normalizeColumnValue(value)
		if err != nil {
			return tableName, "", nil, sdk.SkippableError(fmt.Errorf("normalize payload field %q: %w", key, err))
		}
		row[key] = normalized
	}

	// Apply column mapping (e.g. time -> ts)
	row = applyColumnMapping(row, p.cfg.Output.ColumnMapping)

	var childTable string
	if p.childTableTemplate != "" {
		childTable = resolveChildTableName(p.childTableTemplate, row)
	}

	return tableName, childTable, row, nil
}

func (p *TDengineSink) writeSingleRow(tableName, childTable string, row map[string]interface{}) error {
	if err := p.ensureColumnsForRows(tableName, []map[string]interface{}{row}); err != nil {
		p.reportWriteFailure(err, "single")
		return tdengineSinkRetryable(err)
	}

	query := p.buildInsertSQL(tableName, childTable, row)
	p.ctx.Logger().Debug("tdengine sink write single row",
		sdk.Field{Key: "sql", Value: query},
	)
	if err := p.connectorWrite(sqlWritePayload{SQL: query}); err != nil {
		p.reportWriteFailure(err, "single")
		return tdengineSinkRetryable(err)
	}

	p.ctx.Metrics().IncCounter("tdengine_sink.inserts", map[string]string{"table": tableName})
	p.reportWriteSuccess(1, "single")
	p.reportHeartbeat(1, "single")
	return nil
}

func (p *TDengineSink) enqueueRow(tableName, childTable string, row map[string]interface{}) error {
	rowBytes := estimateRowSize(row)

	var rowsToFlush []queuedRow
	p.bufferMu.Lock()
	if p.pendingRows == nil {
		p.pendingRows = make(map[string][]queuedRow)
	}
	if p.pendingBytes == nil {
		p.pendingBytes = make(map[string]int)
	}
	p.pendingRows[tableName] = append(p.pendingRows[tableName], queuedRow{childTable: childTable, data: row})
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

func (p *TDengineSink) flushLoop() {
	defer close(p.flushDoneCh)
	for {
		select {
		case <-p.stopFlushCh:
			return
		case <-p.flushTicker.C:
			if err := p.flushAllBuffered(); err != nil && p.ctx != nil {
				p.ctx.Logger().Warn("tdengine sink timed flush failed", sdk.Field{Key: "error", Value: err.Error()})
			}
		}
	}
}

func (p *TDengineSink) flushAllBuffered() error {
	p.bufferMu.Lock()
	if len(p.pendingRows) == 0 {
		p.bufferMu.Unlock()
		return nil
	}

	pendingRows := p.pendingRows
	p.pendingRows = make(map[string][]queuedRow)
	p.pendingBytes = make(map[string]int)
	p.bufferMu.Unlock()

	for tableName, rows := range pendingRows {
		if err := p.flushRows(tableName, rows); err != nil {
			return err
		}
	}
	return nil
}

func (p *TDengineSink) flushRows(tableName string, rows []queuedRow) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) == 1 {
		return p.writeSingleRow(tableName, rows[0].childTable, rows[0].data)
	}

	// For supertable mode, each row may have a different child table, so we cannot batch them.
	// Fall back to row-by-row insert.
	if p.childTableTemplate != "" {
		return p.fallbackRowByRow(tableName, rows)
	}

	rawRows := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		rawRows[i] = r.data
	}

	if err := p.ensureColumnsForRows(tableName, rawRows); err != nil {
		p.reportWriteFailure(err, "batch")
		return tdengineSinkRetryable(err)
	}

	columnNames := unionColumnNames(rawRows)
	valueGroups := make([]string, 0, len(rawRows))
	for _, row := range rawRows {
		values := make([]string, len(columnNames))
		for idx := range columnNames {
			values[idx] = formatSQLValue(sqlArgValue(row[columnNames[idx]]))
		}
		valueGroups = append(valueGroups, "("+stringsJoin(values, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName, stringsJoin(columnNames, ", "), stringsJoin(valueGroups, ", "))
	p.ctx.Logger().Debug("tdengine sink flush batch rows",
		sdk.Field{Key: "sql", Value: query},
	)
	if err := p.connectorWrite(sqlWritePayload{SQL: query}); err == nil {
		p.ctx.Metrics().IncCounter("tdengine_sink.inserts", map[string]string{"table": tableName})
		p.reportWriteSuccess(len(rawRows), "batch")
		p.reportHeartbeat(len(rawRows), "batch")
		return nil
	}

	return p.fallbackRowByRow(tableName, rows)
}

func (p *TDengineSink) fallbackRowByRow(tableName string, rows []queuedRow) error {
	var failed int
	var firstErr error
	for _, r := range rows {
		if err := p.writeSingleRow(tableName, r.childTable, r.data); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed == 0 {
		return nil
	}
	return sdk.SkippableError(fmt.Errorf("tdengine batch fallback still failed for %d row(s): %w", failed, firstErr))
}

func (p *TDengineSink) watchResourceEvents() {
	ch := p.ctx.ResourceEvents()
	if ch == nil {
		return
	}
	for event := range ch {
		p.onResourceStatus(event)
	}
}

func (p *TDengineSink) onResourceStatus(event sdk.ResourceStatusEvent) {
	p.reportStatus(event)
}

func stringsJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}
	return result
}
