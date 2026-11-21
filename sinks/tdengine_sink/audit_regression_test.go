package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestRejectInvalidResolvedSQLIdentifiersBeforeConnectorIO(t *testing.T) {
	connector := &tdConnector{}
	sink := &TDengineSink{ctx: tdRuntimeContext{connector: connector}, tableTemplate: "${metadata.table}", autoCreateCols: true}
	for _, event := range []sdk.Event{
		{Metadata: map[string]string{"table": "invalid table"}, Payload: map[string]any{"reading": 1}},
		{Metadata: map[string]string{"table": "readings"}, Payload: map[string]any{"invalid field": 1}},
		{Metadata: map[string]string{"table": "readings"}, Payload: map[string]any{"Reading": 1, "reading": 2}},
	} {
		if err := sink.OnEvent(event); err == nil {
			t.Fatal("expected invalid identifiers to be rejected")
		}
	}
	if len(connector.writes)+len(connector.reads) != 0 {
		t.Fatal("invalid identifiers reached connector")
	}
}

func TestFailedBatchDoesNotReplayAndOtherTablesAreFlushed(t *testing.T) {
	responseLost := errors.New("write response unavailable")
	connector := &tdConnector{writeFn: func(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		var write sqlWritePayload
		if err := json.Unmarshal(req.Payload, &write); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(write.SQL, "INSERT INTO failed_table ") {
			return sdk.ConnectorResponse{}, responseLost
		}
		return sdk.ConnectorResponse{}, nil
	}}
	row := func(v int) queuedRow { return queuedRow{data: map[string]interface{}{"reading": v}} }
	sink := &TDengineSink{ctx: tdRuntimeContext{connector: connector}, pendingRows: map[string][]queuedRow{
		"failed_table": {row(1), row(2)}, "other_table": {row(3), row(4)},
	}}
	if err := sink.flushAllBuffered(); err == nil {
		t.Fatal("ambiguous write failure was swallowed")
	}
	if len(connector.writes) != 2 {
		t.Fatalf("expected one write attempt per table, got %d", len(connector.writes))
	}
}

func TestTDengineQuotesBackslashesAndApostrophes(t *testing.T) {
	if result := formatSQLValue(`machine\line O'Brien`); result != `'machine\\line O\'Brien'` {
		t.Fatalf("unexpected TDengine text serialization: %q", result)
	}
}
