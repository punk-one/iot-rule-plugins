package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/punk-one/gos7"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (c *s7Connector) executeMetadata(resource *s7Resource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "info":
		stats := resource.stats()
		return buildJSONResponse(map[string]any{
			"resource_id":    resource.resource.ID,
			"plugin_id":      c.Info().ID,
			"protocol":       "s7",
			"session_policy": resource.sessionPolicy,
			"session": map[string]any{
				"state":                stats.State,
				"opens":                stats.Opened,
				"reuses":               stats.Reused,
				"closes":               stats.Closed,
				"consecutive_failures": stats.ConsecutiveFailures,
				"circuit_open_until":   stats.CircuitOpenUntil,
			},
			"connection": map[string]any{
				"host":         resource.cfg.Host,
				"port":         resource.cfg.Port,
				"rack":         resource.cfg.Rack,
				"slot":         resource.cfg.Slot,
				"connect_type": resource.cfg.ConnectType,
			},
			"supported_targets": []string{
				"metadata/info",
				"metadata/parse_addresses",
				"metadata/ping",
				"batch_read/points",
				"read/release_demand",
				"batch_write/points",
			},
		}, map[string]string{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
		})
	case "parse_addresses":
		var payload s7ParseAddressesPayload
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("parse metadata payload: %w", err)
		}
		points := make([]map[string]any, 0, len(payload.Points))
		for _, point := range payload.Points {
			info, err := s7ParseAddress(point.Address)
			if err != nil {
				points = append(points, map[string]any{
					"name":    point.Name,
					"address": point.Address,
					"error":   err.Error(),
				})
				continue
			}
			_, byteSize := s7ResolveReadSize(info, point.ValueType, point.MaxLength)
			points = append(points, map[string]any{
				"name":                  point.Name,
				"address":               point.Address,
				"value_type":            point.ValueType,
				"normalized_value_type": normalizeValueType(point.ValueType),
				"area":                  info.Area,
				"db_number":             info.DBNumber,
				"start":                 info.Start,
				"bit":                   info.Bit,
				"word_length":           info.WordLength,
				"byte_size":             byteSize,
			})
		}
		return buildJSONResponse(map[string]any{"points": points}, nil)
	case "ping":
		var status int
		err := c.withClient(resource, req, func(client gos7.Client) error {
			value, err := client.PLCGetStatus()
			status = value
			return err
		})
		if err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("plc ping failed: %w", err)
		}
		return buildJSONResponse(map[string]any{
			"ok":              true,
			"plc_status_code": status,
		}, map[string]string{
			"status": "healthy",
		})
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported metadata target %q", req.Target)
	}
}

func (c *s7Connector) probe(resource *s7Resource, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	var status int
	err := c.withClient(resource, req, func(client gos7.Client) error {
		value, err := client.PLCGetStatus()
		status = value
		return err
	})
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}

	return sdk.ResourceStatusEvent{
		ResourceID:         resource.resource.ID,
		ProviderPluginID:   c.Info().ID,
		Status:             sdk.ResourceStatusHealthy,
		ReasonCode:         "READY",
		Message:            "plc reachable",
		Details:            map[string]string{"plc_status_code": fmt.Sprintf("%d", status)},
		ObservedAtUnixNano: time.Now().UnixNano(),
	}, nil
}

func (c *s7Connector) executeRead(resource *s7Resource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	if req.Target == "release_demand" {
		if err := c.releaseDemand(resource, req.ResourceToken); err != nil {
			return sdk.ConnectorResponse{}, err
		}
		return buildJSONResponse(map[string]any{"released": true}, nil)
	}
	var payload s7BatchReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse read payload: %w", err)
	}
	if len(payload.Points) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("read.points is required")
	}
	batchSize := payload.BatchSize
	if batchSize <= 0 || batchSize > 19 {
		batchSize = 16
	}

	values := make(map[string]any, len(payload.Points))
	startedAt := time.Now()
	deadline := startedAt.Add(requestTimeout(req.TimeoutMS, time.Duration(resource.cfg.TimeoutSec)*time.Second))
	chunkCount := 0
	err := c.withClient(resource, req, func(client gos7.Client) error {
		var helper gos7.Helper
		for start := 0; start < len(payload.Points); start += batchSize {
			if time.Now().After(deadline) {
				return s7OperationDeadlineError(req)
			}
			chunkCount++
			end := start + batchSize
			if end > len(payload.Points) {
				end = len(payload.Points)
			}
			batch := payload.Points[start:end]

			bufs := make([][]byte, len(batch))
			items := make([]gos7.S7DataItem, len(batch))
			infos := make([]*s7DBInfo, len(batch))

			for i, point := range batch {
				info, err := s7ParseAddress(point.Address)
				if err != nil {
					return fmt.Errorf("parse address %s: %w", point.Address, err)
				}
				wordLength, byteSize := s7ResolveReadSize(info, point.ValueType, point.MaxLength)
				infos[i] = info
				bufs[i] = make([]byte, byteSize)
				items[i] = gos7.S7DataItem{
					Area:     info.Area,
					WordLen:  wordLength,
					DBNumber: info.DBNumber,
					Start:    info.Start,
					Amount:   byteSize,
					Data:     bufs[i],
				}
			}

			if err := client.AGReadMulti(items, len(items)); err != nil {
				return fmt.Errorf("ag_read_multi: %w", err)
			}
			if time.Now().After(deadline) {
				return s7OperationDeadlineError(req)
			}
			for i, point := range batch {
				if items[i].Error != "" {
					return fmt.Errorf("read item %s failed: %s", point.Address, items[i].Error)
				}
				value, err := s7ExtractValue(helper, bufs[i], infos[i], point.ValueType)
				if err != nil {
					return fmt.Errorf("extract value %s: %w", point.Address, err)
				}
				value, err = s7ApplyPointTransforms(value, point)
				if err != nil {
					return fmt.Errorf("apply transforms %s: %w", point.Address, err)
				}
				key := point.Name
				if key == "" {
					key = point.Address
				}
				values[key] = value
			}
		}
		return nil
	})
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	return buildJSONResponse(map[string]any{
		"values": values,
	}, map[string]string{
		"s7_chunks":           fmt.Sprintf("%d", chunkCount),
		"s7_scan_duration_ms": fmt.Sprintf("%d", time.Since(startedAt).Milliseconds()),
	})
}

func (c *s7Connector) executeWrite(resource *s7Resource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload s7BatchWritePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse write payload: %w", err)
	}
	if len(payload.Writes) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.writes is required")
	}
	batchSize := payload.BatchSize
	if batchSize <= 0 || batchSize > 19 {
		batchSize = 16
	}

	successCount := 0
	deadline := time.Now().Add(requestTimeout(req.TimeoutMS, time.Duration(resource.cfg.TimeoutSec)*time.Second))
	err := c.withClient(resource, req, func(client gos7.Client) error {
		var helper gos7.Helper
		for start := 0; start < len(payload.Writes); start += batchSize {
			if time.Now().After(deadline) {
				return s7OperationDeadlineError(req)
			}
			end := start + batchSize
			if end > len(payload.Writes) {
				end = len(payload.Writes)
			}
			batch := payload.Writes[start:end]

			items := make([]gos7.S7DataItem, 0, len(batch))
			for _, write := range batch {
				if time.Now().After(deadline) {
					return s7OperationDeadlineError(req)
				}
				info, err := s7ParseAddress(write.Address)
				if err != nil {
					return fmt.Errorf("parse address %s: %w", write.Address, err)
				}
				if info.WordLength == s7WLBit {
					if err := writeBit(client, info, write.Value); err != nil {
						return fmt.Errorf("write bit %s: %w", write.Address, err)
					}
					successCount++
					continue
				}
				if s7IsStringType(write.ValueType) {
					item, err := s7BuildStringWriteItem(helper, info, write)
					if err != nil {
						return fmt.Errorf("build string write %s: %w", write.Address, err)
					}
					items = append(items, item)
					continue
				}
				buf, err := s7EncodeValue(helper, info, write.ValueType, write.MaxLength, write.Value)
				if err != nil {
					return fmt.Errorf("encode value %s: %w", write.Address, err)
				}
				items = append(items, gos7.S7DataItem{
					Area:     info.Area,
					WordLen:  info.WordLength,
					DBNumber: info.DBNumber,
					Start:    info.Start,
					Amount:   info.Amount,
					Data:     buf,
				})
			}

			if len(items) == 0 {
				continue
			}
			if err := client.AGWriteMulti(items, len(items)); err != nil {
				return fmt.Errorf("ag_write_multi: %w", err)
			}
			if time.Now().After(deadline) {
				return s7OperationDeadlineError(req)
			}
			successCount += len(items)
		}
		return nil
	})
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	return buildJSONResponse(map[string]any{
		"writes": successCount,
	}, nil)
}

func writeBit(client gos7.Client, info *s7DBInfo, val interface{}) error {
	readBuf := make([]byte, 1)
	readItem := []gos7.S7DataItem{{
		Area:     info.Area,
		WordLen:  s7WLByte,
		DBNumber: info.DBNumber,
		Start:    info.Start,
		Amount:   1,
		Data:     readBuf,
	}}
	if err := client.AGReadMulti(readItem, 1); err != nil {
		return fmt.Errorf("bit read-before-write failed: %w", err)
	}

	boolVal := false
	switch v := val.(type) {
	case bool:
		boolVal = v
	case float64:
		boolVal = v != 0
	case int:
		boolVal = v != 0
	case string:
		boolVal = v == "true" || v == "1"
	}
	bit := info.Bit
	if bit < 0 {
		bit = 0
	}
	if boolVal {
		readBuf[0] |= 1 << uint(bit)
	} else {
		readBuf[0] &^= 1 << uint(bit)
	}

	writeItem := []gos7.S7DataItem{{
		Area:     info.Area,
		WordLen:  s7WLByte,
		DBNumber: info.DBNumber,
		Start:    info.Start,
		Amount:   1,
		Data:     readBuf,
	}}
	return client.AGWriteMulti(writeItem, 1)
}

func normalizeValueType(valueType string) string {
	return strings.ToLower(strings.TrimSpace(valueType))
}
