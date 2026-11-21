package main

import (
	"fmt"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	maxRedisStreamReadCount  = 1024
	maxRedisHashFields       = 4096
	maxRedisResultBytes      = 4 << 20
	maxRedisReadWaitMS       = 10000
	maxRedisStreamSessions   = 256
	maxRedisBufferedMessages = 4096
	maxRedisTTLSeconds       = int64(1<<63-1) / int64(time.Second)
)

func normalizeRedisReadLimits(payload *redisReadPayload) error {
	if payload.Count <= 0 {
		payload.Count = 1
	}
	if payload.BlockTimeoutMS <= 0 {
		payload.BlockTimeoutMS = 1000
	}
	if payload.Count > maxRedisStreamReadCount {
		return fmt.Errorf("Redis stream count exceeds %d", maxRedisStreamReadCount)
	}
	if payload.BlockTimeoutMS > maxRedisReadWaitMS {
		return fmt.Errorf("Redis stream block timeout exceeds %d ms", maxRedisReadWaitMS)
	}
	return nil
}

func boundedRedisResponse(payload any) (sdk.ConnectorResponse, error) {
	response, err := buildJSONResponse(payload, nil)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if len(response.Payload) > maxRedisResultBytes {
		return sdk.ConnectorResponse{}, fmt.Errorf("Redis response exceeds %d bytes", maxRedisResultBytes)
	}
	return response, nil
}
