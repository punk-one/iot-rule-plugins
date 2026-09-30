package main

import (
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	s7DefaultPort                  = 102
	s7ConnectTypePG                = 1
	s7ConnectTypeOP                = 2
	s7ConnectTypeBasic             = 3
	s7InvalidPDUText               = "ISO : Invalid PDU received"
	s7DefaultConnectType           = "pg"
	s7DefaultReuseHorizonMillis    = 2000
	s7DefaultFailureThreshold      = 3
	s7DefaultRetryBackoffMillis    = 1000
	s7DefaultMaxRetryBackoffMillis = 30000

	s7SessionPolicySharedDuplex = "shared_duplex" // legacy v1 input
	s7SessionPolicySplitRW      = "split_rw"      // legacy v1 input
)

const connectorConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["host"],
  "properties": {
    "host": {
      "type": "string",
      "title": "Host",
      "description": "PLC IP or hostname"
    },
    "port": {
      "type": "integer",
      "title": "Port",
      "minimum": 1,
      "maximum": 65535,
      "default": 102
    },
    "rack": {
      "type": "integer",
      "title": "Rack",
      "minimum": 0,
      "default": 0
    },
    "slot": {
      "type": "integer",
      "title": "Slot",
      "minimum": 0,
      "default": 1
    },
    "connect_type": {
      "type": "string",
      "title": "Connect Type",
      "enum": ["pg", "op", "basic"],
      "default": "pg"
    },
    "timeout_sec": {
      "type": "integer",
      "title": "Timeout",
      "minimum": 1,
      "default": 5
    },
    "idle_timeout_sec": {
      "type": "integer",
      "title": "Idle Timeout",
      "minimum": 1,
      "default": 30
    },
    "sessionReuseHorizonMillis": {
      "type": "integer",
      "title": "Session Reuse Horizon (ms)",
      "description": "Reuse a healthy session only when the next scheduled demand is this close; 0 enforces one session per batch",
      "minimum": 0,
      "maximum": 60000,
      "default": 2000
    },
    "closeWhenNoDemand": {
      "type": "boolean",
      "title": "Close When No Demand",
      "description": "Close the device session immediately when no source has announced a near-term scan",
      "default": true
    },
    "maxSessionAgeMillis": {
      "type": "integer",
      "title": "Maximum Session Age (ms)",
      "description": "Rotate a reused session at a safe batch boundary; 0 disables age-based rotation",
      "minimum": 0,
      "default": 0
    },
    "failureThreshold": {
      "type": "integer",
      "title": "Circuit Failure Threshold",
      "minimum": 1,
      "maximum": 100,
      "default": 3
    },
    "retryBackoffMillis": {
      "type": "integer",
      "title": "Initial Circuit Backoff (ms)",
      "minimum": 100,
      "default": 1000
    },
    "maxRetryBackoffMillis": {
      "type": "integer",
      "title": "Maximum Circuit Backoff (ms)",
      "minimum": 100,
      "default": 30000
    }
  },
  "additionalProperties": false
}`

type s7ConnectionConfig struct {
	Host                      string `json:"host"`
	Port                      int    `json:"port"`
	Rack                      int    `json:"rack"`
	Slot                      int    `json:"slot"`
	ConnectType               string `json:"connect_type"`
	TimeoutSec                int    `json:"timeout_sec"`
	IdleTimeout               int    `json:"idle_timeout_sec"`
	SessionReuseHorizonMillis *int   `json:"sessionReuseHorizonMillis"`
	CloseWhenNoDemand         *bool  `json:"closeWhenNoDemand"`
	MaxSessionAgeMillis       int    `json:"maxSessionAgeMillis"`
	FailureThreshold          int    `json:"failureThreshold"`
	RetryBackoffMillis        int    `json:"retryBackoffMillis"`
	MaxRetryBackoffMillis     int    `json:"maxRetryBackoffMillis"`
}

type s7Point struct {
	Address   string   `json:"address"`
	Name      string   `json:"name"`
	ValueType string   `json:"value_type"`
	MaxLength int      `json:"max_length,omitempty"`
	Scale     *float64 `json:"scale,omitempty"`
	Precision int      `json:"precision,omitempty"`
}

type s7WritePoint struct {
	Address   string `json:"address"`
	Name      string `json:"name"`
	ValueType string `json:"value_type"`
	MaxLength int    `json:"max_length,omitempty"`
	Value     any    `json:"value"`
}

type s7ParseAddressesPayload struct {
	Points []s7Point `json:"points"`
}

type s7BatchReadPayload struct {
	Points         []s7Point `json:"points"`
	BatchSize      int       `json:"batch_size"`
	NextDemandInMS int64     `json:"next_demand_in_ms,omitempty"`
}

type s7BatchWritePayload struct {
	Writes    []s7WritePoint `json:"writes"`
	BatchSize int            `json:"batch_size"`
}

func parseConnectionConfig(raw json.RawMessage) (s7ConnectionConfig, error) {
	var cfg s7ConnectionConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("connection config is required")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse connection config: %w", err)
	}

	cfg.Host = strings.TrimSpace(cfg.Host)
	if cfg.Host == "" {
		return cfg, fmt.Errorf("connection.host is required")
	}
	cfg.Port = s7NormalizePort(cfg.Port)
	cfg.ConnectType = s7NormalizeConnectType(cfg.ConnectType)
	if cfg.TimeoutSec <= 0 {
		cfg.TimeoutSec = 5
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 30
	}
	if cfg.SessionReuseHorizonMillis != nil && *cfg.SessionReuseHorizonMillis < 0 {
		return cfg, fmt.Errorf("connection.sessionReuseHorizonMillis must be >= 0")
	}
	if cfg.SessionReuseHorizonMillis == nil {
		reuseHorizon := s7DefaultReuseHorizonMillis
		cfg.SessionReuseHorizonMillis = &reuseHorizon
	}
	if cfg.CloseWhenNoDemand == nil {
		closeWhenNoDemand := true
		cfg.CloseWhenNoDemand = &closeWhenNoDemand
	}
	if cfg.MaxSessionAgeMillis < 0 {
		return cfg, fmt.Errorf("connection.maxSessionAgeMillis must be >= 0")
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = s7DefaultFailureThreshold
	}
	if cfg.RetryBackoffMillis <= 0 {
		cfg.RetryBackoffMillis = s7DefaultRetryBackoffMillis
	}
	if cfg.MaxRetryBackoffMillis <= 0 {
		cfg.MaxRetryBackoffMillis = s7DefaultMaxRetryBackoffMillis
	}
	if cfg.MaxRetryBackoffMillis < cfg.RetryBackoffMillis {
		cfg.MaxRetryBackoffMillis = cfg.RetryBackoffMillis
	}
	return cfg, nil
}

func normalizeSessionPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case sdk.ConnectorSessionPolicyPerOperation:
		return sdk.ConnectorSessionPolicyPerOperation
	case "", sdk.ConnectorSessionPolicyScheduledReuse,
		s7SessionPolicySharedDuplex, s7SessionPolicySplitRW:
		return sdk.ConnectorSessionPolicyScheduledReuse
	default:
		return sdk.ConnectorSessionPolicyScheduledReuse
	}
}
