package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const connectorConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["dsn"],
  "properties": {
    "dsn": {
      "type": "string",
      "title": "DSN",
      "description": "TDengine connection string, format: user:password@http(host:port)/database, default port 6041"
    },
    "max_open_conns": {
      "type": "integer",
      "title": "Max Open Conns",
      "minimum": 1,
      "default": 8
    },
    "max_idle_conns": {
      "type": "integer",
      "title": "Max Idle Conns",
      "minimum": 0,
      "default": 4
    },
    "conn_max_lifetime_sec": {
      "type": "integer",
      "title": "Conn Max Lifetime",
      "minimum": 0,
      "default": 300
    }
  },
  "additionalProperties": false
}`

type connectionConfig struct {
	DSN                string `json:"dsn"`
	MaxOpenConns       int    `json:"max_open_conns"`
	MaxIdleConns       int    `json:"max_idle_conns"`
	ConnMaxLifetimeSec int    `json:"conn_max_lifetime_sec"`
}

func parseConnectionConfig(raw json.RawMessage) (connectionConfig, error) {
	var cfg connectionConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("connection config is required")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse connection config: %w", err)
	}
	cfg.DSN = normalizeDSN(cfg.DSN)
	if strings.TrimSpace(cfg.DSN) == "" {
		return cfg, fmt.Errorf("connection.dsn is required")
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 8
	}
	if cfg.MaxIdleConns < 0 {
		cfg.MaxIdleConns = 0
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		cfg.MaxIdleConns = cfg.MaxOpenConns
	}
	if cfg.ConnMaxLifetimeSec < 0 {
		cfg.ConnMaxLifetimeSec = 0
	}
	return cfg, nil
}

func (c connectionConfig) connMaxLifetime() time.Duration {
	if c.ConnMaxLifetimeSec <= 0 {
		return 0
	}
	return time.Duration(c.ConnMaxLifetimeSec) * time.Second
}

func normalizeDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	// taosRestful driver requires http(host:port) format, not tcp(host:port)
	dsn = strings.Replace(dsn, "@tcp(", "@http(", 1)
	return dsn
}
