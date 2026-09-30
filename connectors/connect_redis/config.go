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
  "required": ["addr"],
  "properties": {
    "addr": {
      "type": "string",
      "title": "Address",
      "description": "Redis server address, e.g. 127.0.0.1:6379"
    },
    "username": {
      "type": "string",
      "title": "Username"
    },
    "password": {
      "type": "string",
      "title": "Password"
    },
    "db": {
      "type": "integer",
      "title": "DB",
      "default": 0,
      "minimum": 0
    },
    "dial_timeout_sec": {
      "type": "integer",
      "title": "Dial Timeout",
      "minimum": 1,
      "default": 5
    },
    "read_timeout_sec": {
      "type": "integer",
      "title": "Read Timeout",
      "minimum": 1,
      "default": 5
    },
    "write_timeout_sec": {
      "type": "integer",
      "title": "Write Timeout",
      "minimum": 1,
      "default": 5
    },
    "pool_size": {
      "type": "integer",
      "title": "Pool Size",
      "minimum": 1,
      "default": 8
    },
    "min_idle_conns": {
      "type": "integer",
      "title": "Min Idle Conns",
      "minimum": 0,
      "default": 0
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
	Addr               string `json:"addr"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	DB                 int    `json:"db"`
	DialTimeoutSec     int    `json:"dial_timeout_sec"`
	ReadTimeoutSec     int    `json:"read_timeout_sec"`
	WriteTimeoutSec    int    `json:"write_timeout_sec"`
	PoolSize           int    `json:"pool_size"`
	MinIdleConns       int    `json:"min_idle_conns"`
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

	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.Username = strings.TrimSpace(cfg.Username)
	if cfg.Addr == "" {
		return cfg, fmt.Errorf("connection.addr is required")
	}
	if cfg.DB < 0 {
		cfg.DB = 0
	}
	if cfg.DialTimeoutSec <= 0 {
		cfg.DialTimeoutSec = 5
	}
	if cfg.ReadTimeoutSec <= 0 {
		cfg.ReadTimeoutSec = 5
	}
	if cfg.WriteTimeoutSec <= 0 {
		cfg.WriteTimeoutSec = 5
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 8
	}
	if cfg.MinIdleConns < 0 {
		cfg.MinIdleConns = 0
	}
	if cfg.ConnMaxLifetimeSec < 0 {
		cfg.ConnMaxLifetimeSec = 0
	}
	return cfg, nil
}

func (c connectionConfig) dialTimeout() time.Duration {
	return time.Duration(c.DialTimeoutSec) * time.Second
}

func (c connectionConfig) readTimeout() time.Duration {
	return time.Duration(c.ReadTimeoutSec) * time.Second
}

func (c connectionConfig) writeTimeout() time.Duration {
	return time.Duration(c.WriteTimeoutSec) * time.Second
}

func (c connectionConfig) connMaxLifetime() time.Duration {
	if c.ConnMaxLifetimeSec <= 0 {
		return 0
	}
	return time.Duration(c.ConnMaxLifetimeSec) * time.Second
}
