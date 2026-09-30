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
  "required": ["broker"],
  "properties": {
    "broker": {
      "type": "string",
      "title": "Broker",
      "description": "MQTT broker address, e.g. tcp://127.0.0.1:1883"
    },
    "client_id": {
      "type": "string",
      "title": "Client ID"
    },
    "username": {
      "type": "string",
      "title": "Username"
    },
    "password": {
      "type": "string",
      "title": "Password"
    },
    "connect_timeout_sec": {
      "type": "integer",
      "title": "Connect Timeout",
      "minimum": 1,
      "default": 10
    },
    "queue_size": {
      "type": "integer",
      "title": "Queue Size",
      "minimum": 1,
      "default": 256
    },
    "conn_max_lifetime_sec": {
      "type": "integer",
      "title": "Conn Max Lifetime",
      "minimum": 0,
      "default": 0
    },
    "shared_subscription_group": {
      "type": "string",
      "title": "Shared Subscription Group",
      "description": "MQTT shared subscription group name. When set, subscriptions use $share/<group>/<topic> format for load balancing across multiple consumers."
    }
  },
  "additionalProperties": false
}`

type connectionConfig struct {
	Broker                  string `json:"broker"`
	ClientID                string `json:"client_id"`
	Username                string `json:"username"`
	Password                string `json:"password"`
	ConnectTimeoutSec       int    `json:"connect_timeout_sec"`
	QueueSize               int    `json:"queue_size"`
	ConnMaxLifetimeSec      int    `json:"conn_max_lifetime_sec"`
	SharedSubscriptionGroup string `json:"shared_subscription_group"`
}

func parseConnectionConfig(raw json.RawMessage) (connectionConfig, error) {
	var cfg connectionConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("connection config is required")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse connection config: %w", err)
	}
	cfg.Broker = strings.TrimSpace(cfg.Broker)
	cfg.ClientID = strings.TrimSpace(cfg.ClientID)
	cfg.Username = strings.TrimSpace(cfg.Username)
	if cfg.Broker == "" {
		return cfg, fmt.Errorf("connection.broker is required")
	}
	if cfg.ConnectTimeoutSec <= 0 {
		cfg.ConnectTimeoutSec = 10
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
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
