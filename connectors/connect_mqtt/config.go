package main

import (
	"encoding/json"
	"fmt"
	"net/url"
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
    "clean_session":{"type":"boolean","default":true,"description":"MQTT 3.1.1 broker session; false requires a stable client identity and broker offline-queue limits. This is not business commit confirmation."},
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
	CleanSession            *bool  `json:"clean_session,omitempty"`
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
	if !strings.Contains(cfg.Broker, "://") {
		cfg.Broker = "tcp://" + cfg.Broker
	}
	endpoint, err := url.Parse(cfg.Broker)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return cfg, fmt.Errorf("MQTT broker must be an endpoint URL without embedded credentials/query")
	}
	switch endpoint.Scheme {
	case "tcp", "ssl", "tls", "tcps", "ws", "wss":
	default:
		return cfg, fmt.Errorf("unsupported MQTT broker transport")
	}
	if len(cfg.SharedSubscriptionGroup) > 128 || strings.ContainsAny(cfg.SharedSubscriptionGroup, "/+\x00#") {
		return cfg, fmt.Errorf("invalid shared subscription group")
	}
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
	if cfg.QueueSize > 4096 || cfg.ConnectTimeoutSec > 30 || len(cfg.ClientID) > 128 || strings.ContainsRune(cfg.ClientID, 0) {
		return cfg, fmt.Errorf("MQTT configuration limits exceeded")
	}
	return cfg, nil
}

func (c connectionConfig) connMaxLifetime() time.Duration {
	if c.ConnMaxLifetimeSec <= 0 {
		return 0
	}
	return time.Duration(c.ConnMaxLifetimeSec) * time.Second
}

func (c connectionConfig) cleanSession() bool { return c.CleanSession == nil || *c.CleanSession }
