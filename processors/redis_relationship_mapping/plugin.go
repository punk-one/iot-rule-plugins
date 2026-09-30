package main

import (
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type RedisRelationshipMappingProcessor struct {
	sdk.BasePlugin
	cfg pluginConfig
	ctx sdk.RuntimeContext
}

func (p *RedisRelationshipMappingProcessor) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "redis-relationship-mapping",
		Name:        "Redis Relationship Mapping",
		Version:     "1.0.0",
		Description: "Enrich event fields from generic relationship tables stored in Redis Hashes",
		Type:        sdk.PluginTypeProcessor,
		Category:    "transform",
		Capabilities: sdk.PluginCapabilities{
			InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required: true, AcceptedFamilies: []string{"redis"}, RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (p *RedisRelationshipMappingProcessor) Init(cfg sdk.PluginConfig) error {
	if err := json.Unmarshal(cfg.Raw, &p.cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	p.cfg.ConnectorRef = strings.TrimSpace(p.cfg.ConnectorRef)
	p.cfg.DefaultHashKey = strings.TrimSpace(p.cfg.DefaultHashKey)
	if p.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if p.cfg.TimeoutMS == 0 {
		p.cfg.TimeoutMS = 3000
	}
	if p.cfg.TimeoutMS < 10 || p.cfg.TimeoutMS > 30000 {
		return fmt.Errorf("timeout_ms must be between 10 and 30000")
	}
	if len(p.cfg.Mappings) == 0 {
		return fmt.Errorf("at least one mapping is required")
	}
	for i := range p.cfg.Mappings {
		mapping := &p.cfg.Mappings[i]
		mapping.SourcePath = strings.TrimSpace(mapping.SourcePath)
		mapping.TargetPath = strings.TrimSpace(mapping.TargetPath)
		mapping.HashKey = strings.TrimSpace(mapping.HashKey)
		mapping.OnMissing = strings.ToLower(strings.TrimSpace(mapping.OnMissing))
		if mapping.OnMissing == "" {
			mapping.OnMissing = "skip"
		}
		if mapping.SourcePath == "" || mapping.TargetPath == "" {
			return fmt.Errorf("mappings[%d] source_path and target_path are required", i)
		}
		if strings.HasPrefix(mapping.SourcePath, "payload.") || strings.HasPrefix(mapping.TargetPath, "payload.") {
			return fmt.Errorf("mappings[%d] paths are relative to event.payload and must not start with payload.", i)
		}
		if mapping.HashKey == "" && p.cfg.DefaultHashKey == "" {
			return fmt.Errorf("mappings[%d] requires hash_key or default_hash_key", i)
		}
		switch mapping.OnMissing {
		case "skip", "default", "error":
		default:
			return fmt.Errorf("mappings[%d].on_missing must be skip, default, or error", i)
		}
	}
	return nil
}

func (p *RedisRelationshipMappingProcessor) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	return nil
}

func (p *RedisRelationshipMappingProcessor) Stop() error { return nil }

func (p *RedisRelationshipMappingProcessor) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *RedisRelationshipMappingProcessor) OnEvent(event sdk.Event) error {
	if p.ctx == nil {
		return fmt.Errorf("runtime context not initialized")
	}
	out := event.Clone()
	if out.Payload == nil {
		out.Payload = map[string]interface{}{}
	}
	for i, mapping := range p.cfg.Mappings {
		lookupValue := getPathValue(out.Payload, mapping.SourcePath)
		if lookupValue == nil || strings.TrimSpace(fmt.Sprint(lookupValue)) == "" {
			if err := applyMissing(out.Payload, mapping, fmt.Sprintf("source path %q is empty", mapping.SourcePath)); err != nil {
				return fmt.Errorf("mapping %d: %w", i, err)
			}
			continue
		}
		hashKey := mapping.HashKey
		if hashKey == "" {
			hashKey = p.cfg.DefaultHashKey
		}
		mappedValue, found, err := p.readHashField(hashKey, fmt.Sprint(lookupValue))
		if err != nil {
			return fmt.Errorf("mapping %d: %w", i, err)
		}
		if !found {
			if err := applyMissing(out.Payload, mapping, fmt.Sprintf("relationship %q was not found in hash %q", lookupValue, hashKey)); err != nil {
				return fmt.Errorf("mapping %d: %w", i, err)
			}
			continue
		}
		setPathValue(out.Payload, mapping.TargetPath, mappedValue)
	}
	out.RuleID = p.ctx.RuleID()
	out.Source = p.ctx.NodeID()
	if err := p.ctx.Emitter().Publish(out); err != nil {
		return fmt.Errorf("publish mapped event: %w", err)
	}
	return nil
}

func (p *RedisRelationshipMappingProcessor) readHashField(hashKey, field string) (string, bool, error) {
	payload, err := json.Marshal(map[string]string{"key": hashKey, "field": field})
	if err != nil {
		return "", false, fmt.Errorf("encode redis request: %w", err)
	}
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{
		Target: "hash_get", Payload: payload, TimeoutMS: p.cfg.TimeoutMS,
	})
	if err != nil {
		return "", false, fmt.Errorf("read redis relationship: %w", err)
	}
	var decoded struct {
		Fields map[string]string `json:"fields"`
	}
	if err := json.Unmarshal(response.Payload, &decoded); err != nil {
		return "", false, fmt.Errorf("decode redis response: %w", err)
	}
	value, found := decoded.Fields[field]
	return value, found, nil
}

func applyMissing(payload map[string]interface{}, mapping mappingConfig, reason string) error {
	switch mapping.OnMissing {
	case "skip":
		return nil
	case "default":
		setPathValue(payload, mapping.TargetPath, mapping.DefaultValue)
		return nil
	case "error":
		return fmt.Errorf("%s", reason)
	default:
		return fmt.Errorf("invalid on_missing policy %q", mapping.OnMissing)
	}
}

func getPathValue(payload map[string]interface{}, path string) interface{} {
	current := interface{}(payload)
	for _, part := range strings.Split(path, ".") {
		object, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current, ok = object[part]
		if !ok {
			return nil
		}
	}
	return current
}

func setPathValue(payload map[string]interface{}, path string, value interface{}) {
	parts := strings.Split(path, ".")
	current := payload
	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		next, ok := current[part].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			current[part] = next
		}
		current = next
	}
}

var _ sdk.Plugin = (*RedisRelationshipMappingProcessor)(nil)
