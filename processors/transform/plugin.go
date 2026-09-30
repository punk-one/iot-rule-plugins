package main

import (
	"fmt"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

// TransformPlugin implements the Transform Processor plugin.
type TransformPlugin struct {
	sdk.BasePlugin
	cfg *Config
	ctx sdk.RuntimeContext
}

// Info returns plugin metadata.
func (p *TransformPlugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "transform",
		Name:        "Transform",
		Version:     "1.0.0",
		Description: "Transform payloads using copy/object/template mappings with JSONPath and Go template support",
		Type:        sdk.PluginTypeProcessor,
		Category:    "transform",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  1,
			SupportBatch: false,
			SupportAck:   false,
			ConfigSchema: transformConfigSchema,
		},
	}
}

// Init parses and validates the plugin configuration.
func (p *TransformPlugin) Init(cfg sdk.PluginConfig) error {
	parsed, err := compileConfigJSON(cfg.Raw)
	if err != nil {
		return fmt.Errorf("transform init: %w", err)
	}
	p.cfg = parsed
	return nil
}

// Start stores the runtime context.
func (p *TransformPlugin) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	return nil
}

// Stop performs cleanup.
func (p *TransformPlugin) Stop() error {
	return nil
}

// OnEvent processes a single event through the transform pipeline.
func (p *TransformPlugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return fmt.Errorf("runtime context not initialized")
	}

	payload := e.Payload
	if payload == nil {
		payload = make(map[string]interface{})
	}
	metadata := e.Metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}
	traceID := sdk.GetTraceID(e)

	// Build output payload
	output := make(map[string]interface{})

	// Copy metadata fields from input payload to output
	for _, key := range p.cfg.Process.Metadata.Copy {
		if v, ok := payload[key]; ok {
			output[key] = cloneValue(v)
		}
	}

	// Execute mappings in order
	for _, m := range p.cfg.Process.Mappings {
		if err := executeMapping(m, payload, metadata, traceID, output); err != nil {
			return fmt.Errorf("mapping %q: %w", m.Name, err)
		}
	}

	// Publish the output event
	out := e
	out.Payload = output
	out.RuleID = p.ctx.RuleID()
	out.Source = p.ctx.NodeID()
	if err := p.ctx.Emitter().Publish(out); err != nil {
		return err
	}

	return nil
}

// OnEvents handles batch events.
func (p *TransformPlugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}

// executeMapping dispatches to the appropriate mapping executor.
func executeMapping(m Mapping, payload map[string]interface{}, metadata map[string]string, traceID string, output map[string]interface{}) error {
	switch m.Type {
	case MappingCopy:
		key, value, err := executeCopyMapping(payload, m)
		if err != nil {
			return err
		}
		if key != "" && value != nil {
			output[key] = value
		}

	case MappingObject:
		key, value, err := executeObjectMapping(payload, metadata, traceID, m)
		if err != nil {
			return err
		}
		if key != "" && value != nil {
			output[key] = value
		}

	case MappingTemplate:
		key, value, err := executeTemplateMapping(payload, metadata, traceID, m)
		if err != nil {
			return err
		}
		if key != "" && value != nil {
			output[key] = value
		}
	}

	return nil
}

// Ensure plugin implements sdk.Plugin.
var _ sdk.Plugin = (*TransformPlugin)(nil)
