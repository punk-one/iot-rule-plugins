package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"path"
	"strings"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	Directory    string `json:"directory"`
	TimeoutMS    int    `json:"timeout_ms"`
	RequireSync  bool   `json:"require_sync"`
	Ack          bool   `json:"ack_remote_receipt"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-sftp", Name: "SFTP File Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Stable event filenames and atomic no-replace publication, optional explicit remote file-sync receipt", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"sftp"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Directory: ".", TimeoutMS: 5000, RequireSync: true}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.Directory == "" || len(p.cfg.Directory) > 1024 || path.Clean(p.cfg.Directory) != p.cfg.Directory || path.IsAbs(p.cfg.Directory) || p.cfg.Directory == ".." || strings.HasPrefix(p.cfg.Directory, "../") || strings.ContainsAny(p.cfg.Directory, "\\\x00\r\n") || p.cfg.Ack && !p.cfg.RequireSync {
		return sdk.FatalError(errors.New("invalid SFTP sink configuration; ACK requires file sync"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("SFTP connector and emitter required")
	}
	p.ctx = ctx
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("SFTP sink not started"))
	}
	if e.ID == "" || len(e.ID) > 1024 {
		return sdk.SkippableError(errors.New("bounded stable event ID required"))
	}
	if e.Metadata["buffer_record_id"] != "" && !p.cfg.Ack {
		return sdk.SkippableError(errors.New("buffered input requires explicit ack_remote_receipt choice"))
	}
	if e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] == "" {
		return sdk.SkippableError(errors.New("buffer node ID missing"))
	}
	raw, err := json.Marshal(map[string]any{"event_id": e.ID, "payload": e.Payload})
	if err != nil || len(raw) > 1048576 {
		return sdk.SkippableError(errors.New("SFTP event JSON exceeds 1 MiB"))
	}
	name := sha256.Sum256([]byte(e.ID))
	digest := sha256.Sum256(raw)
	request, _ := json.Marshal(map[string]any{"path": path.Join(p.cfg.Directory, hex.EncodeToString(name[:])+".json"), "data": raw, "sha256": hex.EncodeToString(digest[:]), "require_sync": p.cfg.RequireSync})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "put", Payload: request, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("SFTP write failed; outcome may be unknown, reconcile stable event filename"))
	}
	var result struct {
		Received  bool `json:"received"`
		Synced    bool `json:"synced"`
		Duplicate bool `json:"duplicate"`
	}
	if len(response.Payload) > 1024 || json.Unmarshal(response.Payload, &result) != nil || !result.Received || p.cfg.RequireSync && !result.Synced {
		return sdk.SkippableError(errors.New("invalid SFTP receipt"))
	}
	if p.cfg.Ack && e.Metadata["buffer_record_id"] != "" {
		if e.Metadata["buffer_node_id"] == "" {
			return sdk.SkippableError(errors.New("buffer node ID missing"))
		}
		return p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess})
	}
	return nil
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}

const configSchema = `{"type":"object","required":["connector_ref"],"properties":{"connector_ref":{"type":"string"},"directory":{"type":"string","default":"."},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"require_sync":{"type":"boolean","default":true},"ack_remote_receipt":{"type":"boolean","default":false,"description":"Explicit remote synchronized-file receipt only; not business commit or remote directory power-loss durability"}},"additionalProperties":false}`
