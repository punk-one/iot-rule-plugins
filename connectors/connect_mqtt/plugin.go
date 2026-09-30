package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type mqttConnector struct {
	mu        sync.Mutex
	resources map[string]*mqttResource
}

type mqttResource struct {
	resource        sdk.ConnectorResource
	cfg             connectionConfig
	client          mqtt.Client
	lastConnectedAt time.Time
	mu              sync.Mutex
	bindings        map[string]*mqttBinding
	sessions        map[string]*mqttConsumerSession
	subscriptions   map[string]*mqttSubscription
}

type mqttBinding struct {
	topics map[string]struct{}
	inbox  chan mqttInboundMessage
}

type mqttSubscription struct {
	qos      byte
	tokens   map[string]struct{}
	sessions map[string]struct{}
}

type mqttInboundMessage struct {
	Topic         string `json:"topic"`
	Subscription  string `json:"subscription,omitempty"`
	PayloadBase64 string `json:"payload_base64"`
	ReceivedAt    string `json:"received_at,omitempty"`
	RawPayload    []byte `json:"-"`
}

func NewConnector() sdk.ConnectorPlugin {
	return &mqttConnector{
		resources: make(map[string]*mqttResource),
	}
}

func (c *mqttConnector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-mqtt",
		Name:        "Connector MQTT",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeConnector,
		Description: "Shared MQTT connector for source and sink plugins",
		Category:    "connector",
		Capabilities: sdk.PluginCapabilities{
			ConfigSchema: connectorConfigSchema,
			ConnectorDescriptor: &sdk.ConnectorDescriptor{
				Family:        "mqtt",
				Label:         "MQTT",
				Capabilities:  []string{"read", "write", "shared_duplex", "session:persistent"},
				DisplayFields: []string{"broker", "client_id"},
			},
		},
	}
}

func (c *mqttConnector) CreateResource(resource sdk.ConnectorResource) (string, error) {
	cfg, err := parseConnectionConfig(resource.Connection)
	if err != nil {
		return "", err
	}
	handle := stringsTrim(resource.ID)
	if handle == "" {
		return "", fmt.Errorf("resource id is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.resources[handle]; ok && current.client != nil {
		current.client.Disconnect(250)
	}
	c.resources[handle] = &mqttResource{
		resource:      resource,
		cfg:           cfg,
		bindings:      make(map[string]*mqttBinding),
		sessions:      make(map[string]*mqttConsumerSession),
		subscriptions: make(map[string]*mqttSubscription),
	}
	return handle, nil
}

func (c *mqttConnector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil
	}
	resource.mu.Lock()
	if resource.client != nil {
		resource.client.Disconnect(250)
		resource.client = nil
		resource.lastConnectedAt = time.Time{}
	}
	for _, session := range resource.sessions {
		session.close()
	}
	resource.mu.Unlock()
	delete(c.resources, providerHandle)
	return nil
}

func (c *mqttConnector) Execute(providerHandle string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	switch req.Kind {
	case sdk.ConnectorRequestMetadata:
		return c.executeMetadata(resource, req)
	case sdk.ConnectorRequestRead, sdk.ConnectorRequestBatchRead:
		return c.executeRead(resource, req)
	case sdk.ConnectorRequestWrite, sdk.ConnectorRequestBatchWrite:
		return c.executeWrite(resource, req)
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported request kind %q", req.Kind)
	}
}

func (c *mqttConnector) Probe(providerHandle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return c.probe(resource, req)
}

func (c *mqttConnector) OpenStream(providerHandle string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	return c.openConsumerStream(resource, req)
}

func (c *mqttConnector) ReceiveStream(providerHandle string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	return c.receiveConsumerStream(resource, req)
}

func (c *mqttConnector) AckStream(providerHandle string, req sdk.StreamAckRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.ackConsumerStream(resource, req)
}

func (c *mqttConnector) NackStream(providerHandle string, req sdk.StreamNackRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.nackConsumerStream(resource, req)
}

func (c *mqttConnector) GrantCredits(providerHandle string, req sdk.StreamGrantCreditsRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.grantConsumerCredits(resource, req)
}

func (c *mqttConnector) CloseStream(providerHandle string, req sdk.StreamCloseRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.closeConsumerStream(resource, req)
}

func (c *mqttConnector) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, resource := range c.resources {
		resource.mu.Lock()
		if resource.client != nil {
			resource.client.Disconnect(250)
			resource.client = nil
			resource.lastConnectedAt = time.Time{}
		}
		for _, session := range resource.sessions {
			session.close()
		}
		resource.mu.Unlock()
		delete(c.resources, id)
	}
	return nil
}

func (c *mqttConnector) getResource(providerHandle string) (*mqttResource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil, fmt.Errorf("resource %s not found", providerHandle)
	}
	return resource, nil
}

func (c *mqttConnector) ensureConnected(resource *mqttResource) (mqtt.Client, error) {
	resource.mu.Lock()
	if mqttClientReady(resource.client) && !resource.connectionExpired() {
		client := resource.client
		resource.mu.Unlock()
		return client, nil
	}
	if mqttClientReady(resource.client) && resource.connectionExpired() {
		resource.client.Disconnect(250)
		resource.client = nil
		resource.lastConnectedAt = time.Time{}
	}
	resource.mu.Unlock()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(resource.cfg.Broker)
	clientID := resource.cfg.ClientID
	if clientID == "" {
		clientID = "punk-mqtt-connector"
	}
	clientID = clientID + "-" + uuid.New().String()[:6]
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetConnectTimeout(time.Duration(resource.cfg.ConnectTimeoutSec) * time.Second)
	if resource.cfg.Username != "" {
		opts.SetUsername(resource.cfg.Username)
		opts.SetPassword(resource.cfg.Password)
	}
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		resource.mu.Lock()
		if resource.client == client {
			resource.client = nil
			resource.lastConnectedAt = time.Time{}
		}
		resource.mu.Unlock()
	})
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		_ = c.resubscribeAll(resource, client)
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	resource.mu.Lock()
	resource.client = client
	resource.lastConnectedAt = time.Now()
	resource.mu.Unlock()

	return client, c.resubscribeAll(resource, client)
}

func (r *mqttResource) connectionExpired() bool {
	lifetime := r.cfg.connMaxLifetime()
	if lifetime <= 0 || r.lastConnectedAt.IsZero() {
		return false
	}
	return time.Since(r.lastConnectedAt) >= lifetime
}

func (c *mqttConnector) resubscribeAll(resource *mqttResource, client mqtt.Client) error {
	type subscriptionState struct {
		topic string
		qos   byte
	}

	resource.mu.Lock()
	subscriptions := make([]subscriptionState, 0, len(resource.subscriptions))
	for topic, state := range resource.subscriptions {
		subscriptions = append(subscriptions, subscriptionState{topic: topic, qos: state.qos})
	}
	resource.mu.Unlock()

	for _, state := range subscriptions {
		effectiveTopic := resource.effectiveTopic(state.topic)
		token := client.Subscribe(effectiveTopic, state.qos, c.makeMessageHandler(resource, state.topic))
		if token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}

// effectiveTopic returns the MQTT topic with $share/ prefix if shared subscription is configured.
func (r *mqttResource) effectiveTopic(topic string) string {
	if r.cfg.SharedSubscriptionGroup == "" {
		return topic
	}
	return "$share/" + r.cfg.SharedSubscriptionGroup + "/" + topic
}

func (c *mqttConnector) makeMessageHandler(resource *mqttResource, subscription string) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		resource.mu.Lock()
		state := resource.subscriptions[subscription]
		targets := make([]*mqttBinding, 0)
		if state != nil {
			targets = make([]*mqttBinding, 0, len(state.tokens))
			for token := range state.tokens {
				if binding, ok := resource.bindings[token]; ok {
					targets = append(targets, binding)
				}
			}
		}
		sessions := make([]*mqttConsumerSession, 0)
		if state != nil {
			sessions = make([]*mqttConsumerSession, 0, len(state.sessions))
			for sessionID := range state.sessions {
				if session, ok := resource.sessions[sessionID]; ok {
					sessions = append(sessions, session)
				}
			}
		}
		resource.mu.Unlock()

		inbound := mqttInboundMessage{
			Topic:         msg.Topic(),
			Subscription:  subscription,
			PayloadBase64: base64.StdEncoding.EncodeToString(msg.Payload()),
			ReceivedAt:    time.Now().Format(time.RFC3339Nano),
			RawPayload:    append([]byte(nil), msg.Payload()...),
		}
		for _, binding := range targets {
			enqueueInbound(binding.inbox, inbound)
		}
		for _, session := range sessions {
			session.enqueue(inbound)
		}
	}
}

func enqueueInbound(ch chan mqttInboundMessage, message mqttInboundMessage) {
	select {
	case ch <- message:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- message:
		default:
		}
	}
}

func buildJSONResponse(payload any, metadata map[string]string) (sdk.ConnectorResponse, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	return sdk.ConnectorResponse{Payload: raw, Metadata: metadata}, nil
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}

func mqttClientReady(client mqtt.Client) bool {
	return client != nil && client.IsConnectionOpen()
}
