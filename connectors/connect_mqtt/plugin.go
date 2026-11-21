package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type mqttConnector struct {
	bufferedBytes atomic.Int64
	mu            sync.Mutex
	resources     map[string]*mqttResource
	closed        bool
	newClient     func(*mqtt.ClientOptions) mqtt.Client
}

type mqttResource struct {
	owner           *mqttConnector
	bufferedBytes   atomic.Int64
	dropped         atomic.Uint64
	retired         atomic.Bool
	orphans         []mqttInboundMessage
	subscriptionMu  sync.Mutex
	resource        sdk.ConnectorResource
	cfg             connectionConfig
	client          mqtt.Client
	lastConnectedAt time.Time
	mu              sync.Mutex
	connectMu       sync.Mutex
	closed          bool
	bindings        map[string]*mqttBinding
	sessions        map[string]*mqttConsumerSession
	subscriptions   map[string]*mqttSubscription
}

type mqttBinding struct {
	mu     sync.Mutex
	done   chan struct{}
	closed bool
	topics map[string]struct{}
	inbox  chan mqttInboundMessage
}

type mqttSubscription struct {
	subscribedClient mqtt.Client
	subscribedQoS    byte
	qos              byte
	tokens           map[string]struct{}
	sessions         map[string]struct{}
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
		newClient: mqtt.NewClient,
	}
}

func (c *mqttConnector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-mqtt",
		Name:        "Connector MQTT",
		Version:     "1.0.4",
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
	if c.closed {
		c.mu.Unlock()
		return "", fmt.Errorf("connector stopped")
	}
	if len(c.resources) >= 64 && c.resources[handle] == nil {
		c.mu.Unlock()
		return "", fmt.Errorf("MQTT resource limit reached")
	}
	current := c.resources[handle]
	c.resources[handle] = &mqttResource{
		owner:         c,
		resource:      resource,
		cfg:           cfg,
		bindings:      make(map[string]*mqttBinding),
		sessions:      make(map[string]*mqttConsumerSession),
		subscriptions: make(map[string]*mqttSubscription),
	}
	c.mu.Unlock()
	if current != nil {
		closeMQTTResource(current)
	}
	return handle, nil
}

func (c *mqttConnector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	resource, ok := c.resources[providerHandle]
	delete(c.resources, providerHandle)
	c.mu.Unlock()
	if ok {
		closeMQTTResource(resource)
	}
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
	c.closed = true
	resources := c.resources
	c.resources = make(map[string]*mqttResource)
	c.mu.Unlock()
	for _, resource := range resources {
		closeMQTTResource(resource)
	}
	return nil
}

func closeMQTTResource(resource *mqttResource) {
	resource.retired.Store(true)
	// Retire first so an in-flight dial cannot publish a new physical session.
	resource.mu.Lock()
	resource.closed = true
	resource.mu.Unlock()
	resource.connectMu.Lock()
	defer resource.connectMu.Unlock()
	resource.mu.Lock()
	client := resource.client
	resource.client = nil
	resource.lastConnectedAt = time.Time{}
	sessions := make([]*mqttConsumerSession, 0, len(resource.sessions))
	for _, session := range resource.sessions {
		sessions = append(sessions, session)
	}
	bindings := make([]*mqttBinding, 0, len(resource.bindings))
	for _, binding := range resource.bindings {
		bindings = append(bindings, binding)
	}
	orphans := resource.orphans
	resource.orphans = nil
	resource.mu.Unlock()
	for _, message := range orphans {
		resource.release(inboundCost(message))
	}
	for _, binding := range bindings {
		resource.closeBinding(binding)
	}
	for _, session := range sessions {
		session.close()
	}
	if client != nil {
		client.Disconnect(250)
	}
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
	resource.connectMu.Lock()
	defer resource.connectMu.Unlock()
	resource.mu.Lock()
	if resource.closed {
		resource.mu.Unlock()
		return nil, fmt.Errorf("resource stopped")
	}
	if mqttClientReady(resource.client) && !resource.connectionExpired() {
		client := resource.client
		resource.mu.Unlock()
		return client, c.resubscribeAll(resource, client)
	}
	if mqttClientReady(resource.client) && resource.connectionExpired() {
		resource.client.Disconnect(250)
		resource.client = nil
		resource.lastConnectedAt = time.Time{}
	}
	resource.mu.Unlock()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(resource.cfg.Broker)
	opts.SetClientID(stableClientID(resource))
	opts.SetCleanSession(resource.cfg.cleanSession())
	opts.SetResumeSubs(!resource.cfg.cleanSession())
	opts.SetProtocolVersion(4)
	opts.SetDefaultPublishHandler(c.makeMessageHandler(resource, ""))
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

	newClient := c.newClient
	if newClient == nil {
		newClient = mqtt.NewClient
	}
	client := newClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	resource.mu.Lock()
	if resource.closed {
		resource.mu.Unlock()
		client.Disconnect(0)
		return nil, fmt.Errorf("resource stopped while connecting")
	}
	resource.client = client
	resource.lastConnectedAt = time.Now()
	resource.mu.Unlock()

	return client, c.resubscribeAll(resource, client)
}

func stableClientID(resource *mqttResource) string {
	if resource.cfg.ClientID != "" {
		return resource.cfg.ClientID
	}
	namespace := os.Getenv("IOT_RULE_INSTANCE_ID")
	if namespace == "" {
		host, _ := os.Hostname()
		cwd, _ := os.Getwd()
		namespace = host + ":" + cwd + ":" + os.Getenv("IOT_RULE_DATA_DIR")
	}
	sum := sha256.Sum256([]byte(namespace + ":" + resource.resource.ID))
	return fmt.Sprintf("iot-%x", sum[:10])
}

func (r *mqttResource) connectionExpired() bool {
	lifetime := r.cfg.connMaxLifetime()
	if lifetime <= 0 || r.lastConnectedAt.IsZero() {
		return false
	}
	return time.Since(r.lastConnectedAt) >= lifetime
}

func (c *mqttConnector) resubscribeAll(resource *mqttResource, client mqtt.Client) error {
	resource.subscriptionMu.Lock()
	defer resource.subscriptionMu.Unlock()
	type intent struct {
		topic string
		qos   byte
		state *mqttSubscription
	}
	resource.mu.Lock()
	intents := []intent{}
	for topic, state := range resource.subscriptions {
		if state.subscribedClient != client || state.subscribedQoS != state.qos {
			intents = append(intents, intent{topic, state.qos, state})
		}
	}
	resource.mu.Unlock()
	for _, i := range intents {
		if resource.retired.Load() {
			return fmt.Errorf("MQTT resource retired")
		}
		if err := waitMQTT(client.Subscribe(resource.effectiveTopic(i.topic), i.qos, c.makeMessageHandler(resource, i.topic))); err != nil {
			return err
		}
		resource.mu.Lock()
		if resource.subscriptions[i.topic] == i.state && i.state.qos == i.qos {
			i.state.subscribedClient = client
			i.state.subscribedQoS = i.qos
		}
		resource.mu.Unlock()
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
	return func(client mqtt.Client, msg mqtt.Message) { c.message(resource, subscription, msg) }
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
