package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"testing"
	"time"
)

type heldToken struct{ done chan struct{} }

func (t heldToken) Wait() bool { <-t.done; return true }
func (t heldToken) WaitTimeout(timeout time.Duration) bool {
	select {
	case <-t.done:
		return true
	case <-time.After(timeout):
		return false
	}
}
func (t heldToken) Done() <-chan struct{} { return t.done }
func (t heldToken) Error() error          { return nil }

type heldClient struct {
	mqtt.Client
	token                 heldToken
	started, disconnected chan struct{}
	once                  sync.Once
}

func (c *heldClient) Connect() mqtt.Token { close(c.started); return c.token }
func (c *heldClient) Disconnect(uint)     { c.once.Do(func() { close(c.disconnected) }) }

func TestDestroyDuringDialNeverRetainsPhysicalClient(t *testing.T) {
	connector := NewConnector().(*mqttConnector)
	handle, err := connector.CreateResource(sdk.ConnectorResource{ID: "test", Connection: mustJSON(t, map[string]any{"broker": "tcp://127.0.0.1:1883"})})
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := connector.getResource(handle)
	client := &heldClient{token: heldToken{done: make(chan struct{})}, started: make(chan struct{}), disconnected: make(chan struct{})}
	connector.newClient = func(*mqtt.ClientOptions) mqtt.Client { return client }
	connected := make(chan error, 1)
	go func() { _, err := connector.ensureConnected(resource); connected <- err }()
	<-client.started
	destroyed := make(chan struct{})
	go func() { _ = connector.DestroyResource(handle); close(destroyed) }()
	deadline := time.Now().Add(time.Second)
	retired := false
	for time.Now().Before(deadline) {
		resource.mu.Lock()
		retired = resource.closed
		resource.mu.Unlock()
		if retired {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(client.token.done)
	if !retired {
		t.Fatal("resource not retired before joining dial")
	}
	select {
	case err := <-connected:
		if err == nil {
			t.Fatal("dial succeeded after retirement")
		}
	case <-time.After(time.Second):
		t.Fatal("dial did not finish")
	}
	select {
	case <-destroyed:
	case <-time.After(time.Second):
		t.Fatal("destroy did not finish")
	}
	select {
	case <-client.disconnected:
	default:
		t.Fatal("late physical connection leaked")
	}
	if _, err := connector.ensureConnected(resource); err == nil {
		t.Fatal("retired resource reconnected")
	}
}
