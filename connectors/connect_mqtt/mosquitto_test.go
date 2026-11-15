package main

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestIndependentMosquittoPersistentQueueAndReconnect(t *testing.T) {
	binary := os.Getenv("TEST_MOSQUITTO_BINARY")
	if binary == "" {
		t.Skip("independent Mosquitto executable not provided")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "mosquitto.conf")
	text := fmt.Sprintf("listener %d 127.0.0.1\nallow_anonymous true\npersistence true\npersistence_location %s/\nautosave_interval 1\nmax_queued_messages 1000\n", port, dir)
	if err = os.WriteFile(config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(dir, "broker.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var process *exec.Cmd
	start := func() {
		process = exec.Command(binary, "-c", config)
		process.Stdout = log
		process.Stderr = log
		if err = process.Start(); err != nil {
			t.Fatal(err)
		}
		end := time.Now().Add(3 * time.Second)
		for time.Now().Before(end) {
			conn, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
			if e == nil {
				conn.Close()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("Mosquitto did not start")
	}
	stop := func() {
		if process != nil {
			_ = process.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = process.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = process.Process.Kill()
				<-done
			}
			process = nil
		}
	}
	start()
	defer stop()
	broker := fmt.Sprintf("tcp://127.0.0.1:%d", port)
	identity := "persistent-" + rand.Text()
	resource := sdk.ConnectorResource{ID: "r", Connection: mustJSON(t, map[string]any{"broker": broker, "client_id": identity, "clean_session": false, "connect_timeout_sec": 1})}
	create := func() *mqttConnector {
		c := NewConnector().(*mqttConnector)
		if _, e := c.CreateResource(resource); e != nil {
			t.Fatal(e)
		}
		return c
	}
	open := func(c *mqttConnector, key, topic string) string {
		payload := mustJSON(t, map[string]any{"topics": []string{topic}, "qos": 1})
		reply, e := c.OpenStream("r", sdk.StreamOpenRequest{SessionKey: key, Payload: payload, InitialCredits: 1, MaxBufferedMessages: 16})
		if e != nil {
			t.Fatal(e)
		}
		return reply.StreamID
	}
	publish := func(topic, value string) {
		client := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(broker).SetClientID("publisher-" + rand.Text()).SetConnectTimeout(time.Second).SetAutoReconnect(false))
		if e := waitMQTT(client.Connect()); e != nil {
			t.Fatal(e)
		}
		defer client.Disconnect(100)
		if e := waitMQTT(client.Publish(topic, 1, false, []byte(value))); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(c *mqttConnector, id, value string) {
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			response, e := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 200})
			if e != nil {
				t.Fatal(e)
			}
			for _, message := range response.Messages {
				if string(message.RawPayload) == value {
					if e = c.AckStream("r", sdk.StreamAckRequest{StreamID: id, DeliveryID: message.DeliveryID, Credits: 1}); e != nil {
						t.Fatal(e)
					}
					return
				}
				t.Fatalf("unexpected packet %s", message.RawPayload)
			}
		}
		t.Fatal("missing queued publication", value)
	}
	c := create()
	a := open(c, "a", "fixture/a")
	b := open(c, "b", "fixture/b")
	publish("fixture/a", "first")
	receive(c, a, "first")
	publish("fixture/b", "second")
	receive(c, b, "second")
	// Connector Stop models client-process retirement without intentional topic unsubscribe.
	if err = c.Stop(); err != nil {
		t.Fatal(err)
	}
	if c.bufferedBytes.Load() != 0 {
		t.Fatal("retired resource retained queue memory")
	}
	publish("fixture/a", "offline-a")
	publish("fixture/b", "offline-b")
	c = create()
	defer c.Stop()
	a = open(c, "a", "fixture/a")
	// Broker may deliver B's offline message before that Source has registered.
	time.Sleep(100 * time.Millisecond)
	b = open(c, "b", "fixture/b")
	receive(c, a, "offline-a")
	receive(c, b, "offline-b")
	stop()
	time.Sleep(150 * time.Millisecond)
	if _, e := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: a, WaitTimeoutMS: 50}); e == nil {
		t.Fatal("offline broker looked like an indefinitely idle healthy stream")
	}
	start()
	// An existing Source's next read restores the physical session and subscriptions.
	if _, e := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: a, WaitTimeoutMS: 50}); e != nil {
		t.Fatal(e)
	}
	publish("fixture/a", "recovered")
	receive(c, a, "recovered")
	if e := c.CloseStream("r", sdk.StreamCloseRequest{StreamID: a}); e != nil {
		t.Fatal(e)
	}
	publish("fixture/b", "only-b")
	receive(c, b, "only-b")
	if err = c.Stop(); err != nil {
		t.Fatal(err)
	}
	if c.bufferedBytes.Load() != 0 {
		t.Fatal("reconnected resource leaked memory")
	}
}
