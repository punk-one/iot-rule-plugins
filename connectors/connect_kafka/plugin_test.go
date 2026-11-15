package main

import (
	"context"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T) (*connector, string, *kfake.Cluster) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "telemetry"), kfake.BrokerConfigs(map[string]string{"group.initial.rebalance.delay.ms": "0"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	raw, _ := json.Marshal(map[string]any{"brokers": cluster.ListenAddrs(), "topics": []string{"telemetry"}, "tls": false, "allow_write": true, "timeout_ms": 3000})
	c := NewConnector().(*connector)
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "kafka", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h, cluster
}
func publish(t *testing.T, c *connector, h, key string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"topic": "telemetry", "key": key, "value": []byte(`{"value":42}`)})
	if _, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "publish", Payload: raw}); err != nil {
		t.Fatal(err)
	}
}
func open(t *testing.T, c *connector, h, session, group string) string {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"group_id": group, "reset": "earliest"})
	result, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: session, Target: "telemetry", Payload: raw, InitialCredits: 1, MaxBufferedMessages: 1, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if err != nil {
		t.Fatal(err)
	}
	return result.StreamID
}
func receive(t *testing.T, c *connector, h, id string) sdk.StreamMessage {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 1000})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Messages) == 1 {
			return response.Messages[0]
		}
	}
	t.Fatal("Kafka receive deadline exceeded")
	return sdk.StreamMessage{}
}
func TestKafkaGroupManualCommitAndUnackedReplay(t *testing.T) {
	c, h, cluster := fixture(t)
	var commits atomic.Int32
	cluster.ControlKey(8, func(kmsg.Request) (kmsg.Response, error, bool) {
		commits.Add(1)
		cluster.KeepControl()
		return nil, nil, false
	})
	publish(t, c, h, "one")
	publish(t, c, h, "two")
	id := open(t, c, h, "node", "iot-rule.manual")
	first := receive(t, c, h, id)
	if first.Metadata["offset"] != "0" || commits.Load() != 0 {
		t.Fatal(first, commits.Load())
	}
	response, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 10})
	if err != nil || len(response.Messages) != 0 {
		t.Fatal("delivered beyond unacked record", err, response)
	}
	if c.AckStream(h, sdk.StreamAckRequest{StreamID: id, DeliveryID: "wrong", Credits: 1}) == nil || commits.Load() != 0 {
		t.Fatal("wrong/early offset commit")
	}
	if err = c.AckStream(h, sdk.StreamAckRequest{StreamID: id, DeliveryID: first.DeliveryID, Credits: 1}); err != nil {
		t.Fatal(err)
	}
	second := receive(t, c, h, id)
	if second.Metadata["offset"] != "1" || commits.Load() != 1 {
		t.Fatal(second, commits.Load())
	}
	started := time.Now()
	if err = c.CloseStream(h, sdk.StreamCloseRequest{StreamID: id}); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("consumer stop blocked")
	}
	reopened := open(t, c, h, "node", "iot-rule.manual")
	if reopened == id {
		t.Fatal("stream generation reused")
	}
	replay := receive(t, c, h, reopened)
	if replay.DeliveryID != second.DeliveryID || commits.Load() != 1 {
		t.Fatal("uncommitted record lost", replay, second, commits.Load())
	}
	if err = c.AckStream(h, sdk.StreamAckRequest{StreamID: reopened, DeliveryID: replay.DeliveryID, Credits: 1}); err != nil {
		t.Fatal(err)
	}
}
func TestIndependentKafkaGroupsAndStopIsolation(t *testing.T) {
	c, h, _ := fixture(t)
	publish(t, c, h, "one")
	a := open(t, c, h, "a", "iot-rule.a")
	b := open(t, c, h, "b", "iot-rule.b")
	ma := receive(t, c, h, a)
	mb := receive(t, c, h, b)
	if ma.DeliveryID != mb.DeliveryID {
		t.Fatal("different groups did not fan out same log record")
	}
	if err := c.AckStream(h, sdk.StreamAckRequest{StreamID: b, DeliveryID: mb.DeliveryID, Credits: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseStream(h, sdk.StreamCloseRequest{StreamID: a}); err != nil {
		t.Fatal(err)
	}
	publish(t, c, h, "two")
	next := receive(t, c, h, b)
	if next.Metadata["offset"] != "1" {
		t.Fatal(next)
	}
	r, _ := c.get(h)
	r.mu.Lock()
	count := len(r.streams)
	r.mu.Unlock()
	if count != 1 {
		t.Fatal(count)
	}
}
func TestKafkaRejectedRequestsAndOfflineClose(t *testing.T) {
	c, h, _ := fixture(t)
	raw := json.RawMessage(`{"group_id":"outside-prefix","reset":"earliest"}`)
	if _, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: "x", Target: "telemetry", Payload: raw, InitialCredits: 1, MaxBufferedMessages: 1, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"}); err == nil {
		t.Fatal("group prefix bypass")
	}
	r, _ := c.get(h)
	if r.producer != nil {
		t.Fatal("offline create dialed")
	}
	raw, _ = json.Marshal(map[string]any{"topic": "forbidden", "key": "x", "value": []byte("x")})
	if _, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "publish", Payload: raw}); err == nil {
		t.Fatal("topic bypass")
	}
	if r.producer != nil {
		t.Fatal("rejected publish created client")
	}
	var cfg config
	json.Unmarshal(json.RawMessage(`{"brokers":["192.0.2.1:9092"],"topics":["x"],"tls":false}`), &cfg)
	bytes, _ := json.Marshal(cfg)
	if _, err := NewConnector().CreateResource(sdk.ConnectorResource{ID: "bad", Connection: bytes}); err == nil {
		t.Fatal("plaintext nonlocal broker accepted")
	}
}
func TestKafkaWriteDeadlineHasNoPositiveReceipt(t *testing.T) {
	c := NewConnector().(*connector)
	defer c.Stop()
	raw := json.RawMessage(`{"brokers":["127.0.0.1:1"],"topics":["telemetry"],"tls":false,"allow_write":true,"timeout_ms":200}`)
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "offline", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(map[string]any{"topic": "telemetry", "key": "x", "value": []byte("x")})
	started := time.Now()
	if _, err = c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "publish", Payload: request, TimeoutMS: 100}); err == nil {
		t.Fatal("offline publish accepted")
	}
	if time.Since(started) > time.Second {
		t.Fatal("publish deadline ignored")
	}
	id := open(t, c, h, "offline", "iot-rule.offline")
	started = time.Now()
	c.CloseStream(h, sdk.StreamCloseRequest{StreamID: id})
	if time.Since(started) > 2*time.Second {
		t.Fatal("offline consumer close blocked")
	}
}
func TestConfiguredKafkaBounds(t *testing.T) {
	c, h, _ := fixture(t)
	r, _ := c.get(h)
	client, err := kgo.NewClient(r.options(context.Background())...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.OptValue(kgo.MaxDecompressBatchBytes) != (1048576) || client.OptValue(kgo.BrokerMaxReadBytes) != int32(2*1048576) || client.OptValue(kgo.RecordRetries) != int64(0) {
		t.Fatal("native Kafka memory/retry bounds changed")
	}
}
