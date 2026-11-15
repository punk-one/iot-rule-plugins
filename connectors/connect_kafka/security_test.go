package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func ephemeralTLS(t *testing.T) (*tls.Config, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "loopback Kafka fixture"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}, string(certPEM)
}
func TestKafkaVerifiedTLSAndExplicitSASL(t *testing.T) {
	for _, mechanism := range []string{"plain", "scram-sha-256", "scram-sha-512"} {
		t.Run(mechanism, func(t *testing.T) {
			serverTLS, ca := ephemeralTLS(t)
			secret := make([]byte, 16)
			rand.Read(secret)
			password := hex.EncodeToString(secret)
			cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "telemetry"), kfake.TLS(serverTLS), kfake.EnableSASL(), kfake.Superuser(strings.ToUpper(mechanism), "fixture", password))
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			cfg := map[string]any{"brokers": cluster.ListenAddrs(), "topics": []string{"telemetry"}, "tls": true, "ca_pem": ca, "username": "fixture", "password": password, "sasl": mechanism, "allow_write": true, "timeout_ms": 3000}
			raw, _ := json.Marshal(cfg)
			c := NewConnector().(*connector)
			defer c.Stop()
			h, err := c.CreateResource(sdk.ConnectorResource{ID: "tls", Connection: raw})
			if err != nil {
				t.Fatal(err)
			}
			publish(t, c, h, "secure")
			id := open(t, c, h, "secure-node", "iot-rule.secure")
			record := receive(t, c, h, id)
			if err = c.AckStream(h, sdk.StreamAckRequest{StreamID: id, DeliveryID: record.DeliveryID, Credits: 1}); err != nil {
				t.Fatal(err)
			}
			_, wrongCA := ephemeralTLS(t)
			cfg["ca_pem"] = wrongCA
			raw, _ = json.Marshal(cfg)
			bad := NewConnector().(*connector)
			defer bad.Stop()
			h, err = bad.CreateResource(sdk.ConnectorResource{ID: "wrong-ca", Connection: raw})
			if err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(map[string]any{"topic": "telemetry", "key": "x", "value": []byte("x")})
			response, err := bad.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "publish", Payload: request, TimeoutMS: 100})
			if err == nil || len(response.Payload) > 0 {
				t.Fatal("untrusted server produced a receipt")
			}
		})
	}
}
func TestKafkaCompressedBatchCannotExceedExpansionBudget(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "telemetry"), kfake.BrokerConfigs(map[string]string{"message.max.bytes": "4194304"}))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ProducerBatchCompression(kgo.GzipCompression()), kgo.ProducerBatchMaxBytes(3*1048576), kgo.BrokerMaxWriteBytes(4*1048576))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = producer.ProduceSync(ctx, &kgo.Record{Topic: "telemetry", Key: []byte("large"), Value: bytes.Repeat([]byte("A"), 2*1048576)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"brokers": cluster.ListenAddrs(), "topics": []string{"telemetry"}, "tls": false, "max_message_bytes": 1048576})
	c := NewConnector().(*connector)
	defer c.Stop()
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "bounded", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	id := open(t, c, h, "large-node", "iot-rule.large")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 1000})
		if len(response.Messages) > 0 {
			t.Fatal("expanded oversize batch delivered")
		}
		if err != nil {
			if !strings.Contains(err.Error(), "KAFKA_DECOMPRESS_LIMIT") {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("compressed batch rejection not observed")
}
