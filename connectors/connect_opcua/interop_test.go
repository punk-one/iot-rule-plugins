package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func testCertificate(t *testing.T, dir, name string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse("urn:iot-rule:" + name)
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageDataEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, BasicConstraintsValid: true, IsCA: true, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := filepath.Join(dir, name+".pem")
	keyPath := filepath.Join(dir, name+".key")
	if err = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600); err != nil {
		t.Fatal(err)
	}
	return cert, keyPath
}
func TestLocalOPCUASecureReadWriteAndDataSubscription(t *testing.T) {
	if testing.Short() {
		t.Skip("local protocol server integration")
	}
	dir := t.TempDir()
	python := os.Getenv("TEST_OPCUA_PYTHON")
	if python == "" {
		t.Skip("set TEST_OPCUA_PYTHON to a Python interpreter with asyncua==1.1.8 for real-stack integration")
	}

	serverCert, serverKey := testCertificate(t, dir, "server")
	clientCert, clientKey := testCertificate(t, dir, "client")
	wrongCert, _ := testCertificate(t, dir, "wrong-server")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	ready := filepath.Join(dir, "ready.json")
	log, err := os.Create(filepath.Join(dir, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(python, "./testdata/asyncua_server.py", "--port", strconv.Itoa(port), "--cert", serverCert, "--key", serverKey, "--ready", ready)
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	until := time.Now().Add(10 * time.Second)
	for {
		if _, err = os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(until) {
			out, _ := os.ReadFile(filepath.Join(dir, "server.log"))
			t.Fatalf("server startup timeout: %s", out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c := NewConnector().(*connector)
	defer c.Stop()
	cfg := config{Endpoint: fmt.Sprintf("opc.tcp://127.0.0.1:%d", port), SecurityMode: "SignAndEncrypt", ClientCertificateFile: clientCert, PrivateKeyFile: clientKey, ServerCertificateFile: serverCert, TimeoutMS: 5000, ReuseMS: 2000}
	raw, _ := json.Marshal(cfg)
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	points := json.RawMessage(`{"points":[{"name":"counter","node_id":"s=counter","namespace_uri":"urn:iot-rule:test"}]}`)
	result, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: points})
	if err != nil {
		out, _ := os.ReadFile(filepath.Join(dir, "server.log"))
		t.Fatalf("secure read %v server log: %s", err, out)
	}
	var data struct {
		Values map[string]any `json:"values"`
	}
	json.Unmarshal(result.Payload, &data)
	if data.Values["counter"] != "9007199254740993" {
		t.Fatal("secure read changed int64", string(result.Payload))
	}
	req := subRequest("actual", 16)
	req.Payload = json.RawMessage(`{"points":[{"name":"temperature","node_id":"s=temperature","namespace_uri":"urn:iot-rule:test"}],"interval_ms":100}`)
	if _, err = c.OpenStream("r", req); err != nil {
		t.Fatal("secure subscription", err)
	}
	initial, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "actual", MaxMessages: 1, WaitTimeoutMS: 5000})
	if err != nil || len(initial.Messages) == 0 {
		t.Fatal("initial notification", initial, err)
	}
	c.AckStream("r", sdk.StreamAckRequest{StreamID: "actual", DeliveryID: initial.Messages[0].DeliveryID, Credits: 1})
	_, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"points":[{"name":"temperature","node_id":"s=temperature","namespace_uri":"urn:iot-rule:test","valueType":"Float64","value":42.5}]}`)})
	if err != nil {
		t.Fatal("secure write", err)
	}
	changed := false
	until = time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		response, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "actual", MaxMessages: 1, WaitTimeoutMS: 1000})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range response.Messages {
			var v struct {
				Values map[string]any `json:"values"`
			}
			json.Unmarshal(m.RawPayload, &v)
			if v.Values["temperature"] == 42.5 {
				changed = true
			}
			c.AckStream("r", sdk.StreamAckRequest{StreamID: "actual", DeliveryID: m.DeliveryID, Credits: 1})
		}
		if changed {
			break
		}
	}
	if !changed {
		t.Fatal("write did not reach data subscription")
	}
	if err = c.CloseStream("r", sdk.StreamCloseRequest{StreamID: "actual"}); err != nil {
		t.Fatal(err)
	}

	eventReq := sdk.StreamOpenRequest{SessionKey: "events", Target: "events", InitialCredits: 1, MaxBufferedMessages: 16, OverflowPolicy: "fail_session", Payload: json.RawMessage(`{"event_node_id":"i=2253","interval_ms":100}`)}
	if _, err = c.OpenStream("r", eventReq); err != nil {
		t.Fatal("event filter interoperability", err)
	}
	eventResponse, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "events", MaxMessages: 1, WaitTimeoutMS: 5000})
	if err != nil || len(eventResponse.Messages) != 1 {
		t.Fatal("event delivery", eventResponse, err)
	}
	var eventData struct {
		Events []map[string]any `json:"events"`
	}
	if err = json.Unmarshal(eventResponse.Messages[0].RawPayload, &eventData); err != nil || len(eventData.Events) == 0 || eventData.Events[0]["EventId"] == nil {
		t.Fatal("event fields", string(eventResponse.Messages[0].RawPayload), err)
	}
	c.AckStream("r", sdk.StreamAckRequest{StreamID: "events", DeliveryID: eventResponse.Messages[0].DeliveryID, Credits: 1})
	// A real server outage must surface through the native publish channel.
	cmd.Process.Kill()
	cmd.Wait()
	outageVisible := false
	until = time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		_, err = c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "events", MaxMessages: 1, WaitTimeoutMS: 1000})
		if err != nil {
			outageVisible = true
			break
		}
	}
	if !outageVisible {
		t.Fatal("server outage did not invalidate live subscription")
	}
	c.CloseStream("r", sdk.StreamCloseRequest{StreamID: "events"})
	os.Remove(ready)
	cmd = exec.Command(python, "./testdata/asyncua_server.py", "--port", strconv.Itoa(port), "--cert", serverCert, "--key", serverKey, "--ready", ready, "--extra-namespace")
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	until = time.Now().Add(10 * time.Second)
	for {
		if _, err = os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("server restart timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Namespace indexes changed after restart; URI resolution must use this generation.
	if _, err = c.OpenStream("r", req); err != nil {
		t.Fatal("subscription reconstruction", err)
	}
	recovered, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "actual", MaxMessages: 1, WaitTimeoutMS: 5000})
	if err != nil || len(recovered.Messages) != 1 {
		t.Fatal("reconstructed notification missing", recovered, err)
	}
	c.CloseStream("r", sdk.StreamCloseRequest{StreamID: "actual"})
	result, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: points})
	if err != nil {
		t.Fatal("namespace after server restart", err)
	}
	json.Unmarshal(result.Payload, &data)
	if data.Values["counter"] != "9007199254740993" {
		t.Fatal("stale namespace index used after restart", string(result.Payload))
	}
	cfg.ServerCertificateFile = wrongCert
	raw, _ = json.Marshal(cfg)
	c.CreateResource(sdk.ConnectorResource{ID: "wrong-pin", Connection: raw})
	if _, err = c.Execute("wrong-pin", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: points}); err == nil {
		t.Fatal("wrong pinned server certificate accepted")
	}
}
