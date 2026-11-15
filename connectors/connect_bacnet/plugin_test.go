package main

import (
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type udpFixture struct {
	conn          *net.UDPConn
	done          chan struct{}
	reads, writes atomic.Int64
	mu            sync.Mutex
	ports         map[int]bool
	slow          bool
}

func fixture(t *testing.T) *udpFixture {
	t.Helper()
	c, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if e != nil {
		t.Fatal(e)
	}
	f := &udpFixture{conn: c, done: make(chan struct{}), ports: map[int]bool{}}
	go func() {
		defer close(f.done)
		b := make([]byte, 4096)
		for {
			n, a, e := c.ReadFromUDP(b)
			if e != nil {
				return
			}
			raw := append([]byte(nil), b[:n]...)
			if n < 17 {
				continue
			}
			f.mu.Lock()
			f.ports[a.Port] = true
			slow := f.slow
			f.mu.Unlock()
			if slow {
				continue
			}
			p := point{Name: "x", ObjectType: binary.BigEndian.Uint32(raw[11:15]) >> 22, Instance: binary.BigEndian.Uint32(raw[11:15]) & 0x3fffff, Property: uint32(raw[16]), ValueType: "Float32"}
			if raw[9] == 15 {
				f.writes.Add(1)
				c.WriteToUDP([]byte{0x81, 0x0a, 0, 9, 1, 0, 0x20, raw[8], 15}, a)
				continue
			}
			f.reads.Add(1)
			if p.Instance == 99 {
				c.WriteToUDP([]byte{0x81, 0x0a, 0, 13, 1, 0, 0x50, raw[8], 12, 0x91, 2, 0x91, 32}, a)
				continue
			}
			value := fromHex("4441b60000")
			if p.ObjectType == 8 && p.Property == 75 {
				value = make([]byte, 5)
				value[0] = 0xc4
				binary.BigEndian.PutUint32(value[1:], objectID(p))
			}
			c.WriteToUDP(readReply(raw[8], p, value), a)
		}
	}()
	t.Cleanup(func() { c.Close(); <-f.done })
	return f
}
func create(t *testing.T, f *udpFixture, timeout int) (*connector, string) {
	t.Helper()
	p := point{Name: "temperature", ObjectType: 2, Instance: 1, Property: 85, ValueType: "Float32"}
	missing := p
	missing.Name = "missing"
	missing.Instance = 99
	raw, _ := json.Marshal(config{Host: "127.0.0.1", Port: f.conn.LocalAddr().(*net.UDPAddr).Port, TimeoutMS: timeout, ReadAllow: []point{p, missing}, WriteAllow: []point{p}, AllowWrite: true})
	c := NewConnector().(*connector)
	h, e := c.CreateResource(sdk.ConnectorResource{ID: "device", Connection: raw})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h
}
func batchReq(write bool, points []point, expires int64) sdk.ConnectorRequest {
	kind := sdk.ConnectorRequestBatchRead
	if write {
		kind = sdk.ConnectorRequestBatchWrite
	}
	b, _ := json.Marshal(batch{Points: points, ExpiresAt: expires})
	return sdk.ConnectorRequest{Kind: kind, Target: "points", Payload: b}
}
func TestBatchOneSocketQualityAndControlledWrite(t *testing.T) {
	f := fixture(t)
	c, h := create(t, f, 500)
	p := point{Name: "temperature", ObjectType: 2, Instance: 1, Property: 85, ValueType: "Float32"}
	missing := p
	missing.Name = "missing"
	missing.Instance = 99
	response, e := c.Execute(h, batchReq(false, []point{p, missing}, 0))
	if e != nil {
		t.Fatal(e)
	}
	var got struct {
		Values  map[string]any
		Quality map[string]string
	}
	if json.Unmarshal(response.Payload, &got) != nil || got.Values[p.Name] != 22.75 || got.Values[missing.Name] != nil || got.Quality[missing.Name] != "bad:device_2_32" {
		t.Fatalf("%s", response.Payload)
	}
	f.mu.Lock()
	n := len(f.ports)
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("one batch used %d sockets", n)
	}
	p.Value = 23.5
	if _, e = c.Execute(h, batchReq(true, []point{p}, time.Now().Add(time.Second).UnixMilli())); e != nil || f.writes.Load() != 1 {
		t.Fatal(e)
	}
	if _, e = c.Execute(h, batchReq(true, []point{p}, time.Now().Add(-time.Second).UnixMilli())); e == nil {
		t.Fatal("expired command accepted")
	}
	p.Priority = 1
	if _, e = c.Execute(h, batchReq(true, []point{p}, time.Now().Add(time.Second).UnixMilli())); e == nil {
		t.Fatal("unauthorized priority")
	}
	p.Priority = 0
	bad := p
	bad.Name = "second"
	bad.Instance = 2
	if _, e = c.Execute(h, batchReq(true, []point{p, bad}, time.Now().Add(time.Second).UnixMilli())); e == nil || f.writes.Load() != 1 {
		t.Fatal("partial network write before full preflight")
	}
}
func TestDeadlineBreakerAndStop(t *testing.T) {
	f := fixture(t)
	f.mu.Lock()
	f.slow = true
	f.mu.Unlock()
	c, h := create(t, f, 50)
	p := point{Name: "temperature", ObjectType: 2, Instance: 1, Property: 85, ValueType: "Float32"}
	for i := 0; i < 3; i++ {
		if _, e := c.Execute(h, batchReq(false, []point{p}, 0)); e == nil {
			t.Fatal("timeout accepted")
		}
	}
	r, _ := c.get(h)
	r.mu.Lock()
	opens := r.opens
	r.mu.Unlock()
	start := time.Now()
	_, e := c.Execute(h, batchReq(false, []point{p}, 0))
	if e == nil || time.Since(start) > 40*time.Millisecond {
		t.Fatal("breaker did not fail fast", e)
	}
	r.mu.Lock()
	if r.opens != opens {
		t.Fatal("breaker dialed")
	}
	r.retryAfter = time.Time{}
	r.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, e := c.Execute(h, batchReq(false, []point{p}, 0)); done <- e }()
	time.Sleep(5 * time.Millisecond)
	if e := c.DestroyResource(h); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not interrupt socket")
	}
}
func TestFixedIPv4AndAllowlistConfig(t *testing.T) {
	for _, raw := range []string{`{"host":"example.invalid","read_allowlist":[]}`, `{"host":"255.255.255.255"}`, `{"host":"224.0.0.1"}`, `{"host":"0.0.0.0"}`, `{"host":"127.0.0.1","unexpected":true}`, `{"host":"127.0.0.1","read_allowlist":[{"name":"x","object_type":1024,"instance":1,"property":85,"valueType":"Float32"}]}`} {
		c := NewConnector()
		if _, e := c.CreateResource(sdk.ConnectorResource{ID: "test", Connection: []byte(raw)}); e == nil {
			t.Fatal("invalid config accepted", raw)
		}
		c.Stop()
	}
}
func TestIndependentBACnetStack(t *testing.T) {
	binaryPath := os.Getenv("TEST_BACNET_SERVER")
	if binaryPath == "" {
		t.Skip("set TEST_BACNET_SERVER to independently built bacnet-stack 1.7.0 bacserv")
	}
	if os.Getenv("TEST_BACNET_ISOLATED") != "1" {
		t.Fatal("independent service requires isolated network namespace wrapper")
	}
	port := 47808
	cmd := exec.Command(binaryPath, "1234", "Local BACnet interoperability fixture")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "BACNET_DATALINK=bip", "BACNET_IFACE=lo", "BACNET_IP_PORT="+strconv.Itoa(port))
	logFile, e := os.Create(filepath.Join(cmd.Dir, "server.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer logFile.Close()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	identity := uint32(1234)
	p := point{Name: "value", ObjectType: 2, Instance: 1, Property: 85, ValueType: "Float32"}
	name := point{Name: "name", ObjectType: 8, Instance: 1234, Property: 77, ValueType: "String"}
	missing := p
	missing.Name = "not_found"
	missing.Instance = 4194000
	c := NewConnector().(*connector)
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: "127.0.0.1", Port: port, TimeoutMS: 1000, ExpectedDevice: &identity, ReadAllow: []point{p, name, missing}, WriteAllow: []point{p}, AllowWrite: true})
	h, e := c.CreateResource(sdk.ConnectorResource{ID: "native", Connection: raw})
	if e != nil {
		t.Fatal(e)
	}
	var response sdk.ConnectorResponse
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, e = c.Execute(h, batchReq(false, []point{p, name, missing}, 0))
		if e == nil {
			break
		}
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(logFile.Name())
			t.Fatalf("native server read failed %v\n%s", e, data)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var got struct {
		Values  map[string]any
		Quality map[string]string
	}
	json.Unmarshal(response.Payload, &got)
	if got.Quality[p.Name] != "good" || got.Quality[name.Name] != "good" || got.Quality[missing.Name] == "good" {
		t.Fatalf("native quality %s", response.Payload)
	}
	p.Value = 42.75
	if _, e = c.Execute(h, batchReq(true, []point{p}, time.Now().Add(time.Second).UnixMilli())); e != nil {
		t.Fatal(e)
	}
	response, e = c.Execute(h, batchReq(false, []point{p}, 0))
	json.Unmarshal(response.Payload, &got)
	if e != nil || got.Values[p.Name] != 42.75 {
		t.Fatalf("native write/read %s %v", response.Payload, e)
	}
	t.Logf("bacnet-stack 1.7.0 device identity, name, missing-property quality and scalar write/read passed: %s", response.Payload)
}
