package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sessionPLC struct {
	listener    net.Listener
	connections sync.Map
	wg          sync.WaitGroup
	opens       atomic.Int32
	reads       atomic.Int32
	reject      atomic.Bool
}

func newSessionPLC(t *testing.T) *sessionPLC {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &sessionPLC{listener: l}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			p.opens.Add(1)
			if p.reject.Load() {
				conn.Close()
				continue
			}
			p.connections.Store(conn, true)
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer conn.Close()
				defer p.connections.Delete(conn)
				for {
					header := make([]byte, 9)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					size := int(binary.LittleEndian.Uint16(header[7:]))
					if size < 12 || size > 8192 {
						return
					}
					body := make([]byte, size)
					if _, err := io.ReadFull(conn, body); err != nil {
						return
					}
					p.reads.Add(1)
					count := int(binary.LittleEndian.Uint16(body[10:]))
					response := []byte{0, 0}
					if binary.LittleEndian.Uint16(body[2:]) == 0x0401 {
						response = append(response, make([]byte, count*2)...)
					}
					header[0] = 0xd0
					binary.LittleEndian.PutUint16(header[7:], uint16(len(response)))
					if _, err := conn.Write(append(header, response...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		p.connections.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
		p.wg.Wait()
	})
	return p
}
func sessionConnector(t *testing.T, p *sessionPLC, extra map[string]any) *connector {
	t.Helper()
	host, portText, _ := net.SplitHostPort(p.listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	cfg := map[string]any{"host": host, "port": port, "timeout_ms": 1000}
	for k, v := range extra {
		cfg[k] = v
	}
	raw, _ := json.Marshal(cfg)
	c := NewConnector().(*connector)
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c
}
func demandRead(c *connector, token string, interval int) error {
	raw, _ := json.Marshal(map[string]any{"points": []point{{Name: "counter", Area: "D", Address: 1, ValueType: "Uint16"}}, "next_demand_in_ms": interval})
	_, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, ResourceToken: token, Payload: raw})
	return err
}
func demandStats(t *testing.T, c *connector) sessionStats {
	t.Helper()
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "session_stats"})
	if err != nil {
		t.Fatal(err)
	}
	var stats sessionStats
	if err = json.Unmarshal(response.Payload, &stats); err != nil {
		t.Fatal(err)
	}
	return stats
}
func TestFastDemandReusesAndReleaseOnlyOwnsItsToken(t *testing.T) {
	p := newSessionPLC(t)
	c := sessionConnector(t, p, nil)
	for i := 0; i < 6; i++ {
		if err := demandRead(c, "fast", 100); err != nil {
			t.Fatal(err)
		}
	}
	stats := demandStats(t, c)
	if stats.Opens != 1 || stats.Reuses != 5 || !stats.Active || len(c.sessions) != 1 {
		t.Fatal(stats)
	}
	if err := demandRead(c, "slow", 20000); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "release_demand", ResourceToken: "fast"}); err != nil {
		t.Fatal(err)
	}
	stats = demandStats(t, c)
	if stats.Active || stats.Closes != 1 || len(c.sessions) != 0 {
		t.Fatal("held connection for distant remaining demand", stats)
	}
	c.Stop()
	if len(c.sessions) != 0 {
		t.Fatal("session slot leaked")
	}
}
func TestDistantAndStrictDemandClosesEachBatch(t *testing.T) {
	for _, test := range []struct {
		hint   int
		strict bool
	}{{5000, false}, {20000, false}, {100, true}} {
		t.Run(fmt.Sprintf("%d/%v", test.hint, test.strict), func(t *testing.T) {
			p := newSessionPLC(t)
			cfg := map[string]any{}
			if test.strict {
				cfg["session_reuse_horizon_ms"] = 0
			}
			c := sessionConnector(t, p, cfg)
			for i := 0; i < 3; i++ {
				if err := demandRead(c, "source", test.hint); err != nil {
					t.Fatal(err)
				}
			}
			stats := demandStats(t, c)
			if stats.Opens != 3 || stats.Closes != 3 || stats.Active {
				t.Fatal(stats)
			}
		})
	}
}
func TestAbandonedDemandClosesWithoutBackgroundReconnect(t *testing.T) {
	p := newSessionPLC(t)
	c := sessionConnector(t, p, map[string]any{"session_reuse_horizon_ms": 100})
	if err := demandRead(c, "source", 50); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for demandStats(t, c).Active && time.Now().Before(until) {
		time.Sleep(10 * time.Millisecond)
	}
	stats := demandStats(t, c)
	if stats.Active || stats.Opens != 1 || stats.Closes != 1 {
		t.Fatal(stats)
	}
	time.Sleep(100 * time.Millisecond)
	if p.opens.Load() != 1 {
		t.Fatal("background redial without demand")
	}
}
func TestMaximumAgeRotatesAtBatchBoundary(t *testing.T) {
	p := newSessionPLC(t)
	c := sessionConnector(t, p, map[string]any{"max_session_age_ms": 20})
	if err := demandRead(c, "source", 100); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := demandRead(c, "source", 100); err != nil {
		t.Fatal(err)
	}
	stats := demandStats(t, c)
	if stats.Opens != 2 || stats.Closes != 1 || !stats.Active {
		t.Fatal(stats)
	}
}
func TestCircuitSuppressesHighFrequencyDialAndAllowsOneRecovery(t *testing.T) {
	p := newSessionPLC(t)
	p.reject.Store(true)
	c := sessionConnector(t, p, nil)
	for i := 0; i < 3; i++ {
		if err := demandRead(c, "source", 100); err == nil {
			t.Fatal("broken device read succeeded")
		}
	}
	before := p.opens.Load()
	for i := 0; i < 20; i++ {
		if err := demandRead(c, "source", 100); err == nil {
			t.Fatal("open circuit accepted read")
		}
	}
	if p.opens.Load() != before || before != 3 {
		t.Fatal("dial storm", before, p.opens.Load())
	}
	r := c.resources["r"]
	r.gate <- struct{}{}
	r.retryAfter = time.Now().Add(-time.Second)
	<-r.gate
	p.reject.Store(false)
	if err := demandRead(c, "source", 100); err != nil {
		t.Fatal(err)
	}
	if stats := demandStats(t, c); stats.Failures != 0 || !stats.Active {
		t.Fatal(stats)
	}
}
func TestRetainedSessionYieldsUnderPackagePressure(t *testing.T) {
	p := newSessionPLC(t)
	c := sessionConnector(t, p, nil)
	c.sessions = make(chan struct{}, 1)
	if err := demandRead(c, "first", 100); err != nil {
		t.Fatal(err)
	}
	r := c.resources["r"]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := make(chan struct{})
	go func() {
		c.waiters.Add(1)
		select {
		case c.sessions <- struct{}{}:
			c.waiters.Add(-1)
			close(got)
			<-c.sessions
		case <-ctx.Done():
			c.waiters.Add(-1)
		}
	}()
	until := time.Now().Add(time.Second)
	for c.waiters.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if err := demandRead(c, "first", 100); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("retained session starved another resource")
	}
	r.gate <- struct{}{}
	active := r.conn != nil
	<-r.gate
	if active {
		t.Fatal("busy package did not retire retained session")
	}
}

// Real-duration acceptance is opt-in; regular unit tests do not pretend their
// compressed hint checks represent a full minute of 100 ms scheduling.
func TestMCFrequencySixtySeconds(t *testing.T) {
	if os.Getenv("TEST_MC_FREQUENCY_60S") != "1" {
		t.Skip("set TEST_MC_FREQUENCY_60S=1 for real-duration loopback acceptance")
	}
	for _, interval := range []int{100, 500, 20000} {
		t.Run(strconv.Itoa(interval), func(t *testing.T) {
			t.Parallel()
			p := newSessionPLC(t)
			c := sessionConnector(t, p, nil)
			duration := 60 * time.Second
			period := time.Duration(interval) * time.Millisecond
			start := time.Now()
			next := start
			latencies := []float64{}
			dropped := 0
			scans := 0
			for time.Since(start) < duration {
				time.Sleep(max(time.Until(next), 0))
				before := time.Now()
				if time.Since(start) >= duration {
					break
				}
				if err := demandRead(c, "frequency", interval); err != nil {
					t.Fatal(err)
				}
				latencies = append(latencies, float64(time.Since(before).Microseconds())/1000)
				scans++
				next = next.Add(period)
				now := time.Now()
				if !next.After(now) {
					skip := int(now.Sub(next)/period) + 1
					dropped += skip
					next = next.Add(time.Duration(skip) * period)
				}
			}
			stats := demandStats(t, c)
			if interval <= 500 && stats.Opens != 1 {
				t.Fatal("unexpected high-frequency reconnect", stats)
			}
			if interval == 20000 && (stats.Opens != uint64(scans) || stats.Closes != uint64(scans)) {
				t.Fatal("low-frequency connection not per-batch", stats)
			}
			c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "release_demand", ResourceToken: "frequency"})
			if demandStats(t, c).Active {
				t.Fatal("stop did not release connection")
			}
			sort.Float64s(latencies)
			percentile := func(percent float64) float64 {
				if len(latencies) == 0 {
					return 0
				}
				return latencies[min(int(float64(len(latencies)-1)*percent), len(latencies)-1)]
			}
			t.Logf("interval_ms=%d elapsed_s=%.2f scans=%d dropped=%d opens=%d reuses=%d closes=%d duration_ms_p50=%.3f p95=%.3f p99=%.3f", interval, time.Since(start).Seconds(), scans, dropped, stats.Opens, stats.Reuses, stats.Closes, percentile(.5), percentile(.95), percentile(.99))
		})
	}
}
