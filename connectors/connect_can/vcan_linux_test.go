//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestRealVCanKernelFramesAndLifecycle(t *testing.T) {
	name := os.Getenv("TEST_CAN_IFACE")
	if name == "" {
		t.Skip("run test-can-vcan.sh in its private network namespace")
	}
	cfg := config{Interface: name, FD: true, ReadIDs: []identifier{{ID: "0x123"}, {ID: "0x123", Extended: true}}}
	peer, err := openDevice(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	c := NewConnector().(*connector)
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"interface": name, "can_fd": true, "read_ids": cfg.ReadIDs, "write_ids": []identifier{{ID: "0x123"}}, "allow_write": true})
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "vcan", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"ids": cfg.ReadIDs, "include_remote": true})
	opened, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: "native", Target: "frames", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 64, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, spec := range []frameSpec{{ID: "0x123", Data: []byte{1, 2, 3}}, {ID: "0x123", Extended: true, Data: []byte{4}}, {ID: "0x123", Remote: true, RTRLength: 8}, {ID: "0x123", FD: true, BitrateSwitch: true, Data: bytes.Repeat([]byte{7}, 64)}} {
		frame, err := fromSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		if err = peer.Write(ctx, frame); err != nil {
			t.Fatal(err)
		}
		m := receive(t, c, h, opened.StreamID)
		if !bytes.Equal(m.RawPayload, frame.data) || m.Metadata["extended"] != strconv.FormatBool(frame.extended) || m.Metadata["fd"] != strconv.FormatBool(frame.fd) || m.Metadata["remote"] != strconv.FormatBool(frame.remote) {
			t.Fatal("kernel frame changed", m, spec)
		}
		ack(t, c, h, m)
	}
	c.CloseStream(h, sdk.StreamCloseRequest{StreamID: opened.StreamID})
	request, _ := json.Marshal(map[string]any{"frame": frameSpec{ID: "0x123", Data: []byte{8, 9}}, "expires_at": time.Now().Add(time.Second).UnixMilli()})
	reply, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", Payload: request})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(reply.Payload, []byte(`"scope":"local_socket"`)) {
		t.Fatal(string(reply.Payload))
	}
	out, err := peer.Read(ctx)
	if err != nil || !bytes.Equal(out.data, []byte{8, 9}) {
		t.Fatal(out, err)
	}
	r, _ := c.get(h)
	r.mu.Lock()
	active := r.session != nil
	count := r.opened
	r.mu.Unlock()
	if active || count != 2 {
		t.Fatal("demand sessions not released", active, count)
	}
	t.Log("classic, extended, RTR, FD, shared RX/TX and no-demand close passed using Linux vcan")
}
