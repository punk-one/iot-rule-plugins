package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

func modbusPTY(t *testing.T) (*os.File, string) {
	t.Helper()
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	id, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	path := fmt.Sprintf("/dev/pts/%d", id)
	holder, err := os.OpenFile(path, os.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	master := os.NewFile(uintptr(fd), "modbus-test-master")
	t.Cleanup(func() { master.Close(); holder.Close() })
	return master, path
}
func TestSerialBatchUsesOneSessionAcrossSlavesAndReleasesExclusive(t *testing.T) {
	master, path := modbusPTY(t)
	master.SetDeadline(time.Now().Add(3 * time.Second))
	c := NewConnector().(*connector)
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"mode": "rtu", "device": path, "baud": 9600, "parity": "none", "data_bits": 8, "timeout_ms": 1000})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	t.Cleanup(func() { master.Close(); worker.Wait() })
	go func() {
		defer worker.Done()
		for i := 0; i < 2; i++ {
			request := make([]byte, 8)
			if _, err := io.ReadFull(master, request); err != nil {
				done <- err
				return
			}
			if crc16(request) != 0 {
				done <- fmt.Errorf("invalid master request CRC")
				return
			}
			count := int(binary.BigEndian.Uint16(request[4:6]))
			pdu := []byte{3, byte(count * 2)}
			for j := 0; j < count; j++ {
				pdu = append(pdu, 0, 42)
			}
			if _, err := master.Write(encodeSerial(request[0], pdu, false)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, ResourceToken: "source", Payload: json.RawMessage(`{"next_demand_in_ms":100,"points":[{"name":"a","area":"holding","address":0,"valueType":"Uint16","unit_id":1},{"name":"b","area":"holding","address":1,"valueType":"Uint16","unit_id":1},{"name":"c","area":"holding","address":0,"valueType":"Uint16","unit_id":2}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var result struct {
		Values   map[string]int `json:"values"`
		Segments int            `json:"segments"`
	}
	json.Unmarshal(response.Payload, &result)
	if result.Segments != 2 || result.Values["a"] != 42 || result.Values["c"] != 42 {
		t.Fatal(string(response.Payload))
	}
	if stats := demandStats(t, c); stats.Opens != 1 || !stats.Active {
		t.Fatal(stats)
	}
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "duplicate", Connection: raw}); err == nil {
		t.Fatal("two resources opened one physical serial bus")
	}
	if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "release_demand", ResourceToken: "source"}); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal("exclusive TTY lock leaked after release", err)
	}
	unix.Close(fd)
	if err = c.DestroyResource("r"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "replacement", Connection: raw}); err != nil {
		t.Fatal("physical resource reservation leaked", err)
	}
}
func TestSerialIdentityChangeRejectedBeforeExclusive(t *testing.T) {
	_, path := modbusPTY(t)
	cfg := connectionConfig{Mode: "rtu", Device: path, Baud: 9600, DataBits: 8, Parity: "none", StopBits: 1}
	_, identity, err := validateSerial(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.serialIdentity = identity + 1
	if conn, err := dialSerial(context.Background(), cfg); err == nil {
		conn.Close()
		t.Fatal("changed physical identity accepted")
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal("failed identity check leaked exclusive state", err)
	}
	unix.Close(fd)
}
