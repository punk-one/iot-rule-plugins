package main

import (
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"testing"
	"time"
)

func TestSerialPTYExclusiveAndDeadline(t *testing.T) {
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	master := os.NewFile(uintptr(fd), "pty")
	defer master.Close()
	if err = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	number, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	device := fmt.Sprintf("/dev/pts/%d", number)
	keep, err := os.OpenFile(device, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer keep.Close()
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"device": device, "timeout_ms": 100})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "a", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "b", Connection: raw}); err == nil {
		t.Fatal("duplicate physical serial ownership")
	}
	done := make(chan error, 1)
	go func() {
		master.SetDeadline(time.Now().Add(time.Second))
		b := make([]byte, 1)
		_, err := io.ReadFull(master, b)
		if err == nil {
			_, err = master.Write([]byte("ok!"))
		}
		done <- err
	}()
	request, _ := json.Marshal(request{Data: []byte("?"), Frame: framing{Mode: "delimiter", Delimiter: []byte("!")}})
	if _, err = c.Execute("a", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: request}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err = c.Execute("a", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: request}); err == nil {
		t.Fatal("missing response accepted")
	}
	if time.Since(started) > time.Second {
		t.Fatal("serial deadline ignored")
	}
	if err = c.DestroyResource("a"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "b", Connection: raw}); err != nil {
		t.Fatal("port reservation leaked", err)
	}
}
