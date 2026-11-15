package main

import (
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/sys/unix"
	"os"
	"testing"
	"time"
)

func TestPTYSharedContinuousFrames(t *testing.T) {
	fd, e := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	master := os.NewFile(uintptr(fd), "pty")
	defer master.Close()
	if e = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); e != nil {
		t.Fatal(e)
	}
	number, e := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if e != nil {
		t.Fatal(e)
	}
	path := fmt.Sprintf("/dev/pts/%d", number)
	keep, e := os.OpenFile(path, os.O_RDWR|unix.O_NOCTTY, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer keep.Close()
	f := framing{Mode: "delimiter", Delimiter: []byte("!")}
	raw, _ := json.Marshal(map[string]any{"device": path, "mode": "stream", "stream_frame": f, "timeout_ms": 500})
	c := NewConnector().(*connector)
	defer c.Stop()
	if _, e = c.CreateResource(sdk.ConnectorResource{ID: "stream", Connection: raw}); e != nil {
		t.Fatal(e)
	}
	exerciseRaw(t, c, f, func(data []byte) {
		master.SetWriteDeadline(time.Now().Add(time.Second))
		if _, e := master.Write(data); e != nil {
			t.Fatal(e)
		}
	})
}
