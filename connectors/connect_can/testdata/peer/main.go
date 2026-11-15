// Isolated vcan test helper, never a release artifact.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	name := os.Getenv("TEST_CAN_IFACE")
	if os.Getenv("TEST_CAN_ISOLATED") != "1" || !strings.HasPrefix(name, "testvcan") {
		panic("helper only supports explicit isolated testvcan interfaces")
	}
	iface, err := net.InterfaceByName(name)
	if err != nil {
		panic(err)
	}
	fd, err := unix.Socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		panic(err)
	}
	defer unix.Close(fd)
	if unix.SetsockoptInt(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_FD_FRAMES, 1) != nil {
		panic("CAN FD unavailable")
	}
	if unix.Bind(fd, &unix.SockaddrCAN{Ifindex: iface.Index}) != nil {
		panic("vcan bind failed")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var mu sync.Mutex
	frames := []map[string]any{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			_, err := unix.Poll(fds, 50)
			if err != nil {
				return
			}
			if fds[0].Revents&unix.POLLIN == 0 {
				continue
			}
			raw := make([]byte, 72)
			n, err := unix.Read(fd, raw)
			if err != nil {
				continue
			}
			if n != 16 && n != 72 {
				continue
			}
			length := int(raw[4])
			if length > n-8 {
				continue
			}
			id := binary.NativeEndian.Uint32(raw)
			mu.Lock()
			if len(frames) < 256 {
				frames = append(frames, map[string]any{"id": "0x" + strconv.FormatUint(uint64(id&0x1fffffff), 16), "extended": id&0x80000000 != 0, "fd": n == 72, "raw_base64": base64.StdEncoding.EncodeToString(raw[8 : 8+length])})
			}
			mu.Unlock()
		}
	}()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/inject", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var frame struct {
			ID       uint32 `json:"id"`
			Extended bool   `json:"extended"`
			FD       bool   `json:"fd"`
			Data     []byte `json:"data"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&frame) != nil || frame.ID > 0x1fffffff || !frame.Extended && frame.ID > 0x7ff || len(frame.Data) > 64 || !frame.FD && len(frame.Data) > 8 {
			w.WriteHeader(400)
			return
		}
		length := 16
		if frame.FD {
			length = 72
		}
		raw := make([]byte, length)
		id := frame.ID
		if frame.Extended {
			id |= 0x80000000
		}
		binary.NativeEndian.PutUint32(raw, id)
		raw[4] = byte(len(frame.Data))
		if frame.FD {
			raw[5] = 4
		}
		copy(raw[8:], frame.Data)
		if n, err := unix.Write(fd, raw); err != nil || n != len(raw) {
			w.WriteHeader(502)
			return
		}
		fmt.Fprint(w, `{"injected":true}`)
	})
	mux.HandleFunc("/frames", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(w).Encode(frames)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second}
	go server.Serve(listener)
	json.NewEncoder(os.Stdout).Encode(map[string]any{"control_url": "http://" + listener.Addr().String()})
	<-ctx.Done()
	server.Close()
	wg.Wait()
}
