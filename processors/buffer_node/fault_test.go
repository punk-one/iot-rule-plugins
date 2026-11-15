package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
)

func faultBuffer(path string) (*buffer, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"pending", "usage"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	b := &buffer{db: db, ctx: testContext{rule: "fault-rule", node: "fault-buffer"}}
	b.cfg.Storage.MaxRecords = 100
	b.cfg.Storage.MaxBytes = 8 << 20
	b.cfg.RetryMS = 2000
	b.cfg.Rate = 100
	b.cfg.AckNode = "sink"
	return b, nil
}
func TestBufferCrashDurability(t *testing.T) {
	if os.Getenv("IOT_TEST_BUFFER_CRASH_CHILD") == "1" {
		b, err := faultBuffer(os.Getenv("IOT_TEST_BUFFER_CRASH_PATH"))
		if err != nil {
			t.Fatal(err)
		}
		event := sdk.NewEvent(map[string]any{"value": 7}, nil)
		event.ID = "synthetic-crash-event"
		if err = b.OnEvent(event); err != nil {
			t.Fatal(err)
		}
		fmt.Println("committed")
		for {
			time.Sleep(time.Hour)
		}
	}
	path := filepath.Join(t.TempDir(), "queue.db")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestBufferCrashDurability$")
	child.Env = append(os.Environ(), "IOT_TEST_BUFFER_CRASH_CHILD=1", "IOT_TEST_BUFFER_CRASH_PATH="+path)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "committed" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child failed before commit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child commit timeout")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	b, err := faultBuffer(path)
	if err != nil {
		t.Fatal(err)
	}
	emit := testEmitter{events: make(chan sdk.Event, 2)}
	b.ctx = testContext{rule: "fault-rule", node: "fault-buffer", emitter: emit}
	b.stop = make(chan struct{})
	b.wg.Add(1)
	go b.run()
	select {
	case replay := <-emit.events:
		if replay.ID != "synthetic-crash-event" || replay.Payload["event_id"] != replay.ID {
			t.Fatal(replay.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("committed event lost on SIGKILL")
	}
	if err = b.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestBufferDiskFullRollback(t *testing.T) {
	dir := os.Getenv("IOT_TEST_BUFFER_FULL_DIR")
	if dir == "" {
		t.Skip("requires an isolated bounded tmpfs; never fill a real workspace filesystem")
	}
	// The launcher supplies a fresh private mount namespace and small tmpfs.
	b, err := faultBuffer(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.db.Close()
	first := sdk.NewEvent(map[string]any{"value": "committed"}, nil)
	if err = b.OnEvent(first); err != nil {
		t.Fatal(err)
	}
	filler := filepath.Join(dir, "filler")
	f, err := os.Create(filler)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(f, io.LimitReader(infiniteZero{}, 32<<20))
	_ = f.Close()
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("private disk was not full: %v", err)
	}
	next := sdk.NewEvent(map[string]any{"value": strings.Repeat("x", 1<<20)}, nil)
	if err = b.OnEvent(next); err == nil {
		t.Fatal("disk-full write falsely acknowledged")
	}
	if err = b.db.View(func(tx *bolt.Tx) error {
		count, _ := readUsage(tx.Bucket([]byte("usage")))
		if count != 1 {
			return fmt.Errorf("failed transaction changed committed usage: %d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if err = b.OnEvent(next); err != nil {
		t.Fatal("did not recover after space restored", err)
	}
	if err = b.db.View(func(tx *bolt.Tx) error {
		count, _ := readUsage(tx.Bucket([]byte("usage")))
		if count != 2 {
			return fmt.Errorf("bad recovered usage: %d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type infiniteZero struct{}

func (infiniteZero) Read(b []byte) (int, error) { clear(b); return len(b), nil }
