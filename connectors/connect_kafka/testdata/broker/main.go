// Loopback-only test helper. Never part of a plugin release artifact.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "telemetry", "engine-output"), kfake.BrokerConfigs(map[string]string{"group.initial.rebalance.delay.ms": "0"}))
	if err != nil {
		panic(err)
	}
	defer cluster.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	client, err := kgo.NewClient(kgo.WithContext(ctx), kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumerGroup("test-helper-inspector"), kgo.ConsumeTopics("engine-output"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		panic(err)
	}
	defer client.Close()
	var mu sync.Mutex
	records := []map[string]any{}
	go func() {
		for ctx.Err() == nil {
			fetch := client.PollRecords(ctx, 1)
			for _, record := range fetch.Records() {
				var value any
				if json.Unmarshal(record.Value, &value) != nil {
					continue
				}
				mu.Lock()
				if len(records) < 256 {
					records = append(records, map[string]any{"key": string(record.Key), "value": value})
				}
				mu.Unlock()
			}
		}
	}()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var payload struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1048576)).Decode(&payload) != nil {
			w.WriteHeader(400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if client.ProduceSync(ctx, &kgo.Record{Topic: "telemetry", Key: []byte(payload.Key), Value: payload.Value}).FirstErr() != nil {
			w.WriteHeader(502)
			return
		}
		fmt.Fprint(w, `{"published":true}`)
	})
	mux.HandleFunc("/records", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(w).Encode(records)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 6 * time.Second}
	go server.Serve(listener)
	json.NewEncoder(os.Stdout).Encode(map[string]any{"brokers": cluster.ListenAddrs(), "control_url": "http://" + listener.Addr().String()})
	<-ctx.Done()
	server.Close()
}
