//go:build windows

package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	path := flag.String("config", "bridge.json", "bridge configuration file")
	flag.Parse()
	file, err := os.Open(*path)
	if err != nil {
		log.Fatal("cannot open bridge configuration")
	}
	defer file.Close()
	var cfg config
	decoder := json.NewDecoder(http.MaxBytesReader(nil, file, 1048576))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil {
		log.Fatal("invalid bridge configuration")
	}
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		log.Fatal("listen must be host:port")
	}
	if host != "127.0.0.1" && host != "::1" && (!strings.Contains(cfg.Listen, "localhost:") || host != "localhost") && (cfg.TLSCert == "" || cfg.TLSKey == "") {
		log.Fatal("non-loopback bridge listen requires TLS certificate and key")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	bridge, err := newBridge(cfg, os.Getenv("BRIDGE_TOKEN"), readCOM, ctx)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: cfg.Listen, Handler: bridge, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: time.Duration(cfg.TimeoutMS+2000) * time.Millisecond, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	if cfg.TLSCert != "" {
		err = server.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		log.Fatal("bridge HTTP listener failed")
	}
}
