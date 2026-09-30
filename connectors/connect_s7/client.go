package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/punk-one/gos7"
)

func s7ConnectClient(host string, port int, rack int, slot int, connectType string, timeoutSec int, idleTimeoutSec int) (gos7.Client, *gos7.TCPClientHandler, string, string, error) {
	addr := s7Address(host, port)
	handler := s7NewHandler(addr, rack, slot, connectType, timeoutSec, idleTimeoutSec)
	if err := handler.Connect(); err != nil {
		return nil, nil, addr, "", s7WrapConnectError(addr, connectType, err)
	}
	return gos7.NewClient(handler), handler, addr, "", nil
}

func s7NewHandler(addr string, rack int, slot int, connectType string, timeoutSec int, idleTimeoutSec int) *gos7.TCPClientHandler {
	handler := gos7.NewTCPClientHandlerWithConnectType(addr, rack, slot, s7ConnectTypeValue(connectType))
	if timeoutSec <= 0 {
		timeoutSec = 5
	}
	if idleTimeoutSec <= 0 {
		idleTimeoutSec = 30
	}
	handler.Timeout = time.Duration(timeoutSec) * time.Second
	handler.IdleTimeout = time.Duration(idleTimeoutSec) * time.Second
	return handler
}

func s7Address(host string, port int) string {
	return fmt.Sprintf("%s:%d", strings.TrimSpace(host), s7NormalizePort(port))
}

func s7NormalizePort(port int) int {
	if port <= 0 {
		return s7DefaultPort
	}
	return port
}

func s7NormalizeConnectType(connectType string) string {
	normalized := strings.ToLower(strings.TrimSpace(connectType))
	switch normalized {
	case "", s7DefaultConnectType:
		return s7DefaultConnectType
	case "op":
		return "op"
	case "basic":
		return "basic"
	default:
		return s7DefaultConnectType
	}
}

func s7ConnectTypeValue(connectType string) int {
	switch s7NormalizeConnectType(connectType) {
	case "op":
		return s7ConnectTypeOP
	case "basic":
		return s7ConnectTypeBasic
	default:
		return s7ConnectTypePG
	}
}

func s7WrapConnectError(addr string, connectType string, err error) error {
	if err == nil {
		return nil
	}
	msg := fmt.Sprintf("connect to %s (connect_type=%s): %v", addr, s7NormalizeConnectType(connectType), err)
	if strings.Contains(err.Error(), s7InvalidPDUText) {
		msg += "; peer did not return a valid S7 ISO-on-TCP handshake, verify connection.port/connect_type (standard S7 port is 102)"
	}
	return fmt.Errorf("%s", msg)
}
