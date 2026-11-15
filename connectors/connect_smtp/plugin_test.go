package main

import (
	"bufio"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestMailCompositionPolicy(t *testing.T) {
	cfg := config{From: "alerts@example.invalid", Domains: []string{"example.invalid"}}
	m := message{To: []string{"operator@example.invalid"}, Subject: "通知", Body: "sample", ID: "test@example.invalid"}
	if _, _, _, err := compose(cfg, m); err != nil {
		t.Fatal(err)
	}
	m.Subject = "ok\r\nBcc: other@example.invalid"
	if _, _, _, err := compose(cfg, m); err == nil {
		t.Fatal("header injection accepted")
	}
	m.Subject = "ok"
	m.To = []string{"other@outside.invalid"}
	if _, _, _, err := compose(cfg, m); err == nil {
		t.Fatal("recipient policy bypass")
	}
}
func TestLoopbackSMTPReceiptOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	received := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("220 fixture ready\r\n"))
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"):
				conn.Write([]byte("250 fixture\r\n"))
			case strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
				conn.Write([]byte("250 accepted\r\n"))
			case strings.HasPrefix(line, "DATA"):
				conn.Write([]byte("354 send\r\n"))
				body := ""
				for {
					line, err = reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == ".\r\n" {
						break
					}
					body += line
				}
				received <- body
				conn.Write([]byte("250 queued\r\n"))
			default:
				conn.Write([]byte("221 bye\r\n"))
				return
			}
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: host, Port: n, Security: "none", From: "alerts@example.invalid", Domains: []string{"example.invalid"}, TimeoutMS: 1000})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(message{To: []string{"operator@example.invalid"}, Subject: "test", Body: "synthetic", ID: "test@example.invalid"})
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "submit", Payload: raw})
	if err != nil || !strings.Contains(string(response.Payload), "smtp_receipt") {
		t.Fatal(string(response.Payload), err)
	}
	if !strings.Contains(<-received, "Message-ID: <test@example.invalid>") {
		t.Fatal("message ID missing")
	}
}
