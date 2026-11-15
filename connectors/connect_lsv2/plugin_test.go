package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"testing"
)

func TestLSV2LengthIsPayloadOnlyAndSelectors(t *testing.T) {
	frame := telegram("R_VR", []byte{5})
	if !bytes.Equal(frame, []byte{0, 0, 0, 1, 'R', '_', 'V', 'R', 5}) {
		t.Fatal("length incorrectly includes command")
	}
	if queries["execution_state"].selector != 23 || queries["program_state"].selector != 26 {
		t.Fatal("wrong runtime selector")
	}
	bad := make([]byte, 8)
	binary.BigEndian.PutUint32(bad, 65537)
	if _, _, err := readTelegram(bytes.NewReader(bad)); err == nil {
		t.Fatal("unbounded frame")
	}
}
func TestLSV2BatchSharesLoginAndReadOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			command, payload, err := readTelegram(conn)
			if err != nil {
				return
			}
			response := "T_OK"
			raw := []byte{}
			switch command {
			case "A_LG":
				if string(payload) != "INSPECT\x00" && string(payload) != "DNC\x00" {
					t.Error("invalid login")
				}
			case "R_VR":
				response = "S_VR"
				if len(payload) == 0 {
					raw = []byte("TNC\x00NC\x00PLC\x00OPTIONS\x00")
				} else {
					if !bytes.Equal(payload, []byte{5}) {
						t.Error("version selector must be 1 byte")
					}
					raw = []byte("DEMO\x00")
				}
			case "R_RI":
				response = "S_RI"
				raw = []byte{0, 4}
			}
			io.Copy(conn, bytes.NewReader(telegram(response, raw)))
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: host, Port: n, TimeoutMS: 1000, AllowDNC: true})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"version","query":"version"},{"name":"id","query":"control_id"},{"name":"state","query":"execution_state"}]}`)})
	if err != nil || !bytes.Contains(response.Payload, []byte(`"id":"DEMO"`)) || !bytes.Contains(response.Payload, []byte(`"state":4`)) {
		t.Fatal(string(response.Payload), err)
	}
	if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite}); err == nil {
		t.Fatal("write accepted")
	}
}
