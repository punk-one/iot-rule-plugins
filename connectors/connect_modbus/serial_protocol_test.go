package main

import (
	"bytes"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"
)

func TestStandardSerialVectorsAndTamper(t *testing.T) {
	pdu := []byte{3, 0, 0, 0, 10}
	expected, _ := hex.DecodeString("01030000000ac5cd")
	if !bytes.Equal(encodeSerial(1, pdu, false), expected) {
		t.Fatal("CRC wire order wrong")
	}
	if string(encodeSerial(1, pdu, true)) != ":01030000000AF2\r\n" {
		t.Fatal("LRC ASCII encoding wrong")
	}
	for _, ascii := range []bool{false, true} {
		for _, bad := range []bool{false, true} {
			client, server := net.Pipe()
			client.SetDeadline(time.Now().Add(time.Second))
			go func() {
				defer server.Close()
				request := encodeSerial(1, pdu, ascii)
				io.CopyN(io.Discard, server, int64(len(request)))
				response := encodeSerial(1, []byte{3, 2, 0, 42}, ascii)
				if bad {
					response[len(response)-3] ^= 1
				}
				server.Write(response)
			}()
			response, err := serialTransaction(client, 1, pdu, ascii)
			client.Close()
			if bad && err == nil {
				t.Fatal("checksum tamper accepted")
			}
			if !bad && (err != nil || !bytes.Equal(response, []byte{3, 2, 0, 42})) {
				t.Fatalf("%v %v", response, err)
			}
		}
	}
}
