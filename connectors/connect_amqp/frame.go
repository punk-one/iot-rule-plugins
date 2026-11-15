package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

// Inspect bounded individual frames BEFORE the AMQP library can allocate an
// entire content body. This wrapper is installed after TLS decryption.
type frameConn struct {
	net.Conn
	maxBody uint64
	pending []byte
}

func (c *frameConn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(c.pending) == 0 {
		var header [7]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return 0, err
		}
		size := binary.BigEndian.Uint32(header[3:])
		kind := header[0]
		if size > 131072-8 || (kind != 1 && kind != 2 && kind != 3 && kind != 8) {
			c.Close()
			return 0, errors.New("invalid or oversized AMQP frame")
		}
		payload := make([]byte, int(size)+1)
		if _, err := io.ReadFull(c.Conn, payload); err != nil {
			return 0, err
		}
		if payload[len(payload)-1] != 0xce {
			c.Close()
			return 0, errors.New("invalid AMQP frame end")
		}
		if kind == 2 && (len(payload) < 13 || binary.BigEndian.Uint16(payload[:2]) != 60 || binary.BigEndian.Uint16(payload[2:4]) != 0 || binary.BigEndian.Uint64(payload[4:12]) > c.maxBody) {
			c.Close()
			return 0, errors.New("AMQP content body exceeds configured bound")
		}
		c.pending = append(header[:], payload...)
	}
	n := copy(dst, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
