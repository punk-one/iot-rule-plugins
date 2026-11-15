package main

import (
	"errors"
	"io"
)

type framing struct{ Mode string }

func streamReader(conn connection, _ framing) io.Reader { return conn }
func readFrame(r io.Reader, _ framing, limit int) ([]byte, error) {
	b := make([]byte, limit)
	n, e := r.Read(b)
	if e != nil {
		return nil, e
	}
	if n == 0 {
		return nil, errors.New("empty decoded IEC104 ASDU")
	}
	return b[:n], nil
}
func writeAll(w io.Writer, b []byte) error {
	if len(b) != 0 {
		return errors.New("IEC104 application writes disabled")
	}
	return nil
}
