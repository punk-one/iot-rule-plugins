package main

import (
	"bufio"
	"io"
)

func streamReader(conn connection, _ framing) io.Reader { return bufio.NewReader(conn) }
