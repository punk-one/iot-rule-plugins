package main

import (
	"io"
)

func streamReader(conn connection, _ framing) io.Reader { return conn }
