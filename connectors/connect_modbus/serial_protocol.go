package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

func crc16(data []byte) uint16 {
	crc := uint16(0xffff)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xa001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
func lrc(data []byte) byte {
	var sum byte
	for _, b := range data {
		sum += b
	}
	return -sum
}
func encodeSerial(unit byte, pdu []byte, ascii bool) []byte {
	data := append([]byte{unit}, pdu...)
	if ascii {
		data = append(data, lrc(data))
		return []byte(":" + strings.ToUpper(hex.EncodeToString(data)) + "\r\n")
	}
	checksum := crc16(data)
	return append(data, byte(checksum), byte(checksum>>8))
}
func serialTransaction(conn io.ReadWriter, unit byte, pdu []byte, ascii bool) ([]byte, error) {
	frame := encodeSerial(unit, pdu, ascii)
	if _, err := io.Copy(conn, bytes.NewReader(frame)); err != nil {
		return nil, err
	}
	var response []byte
	if ascii {
		reader := bufio.NewReader(conn)
		line := make([]byte, 0, 64)
		for len(line) <= 513 {
			b, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			line = append(line, b)
			if b == '\n' {
				break
			}
		}
		if len(line) < 9 || len(line) > 513 || line[0] != ':' || !bytes.HasSuffix(line, []byte("\r\n")) {
			return nil, errors.New("invalid ASCII frame")
		}
		decoded, err := hex.DecodeString(string(line[1 : len(line)-2]))
		if err != nil || len(decoded) < 4 || lrc(decoded) != 0 {
			return nil, errors.New("ASCII LRC mismatch")
		}
		if decoded[0] != unit {
			return nil, errors.New("ASCII unit mismatch")
		}
		response = decoded[1 : len(decoded)-1]
	} else {
		header := make([]byte, 3)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, err
		}
		if header[0] != unit {
			return nil, errors.New("RTU unit mismatch")
		}
		size := 8
		if header[1]&0x80 != 0 {
			size = 5
		} else if header[1] >= 1 && header[1] <= 4 {
			size = 5 + int(header[2])
		}
		if size < 5 || size > 256 {
			return nil, errors.New("RTU frame exceeds bound")
		}
		frame = make([]byte, size)
		copy(frame, header)
		if _, err := io.ReadFull(conn, frame[3:]); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint16(frame[size-2:]) != crc16(frame[:size-2]) {
			return nil, errors.New("RTU CRC mismatch")
		}
		response = frame[1 : size-2]
	}
	if response[0] == pdu[0]|0x80 {
		if len(response) != 2 {
			return nil, errors.New("malformed exception")
		}
		return nil, fmt.Errorf("Modbus exception %d", response[1])
	}
	if response[0] != pdu[0] {
		return nil, errors.New("unexpected function")
	}
	return response, nil
}
