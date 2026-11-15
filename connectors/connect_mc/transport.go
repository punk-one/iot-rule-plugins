package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
)

var mcSerial atomic.Uint32
var errMCSerial = errors.New("MC response serial mismatch")

func mcASCII(cfg connectionConfig) bool { return strings.HasSuffix(cfg.Mode, "_ascii") }
func mc4E(cfg connectionConfig) bool    { return strings.HasPrefix(cfg.Mode, "4e_") }
func mcHex(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty MC hex field")
	}
	for _, c := range []byte(s) {
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f') {
			return 0, errors.New("invalid MC ASCII hexadecimal field")
		}
	}
	return strconv.ParseUint(s, 16, 32)
}
func asciiRequestBody(body []byte, timer int) ([]byte, error) {
	if len(body) < 12 {
		return nil, errors.New("invalid MC request body")
	}
	command := binary.LittleEndian.Uint16(body[2:])
	sub := binary.LittleEndian.Uint16(body[4:])
	count := int(binary.LittleEndian.Uint16(body[10:]))
	if (command != 0x0401 && command != 0x1401) || sub > 1 || count < 1 {
		return nil, errors.New("invalid MC command/subcommand/count")
	}
	area := ""
	for name, code := range deviceCodes {
		if code == body[9] {
			area = name
			break
		}
	}
	if area == "" {
		return nil, errors.New("unsupported ASCII device")
	}
	address := int(body[6]) | int(body[7])<<8 | int(body[8])<<16
	addressText := fmt.Sprintf("%06X", address)
	if area == "D" || area == "M" || area == "R" {
		if address > 999999 {
			return nil, errors.New("MC ASCII decimal device address exceeds six digits")
		}
		addressText = fmt.Sprintf("%06d", address)
	}
	text := fmt.Sprintf("%04X%04X%04X%s*%s%04X", timer, command, sub, area, addressText, count)
	if command == 0x1401 {
		if sub == 1 {
			if len(body[12:]) != (count+1)/2 {
				return nil, errors.New("invalid packed bit write")
			}
			for i := 0; i < count; i++ {
				shift := uint(4)
				if i%2 == 1 {
					shift = 0
				}
				bit := (body[12+i/2] >> shift) & 15
				if bit > 1 {
					return nil, errors.New("invalid bit value")
				}
				text += strconv.Itoa(int(bit))
			}
		} else {
			if len(body[12:]) != count*2 {
				return nil, errors.New("invalid word write length")
			}
			for i := 0; i < count; i++ {
				text += fmt.Sprintf("%04X", binary.LittleEndian.Uint16(body[12+i*2:]))
			}
		}
	} else if len(body) != 12 {
		return nil, errors.New("unexpected read payload")
	}
	return []byte(text), nil
}
func encodeMCFrame(cfg connectionConfig, body []byte, serial uint16) ([]byte, error) {
	if mc1E(cfg) {
		return encodeOneE(cfg, body)
	}
	timer := cfg.MonitoringTimer
	if timer == 0 {
		timer = 16
	}
	if mcASCII(cfg) {
		data, err := asciiRequestBody(body, timer)
		if err != nil {
			return nil, err
		}
		header := "5000"
		if mc4E(cfg) {
			header = fmt.Sprintf("5400%04X0000", serial)
		}
		return []byte(header + fmt.Sprintf("%02X%02X%04X%02X%04X", cfg.NetworkNumber, cfg.PCNumber, cfg.IONumber, cfg.StationNumber, len(data)) + string(data)), nil
	}
	if len(body) < 12 || len(body) > 8192 {
		return nil, errors.New("invalid binary MC body")
	}
	data := append([]byte(nil), body...)
	binary.LittleEndian.PutUint16(data, uint16(timer))
	prefix := []byte{0x50, 0}
	if mc4E(cfg) {
		prefix = []byte{0x54, 0, byte(serial), byte(serial >> 8), 0, 0}
	}
	route := []byte{byte(cfg.NetworkNumber), byte(cfg.PCNumber), byte(cfg.IONumber), byte(cfg.IONumber >> 8), byte(cfg.StationNumber), byte(len(data)), byte(len(data) >> 8)}
	return append(append(prefix, route...), data...), nil
}
func mcHeaderSize(cfg connectionConfig) int {
	if mc1E(cfg) {
		if mcASCII(cfg) {
			return 4
		}
		return 2
	}
	size := 9
	if mc4E(cfg) {
		size = 13
	}
	if mcASCII(cfg) {
		size *= 2
	}
	return size
}
func mcResponseSize(cfg connectionConfig, header []byte) (int, error) {
	if len(header) != mcHeaderSize(cfg) {
		return 0, errors.New("invalid MC header size")
	}
	size := 0
	if mcASCII(cfg) {
		v, err := mcHex(string(header[len(header)-4:]))
		if err != nil {
			return 0, err
		}
		size = int(v)
		if size < 4 || size > 8192 {
			return 0, errors.New("invalid MC ASCII response size")
		}
	} else {
		size = int(binary.LittleEndian.Uint16(header[len(header)-2:]))
		if size < 2 || size > 8192 {
			return 0, errors.New("invalid MC binary response size")
		}
	}
	return size, nil
}
func decodeMCResponse(cfg connectionConfig, request, frame, body []byte) ([]byte, error) {
	if mc1E(cfg) {
		return decodeOneEResponse(cfg, body, frame)
	}
	headSize := mcHeaderSize(cfg)
	if len(frame) < headSize {
		return nil, errors.New("truncated MC response")
	}
	size, err := mcResponseSize(cfg, frame[:headSize])
	if err != nil || len(frame) != headSize+size {
		return nil, errors.New("invalid MC response length")
	}
	routeOffset := 2
	if mc4E(cfg) {
		routeOffset = 6
	}
	if mcASCII(cfg) {
		routeOffset *= 2
	}
	if mcASCII(cfg) {
		want := "D000"
		if mc4E(cfg) {
			want = "D400"
		}
		if !strings.EqualFold(string(frame[:4]), want) || !strings.EqualFold(string(frame[routeOffset:headSize-4]), string(request[routeOffset:headSize-4])) {
			return nil, errors.New("invalid MC ASCII response header/routing")
		}
		if mc4E(cfg) {
			if string(frame[8:12]) != "0000" {
				return nil, errors.New("invalid MC 4E reserved field")
			}
			if !strings.EqualFold(string(frame[4:8]), string(request[4:8])) {
				return nil, errMCSerial
			}
		}
		result := frame[headSize:]
		end, err := mcHex(string(result[:4]))
		if err != nil {
			return nil, err
		}
		if end != 0 {
			return nil, fmt.Errorf("MC end code 0x%04x", end)
		}
		data := result[4:]
		command := binary.LittleEndian.Uint16(body[2:])
		if command == 0x1401 {
			if len(data) != 0 {
				return nil, errors.New("unexpected MC write response data")
			}
			return nil, nil
		}
		count := int(binary.LittleEndian.Uint16(body[10:]))
		if binary.LittleEndian.Uint16(body[4:]) == 1 {
			if len(data) != count {
				return nil, errors.New("MC ASCII bit response count mismatch")
			}
			packed := make([]byte, (count+1)/2)
			for i, c := range data {
				if c != '0' && c != '1' {
					return nil, errors.New("invalid MC ASCII bit")
				}
				shift := uint(4)
				if i%2 == 1 {
					shift = 0
				}
				packed[i/2] |= (c - '0') << shift
			}
			return packed, nil
		}
		if len(data) != count*4 {
			return nil, errors.New("MC ASCII word response count mismatch")
		}
		words := make([]byte, count*2)
		for i := 0; i < count; i++ {
			v, err := mcHex(string(data[i*4 : i*4+4]))
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint16(words[i*2:], uint16(v))
		}
		return words, nil
	}
	want := byte(0xd0)
	if mc4E(cfg) {
		want = 0xd4
	}
	if frame[0] != want || frame[1] != 0 || !bytes.Equal(frame[routeOffset:headSize-2], request[routeOffset:headSize-2]) {
		return nil, errors.New("invalid MC binary response header/routing")
	}
	if mc4E(cfg) {
		if frame[4] != 0 || frame[5] != 0 {
			return nil, errors.New("invalid MC 4E reserved field")
		}
		if !bytes.Equal(frame[2:4], request[2:4]) {
			return nil, errMCSerial
		}
	}
	result := frame[headSize:]
	if end := binary.LittleEndian.Uint16(result); end != 0 {
		return nil, fmt.Errorf("MC end code 0x%04x", end)
	}
	return result[2:], nil
}
func transactMC(conn net.Conn, cfg connectionConfig, body []byte) ([]byte, error) {
	if mc1E(cfg) {
		return transactOneE(conn, cfg, body)
	}
	request, err := encodeMCFrame(cfg, body, uint16(mcSerial.Add(1)))
	if err != nil {
		return nil, err
	}
	if cfg.Transport == "udp" {
		n, err := conn.Write(request)
		if err != nil {
			return nil, err
		}
		if n != len(request) {
			return nil, io.ErrShortWrite
		}
		for stray := 0; stray < 8; stray++ {
			frame := make([]byte, mcHeaderSize(cfg)+8193)
			n, err = conn.Read(frame)
			if err != nil {
				return nil, err
			}
			response, err := decodeMCResponse(cfg, request, frame[:n], body)
			if errors.Is(err, errMCSerial) && mc4E(cfg) {
				continue
			}
			return response, err
		}
		return nil, errors.New("too many stale MC UDP serial responses")
	}
	if _, err = io.Copy(conn, bytes.NewReader(request)); err != nil {
		return nil, err
	}
	header := make([]byte, mcHeaderSize(cfg))
	if _, err = io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	size, err := mcResponseSize(cfg, header)
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	return decodeMCResponse(cfg, request, append(header, data...), body)
}
