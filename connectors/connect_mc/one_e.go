package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

func mc1E(cfg connectionConfig) bool { return strings.HasPrefix(cfg.Mode, "1e_") }
func oneESpec(body []byte) (byte, string, int, error) {
	if len(body) < 12 {
		return 0, "", 0, errors.New("invalid 1E body")
	}
	cmd := binary.LittleEndian.Uint16(body[2:])
	sub := binary.LittleEndian.Uint16(body[4:])
	count := int(binary.LittleEndian.Uint16(body[10:]))
	if sub > 1 || count < 1 {
		return 0, "", 0, errors.New("invalid 1E subcommand/count")
	}
	area := ""
	for name, code := range deviceCodes {
		if code == body[9] {
			area = name
			break
		}
	}
	if area == "" {
		return 0, "", 0, errors.New("unsupported 1E device")
	}
	bit := area == "M" || area == "X" || area == "Y" || area == "B"
	if (sub == 1) != bit {
		return 0, "", 0, errors.New("1E bit/word device mismatch")
	}
	var native byte
	limit := 64
	switch cmd {
	case 0x0401:
		native = 1
		if bit {
			native = 0
			limit = 256
		}
	case 0x1401:
		native = 3
		if bit {
			native = 2
			limit = 160
		}
	default:
		return 0, "", 0, errors.New("unsupported 1E operation")
	}
	if count > limit {
		return 0, "", 0, errors.New("1E point count exceeds conservative FX5 profile")
	}
	return native, area, count, nil
}
func encodeOneE(cfg connectionConfig, body []byte) ([]byte, error) {
	cmd, area, count, err := oneESpec(body)
	if err != nil {
		return nil, err
	}
	address := uint32(body[6]) | uint32(body[7])<<8 | uint32(body[8])<<16
	// The FX5 1E profile specifies FF PC number, zero timer and fixed final byte.
	// The local request deadline remains finite even though this wire field is zero.
	if mcASCII(cfg) {
		text := fmt.Sprintf("%02XFF0000%02X20%08X%02X00", cmd, area[0], address, byte(count))
		if cmd >= 2 {
			if cmd == 2 {
				if len(body[12:]) != (count+1)/2 {
					return nil, errors.New("invalid 1E bit write")
				}
				for i := 0; i < count; i++ {
					shift := uint(4)
					if i%2 == 1 {
						shift = 0
					}
					bit := (body[12+i/2] >> shift) & 15
					if bit > 1 {
						return nil, errors.New("invalid 1E bit")
					}
					text += fmt.Sprint(bit)
				}
			} else {
				if len(body[12:]) != count*2 {
					return nil, errors.New("invalid 1E word write")
				}
				for i := 0; i < count; i++ {
					text += fmt.Sprintf("%04X", binary.LittleEndian.Uint16(body[12+i*2:]))
				}
			}
		}
		return []byte(text), nil
	}
	frame := make([]byte, 12)
	frame[0] = cmd
	frame[1] = 0xff
	binary.LittleEndian.PutUint32(frame[4:8], address)
	frame[8] = 0x20
	frame[9] = area[0]
	frame[10] = byte(count)
	if cmd >= 2 {
		expected := count * 2
		if cmd == 2 {
			expected = (count + 1) / 2
		}
		if len(body[12:]) != expected {
			return nil, errors.New("invalid 1E write size")
		}
		frame = append(frame, body[12:]...)
	}
	return frame, nil
}
func oneEDataSize(cfg connectionConfig, body []byte) (int, error) {
	cmd, _, count, err := oneESpec(body)
	if err != nil {
		return 0, err
	}
	if cmd >= 2 {
		return 0, nil
	}
	if cmd == 0 {
		if mcASCII(cfg) {
			return count, nil
		}
		return (count + 1) / 2, nil
	}
	if mcASCII(cfg) {
		return count * 4, nil
	}
	return count * 2, nil
}
func decodeOneEResponse(cfg connectionConfig, body, frame []byte) ([]byte, error) {
	cmd, _, count, err := oneESpec(body)
	if err != nil {
		return nil, err
	}
	header := 2
	if mcASCII(cfg) {
		header = 4
	}
	if len(frame) < header {
		return nil, errors.New("truncated 1E response")
	}
	code, end := uint64(frame[0]), uint64(frame[1])
	if mcASCII(cfg) {
		code, err = mcHex(string(frame[:2]))
		if err != nil {
			return nil, err
		}
		end, err = mcHex(string(frame[2:4]))
		if err != nil {
			return nil, err
		}
	}
	if code != uint64(cmd|0x80) {
		return nil, errors.New("1E response operation mismatch")
	}
	if end != 0 {
		return nil, fmt.Errorf("1E end code 0x%02x; no transparent retry", end)
	}
	size, _ := oneEDataSize(cfg, body)
	if len(frame) != header+size {
		return nil, errors.New("1E response point count mismatch")
	}
	data := frame[header:]
	if !mcASCII(cfg) || cmd >= 2 {
		return data, nil
	}
	if cmd == 0 {
		packed := make([]byte, (count+1)/2)
		for i, c := range data {
			if c != '0' && c != '1' {
				return nil, errors.New("invalid 1E ASCII bit value")
			}
			shift := uint(4)
			if i%2 == 1 {
				shift = 0
			}
			packed[i/2] |= (c - '0') << shift
		}
		return packed, nil
	}
	words := make([]byte, count*2)
	for i := 0; i < count; i++ {
		value, err := mcHex(string(data[i*4 : i*4+4]))
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint16(words[i*2:], uint16(value))
	}
	return words, nil
}
func transactOneE(conn net.Conn, cfg connectionConfig, body []byte) ([]byte, error) {
	request, err := encodeOneE(cfg, body)
	if err != nil {
		return nil, err
	}
	if cfg.Transport == "udp" {
		if n, err := conn.Write(request); err != nil {
			return nil, err
		} else if n != len(request) {
			return nil, io.ErrShortWrite
		}
		frame := make([]byte, 1025)
		n, err := conn.Read(frame)
		if err != nil {
			return nil, err
		}
		return decodeOneEResponse(cfg, body, frame[:n])
	}
	if _, err = io.Copy(conn, bytes.NewReader(request)); err != nil {
		return nil, err
	}
	headerSize := 2
	if mcASCII(cfg) {
		headerSize = 4
	}
	head := make([]byte, headerSize)
	if _, err = io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	// Error frames are rejected immediately; retiring the socket discards any
	// optional detailed-error suffix without confusing a following request.
	if mcASCII(cfg) {
		end, err := mcHex(string(head[2:]))
		if err != nil || end != 0 {
			return decodeOneEResponse(cfg, body, head)
		}
	} else if head[1] != 0 {
		return decodeOneEResponse(cfg, body, head)
	}
	size, _ := oneEDataSize(cfg, body)
	data := make([]byte, size)
	if _, err = io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	return decodeOneEResponse(cfg, body, append(head, data...))
}
