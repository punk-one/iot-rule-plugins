package main

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

const (
	effFlag uint32 = 0x80000000
	rtrFlag uint32 = 0x40000000
	errFlag uint32 = 0x20000000
	effMask uint32 = 0x1fffffff
	sffMask uint32 = 0x7ff
)

type identifier struct {
	ID       string `json:"id"`
	Extended bool   `json:"extended"`
}
type frameSpec struct {
	ID            string `json:"id"`
	Extended      bool   `json:"extended"`
	FD            bool   `json:"fd"`
	Remote        bool   `json:"remote"`
	RTRLength     int    `json:"rtr_length,omitempty"`
	BitrateSwitch bool   `json:"bitrate_switch"`
	ErrorState    bool   `json:"error_state"`
	Data          []byte `json:"data"`
}
type canFrame struct {
	expiresAt                                int64
	id                                       uint32
	extended, fd, remote, brs, esi, busError bool
	data                                     []byte
	rtrLength, rawDLC                        int
	dropDelta                                uint32
}

func parseID(raw string, extended bool) (uint32, error) {
	if raw == "" || len(raw) > 10 || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "+-") {
		return 0, errors.New("CAN ID must be canonical decimal or 0x-prefixed hex")
	}
	base := 10
	value := raw
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		base = 16
		value = raw[2:]
	} else if len(raw) > 1 && raw[0] == '0' {
		return 0, errors.New("CAN decimal ID must not contain leading zeroes")
	}
	number, err := strconv.ParseUint(value, base, 32)
	limit := uint64(sffMask)
	if extended {
		limit = uint64(effMask)
	}
	if err != nil || number > limit {
		return 0, errors.New("CAN ID exceeds its 11/29-bit format")
	}
	return uint32(number), nil
}
func key(id uint32, extended bool) string {
	prefix := "s:"
	if extended {
		prefix = "e:"
	}
	return prefix + strconv.FormatUint(uint64(id), 16)
}
func identifierKey(spec identifier) (string, error) {
	id, err := parseID(spec.ID, spec.Extended)
	return key(id, spec.Extended), err
}
func validFDLength(n int) bool {
	return n >= 0 && n <= 8 || n == 12 || n == 16 || n == 20 || n == 24 || n == 32 || n == 48 || n == 64
}
func fromSpec(spec frameSpec) (canFrame, error) {
	id, err := parseID(spec.ID, spec.Extended)
	if err != nil {
		return canFrame{}, err
	}
	if spec.FD {
		if spec.Remote || spec.RTRLength != 0 || !validFDLength(len(spec.Data)) {
			return canFrame{}, errors.New("CAN FD needs explicit canonical payload length and cannot be RTR")
		}
	} else if len(spec.Data) > 8 || spec.BitrateSwitch || spec.ErrorState || spec.RTRLength < 0 || spec.RTRLength > 8 || spec.Remote && len(spec.Data) != 0 || !spec.Remote && spec.RTRLength != 0 {
		return canFrame{}, errors.New("invalid classic CAN frame or RTR length")
	}
	return canFrame{id: id, extended: spec.Extended, fd: spec.FD, remote: spec.Remote, brs: spec.BitrateSwitch, esi: spec.ErrorState, data: append([]byte(nil), spec.Data...), rtrLength: spec.RTRLength}, nil
}
func encodeFrame(f canFrame) ([]byte, error) {
	if f.busError {
		return nil, errors.New("transmitting CAN error frames is forbidden")
	}
	spec := frameSpec{ID: "0x" + strconv.FormatUint(uint64(f.id), 16), Extended: f.extended, FD: f.fd, Remote: f.remote, RTRLength: f.rtrLength, BitrateSwitch: f.brs, ErrorState: f.esi, Data: f.data}
	if _, err := fromSpec(spec); err != nil {
		return nil, err
	}
	length := 16
	if f.fd {
		length = 72
	}
	out := make([]byte, length)
	id := f.id
	if f.extended {
		id |= effFlag
	}
	if f.remote {
		id |= rtrFlag
	}
	binary.NativeEndian.PutUint32(out, id)
	out[4] = byte(len(f.data))
	if f.remote {
		out[4] = byte(f.rtrLength)
	}
	if f.fd {
		out[5] = 4
		if f.brs {
			out[5] |= 1
		}
		if f.esi {
			out[5] |= 2
		}
	}
	copy(out[8:], f.data)
	return out, nil
}
func decodeFrame(raw []byte) (canFrame, error) {
	if len(raw) != 16 && len(raw) != 72 {
		return canFrame{}, errors.New("invalid SocketCAN frame size")
	}
	wire := binary.NativeEndian.Uint32(raw)
	f := canFrame{id: wire & effMask, extended: wire&effFlag != 0, remote: wire&rtrFlag != 0, busError: wire&errFlag != 0, fd: len(raw) == 72, rawDLC: int(raw[7])}
	length := int(raw[4])
	if !f.fd && f.rawDLC != 0 && (length != 8 || f.rawDLC < 9 || f.rawDLC > 15) {
		return canFrame{}, errors.New("invalid raw classic CAN DLC")
	}
	if f.busError {
		if f.fd || f.remote || f.extended || length != 8 {
			return canFrame{}, errors.New("invalid CAN error frame")
		}
	} else if !f.extended && f.id > sffMask {
		return canFrame{}, errors.New("standard CAN ID has reserved bits")
	}
	if f.fd {
		if f.remote || !validFDLength(length) || raw[5]&^byte(7) != 0 {
			return canFrame{}, errors.New("invalid CAN FD length/flags")
		}
		f.brs = raw[5]&1 != 0
		f.esi = raw[5]&2 != 0
	} else if length > 8 {
		return canFrame{}, errors.New("invalid classic CAN length")
	}
	if f.remote {
		f.rtrLength = length
	} else {
		f.data = append([]byte(nil), raw[8:8+length]...)
	}
	return f, nil
}
