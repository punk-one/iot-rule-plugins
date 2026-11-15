package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

type connection interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// Read owns all protocol state and writes. There is one reader per resource,
// so S/U acknowledgements cannot interleave bytes with another rule's command.
type iecStreamDevice struct {
	conn                                        net.Conn
	cfg                                         config
	receive, sent, ack                          uint16
	lastTraffic, pendingI, testSent, giDeadline time.Time
	giActive, giComplete                        bool
	allowed                                     map[uint32]bool
}
type iecFrame struct {
	Values   map[string]any    `json:"values"`
	Quality  map[string]string `json:"quality"`
	Details  map[string]any    `json:"details"`
	Cause    int               `json:"cause"`
	Received int64             `json:"received_at"`
}

func dial(ctx context.Context, cfg config) (connection, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	if err = writeAPDU(conn, apdu([]byte{7, 0, 0, 0}, nil)); err != nil {
		return nil, err
	}
	reply, err := readAPDU(conn)
	if err != nil || len(reply) != 4 || reply[0] != 11 || reply[1] != 0 || reply[2] != 0 || reply[3] != 0 {
		return nil, errors.New("IEC104 STARTDT confirmation missing")
	}
	now := time.Now()
	d := &iecStreamDevice{conn: conn, cfg: cfg, lastTraffic: now, allowed: map[uint32]bool{}, giComplete: !cfg.InitialGI}
	for _, ioa := range cfg.ReadIOAs {
		d.allowed[ioa] = true
	}
	if cfg.InitialGI {
		asdu := []byte{100, 1, 6, byte(cfg.Originator), byte(cfg.CommonAddress), byte(cfg.CommonAddress >> 8), 0, 0, 0, 20}
		if err = writeAPDU(conn, apdu([]byte{0, 0, 0, 0}, asdu)); err != nil {
			return nil, err
		}
		d.sent = 1
		d.pendingI = now
		d.giDeadline = now.Add(time.Duration(cfg.GITimeoutMS) * time.Millisecond)
	}
	if ctx.Err() != nil || !stop() {
		return nil, errors.New("IEC104 stream setup canceled")
	}
	ok = true
	return d, nil
}
func (d *iecStreamDevice) SetDeadline(t time.Time) error { return d.conn.SetDeadline(t) }
func (d *iecStreamDevice) Close() error                  { return d.conn.Close() }
func (d *iecStreamDevice) Write([]byte) (int, error) {
	return 0, errors.New("IEC104 stream does not accept application/control writes")
}
func (d *iecStreamDevice) send(control []byte) error {
	if err := d.conn.SetWriteDeadline(time.Now().Add(time.Duration(d.cfg.TimeoutMS) * time.Millisecond)); err != nil {
		return err
	}
	if err := writeAPDU(d.conn, apdu(control, nil)); err != nil {
		return err
	}
	d.lastTraffic = time.Now()
	return nil
}
func (d *iecStreamDevice) applyAck(raw []byte) error {
	if len(raw) != 2 || raw[0]&1 != 0 {
		return errors.New("invalid IEC104 receive sequence encoding")
	}
	ack := binary.LittleEndian.Uint16(raw) >> 1
	advance := (ack - d.ack) & 0x7fff
	outstanding := (d.sent - d.ack) & 0x7fff
	if advance > outstanding {
		return errors.New("IEC104 ACK outside outstanding send window")
	}
	d.ack = ack
	if d.ack == d.sent {
		d.pendingI = time.Time{}
	}
	return nil
}
func readTimedAPDU(conn net.Conn, deadline time.Time) ([]byte, bool, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, false, err
	}
	header := make([]byte, 2)
	n, err := io.ReadFull(conn, header)
	if err != nil {
		var timeout net.Error
		if n == 0 && errors.As(err, &timeout) && timeout.Timeout() {
			return nil, true, nil
		}
		return nil, false, err
	}
	if header[0] != 0x68 || header[1] < 4 || header[1] > 253 {
		return nil, false, errors.New("invalid IEC104 APDU header")
	}
	raw := make([]byte, int(header[1]))
	_, err = io.ReadFull(conn, raw)
	return raw, false, err
}
func (d *iecStreamDevice) nextDeadline() (time.Time, error) {
	now := time.Now()
	t1 := time.Duration(d.cfg.T1MS) * time.Millisecond
	deadline := d.lastTraffic.Add(time.Duration(d.cfg.T3MS) * time.Millisecond)
	for _, limit := range []time.Time{d.pendingI, d.testSent} {
		if !limit.IsZero() {
			expiry := limit.Add(t1)
			if !expiry.After(now) {
				return time.Time{}, errors.New("IEC104 I/U acknowledgement timeout")
			}
			if expiry.Before(deadline) {
				deadline = expiry
			}
		}
	}
	if !d.giComplete && !d.giDeadline.IsZero() {
		if !d.giDeadline.After(now) {
			return time.Time{}, errors.New("IEC104 initial interrogation deadline exceeded")
		}
		if d.giDeadline.Before(deadline) {
			deadline = d.giDeadline
		}
	}
	return deadline, nil
}
func (d *iecStreamDevice) Read(buffer []byte) (int, error) {
	loc, _ := time.LoadLocation(d.cfg.TimeZone)
	for {
		deadline, err := d.nextDeadline()
		if err != nil {
			return 0, err
		}
		raw, idle, err := readTimedAPDU(d.conn, deadline)
		if err != nil {
			return 0, err
		}
		if idle {
			if _, err = d.nextDeadline(); err != nil {
				return 0, err
			}
			if d.testSent.IsZero() && !time.Now().Before(d.lastTraffic.Add(time.Duration(d.cfg.T3MS)*time.Millisecond)) {
				if err = d.send([]byte{0x43, 0, 0, 0}); err != nil {
					return 0, err
				}
				d.testSent = time.Now()
			}
			continue
		}
		d.lastTraffic = time.Now()
		if raw[0]&1 == 0 {
			if len(raw) < 10 || raw[2]&1 != 0 {
				return 0, errors.New("malformed IEC104 I frame")
			}
			sequence := binary.LittleEndian.Uint16(raw[:2]) >> 1
			if sequence != d.receive {
				return 0, errors.New("IEC104 I sequence mismatch")
			}
			if err = d.applyAck(raw[2:4]); err != nil {
				return 0, err
			}
			d.receive = (d.receive + 1) & 0x7fff
			asdu := raw[4:]
			samples, done, err := parseASDU(asdu, d.cfg.CommonAddress, loc)
			if err != nil {
				return 0, err
			}
			cause := asdu[2] & 0x3f
			if asdu[3] != 0 && asdu[3] != byte(d.cfg.Originator) {
				return 0, errors.New("unexpected IEC104 originator")
			}
			if asdu[0] == 100 {
				if !d.cfg.InitialGI || d.giComplete || cause != 7 && cause != 10 || cause == 10 && !d.giActive {
					return 0, errors.New("unexpected interrogation confirmation/termination")
				}
				if cause == 7 {
					d.giActive = true
				}
				if done {
					d.giComplete = true
				}
			} else if asdu[0] != 70 && cause != 1 && cause != 2 && cause != 3 && cause != 5 && (cause < 20 || cause > 36) {
				return 0, errors.New("unsupported monitoring cause of transmission")
			}
			if err = d.send([]byte{1, 0, byte(d.receive << 1), byte(d.receive >> 7)}); err != nil {
				return 0, err
			}
			frame := iecFrame{Values: map[string]any{}, Quality: map[string]string{}, Details: map[string]any{}, Cause: int(cause), Received: time.Now().UnixMilli()}
			for ioa, value := range samples {
				if !d.allowed[ioa] {
					continue
				}
				name := strconv.FormatUint(uint64(ioa), 10)
				frame.Values[name] = value.value
				frame.Quality[name], frame.Details[name] = sampleQualityAndDetails(ioa, value)
			}
			if len(frame.Values) == 0 {
				continue
			}
			encoded, err := json.Marshal(frame)
			if err != nil || len(encoded) > len(buffer) {
				return 0, errors.New("IEC104 decoded ASDU exceeds frame bound")
			}
			return copy(buffer, encoded), nil
		} else if raw[0]&3 == 1 {
			if len(raw) != 4 || raw[0] != 1 || raw[1] != 0 {
				return 0, errors.New("malformed IEC104 S frame")
			}
			if err = d.applyAck(raw[2:4]); err != nil {
				return 0, err
			}
		} else {
			if len(raw) != 4 || raw[1] != 0 || raw[2] != 0 || raw[3] != 0 {
				return 0, errors.New("malformed IEC104 U frame")
			}
			switch raw[0] {
			case 0x43:
				if err = d.send([]byte{0x83, 0, 0, 0}); err != nil {
					return 0, err
				}
			case 0x83:
				if d.testSent.IsZero() {
					return 0, errors.New("unsolicited TESTFR confirmation")
				}
				d.testSent = time.Time{}
			case 0x13:
				_ = d.send([]byte{0x23, 0, 0, 0})
				return 0, errors.New("remote stopped IEC104 transfer")
			default:
				return 0, errors.New("unsupported U frame while active")
			}
		}
	}
}
func sampleQualityAndDetails(ioa uint32, s sample) (string, map[string]any) {
	quality := "good"
	if s.quality != 0 {
		quality = "bad:device_quality"
	}
	if s.badTime {
		quality = "bad:timestamp"
	}
	if quality == "good" && s.transient {
		quality = "uncertain:transient"
	}
	if quality == "good" && s.counterFlags&0x40 != 0 {
		quality = "uncertain:counter_adjusted"
	}
	return quality, map[string]any{"ioa": ioa, "type_id": s.typ, "quality_raw": s.quality, "source_time": s.timestamp, "counter_flags": s.counterFlags, "transient": s.transient}
}
func projectIECFrame(raw []byte, points map[uint32]string) ([]byte, error) {
	var source iecFrame
	if len(raw) > 65536 || json.Unmarshal(raw, &source) != nil || source.Received <= 0 {
		return nil, errors.New("invalid IEC104 stream projection")
	}
	selected := iecFrame{Values: map[string]any{}, Quality: map[string]string{}, Details: map[string]any{}, Cause: source.Cause, Received: source.Received}
	for ioa, name := range points {
		key := strconv.FormatUint(uint64(ioa), 10)
		if value, exists := source.Values[key]; exists {
			selected.Values[name] = value
			selected.Quality[name] = source.Quality[key]
			selected.Details[name] = source.Details[key]
		}
	}
	if len(selected.Values) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(selected)
	if err != nil || len(encoded) > 65536 {
		return nil, errors.New("IEC104 projected frame exceeds 64 KiB")
	}
	return encoded, nil
}
