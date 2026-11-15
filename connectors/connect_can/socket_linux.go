//go:build linux

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"strconv"
	"sync"
	"time"
)

type rawDevice struct {
	fd     int
	index  int
	mu     sync.RWMutex
	closed bool
	drops  uint32
}

func openDevice(cfg config) (canDevice, error) {
	iface, err := net.InterfaceByName(cfg.Interface)
	if err != nil || iface.Index <= 0 {
		return nil, errors.New("CAN interface unavailable")
	}
	if cfg.FD && iface.MTU < 72 {
		return nil, errors.New("CAN FD requires an interface MTU>=72")
	}
	fd, err := unix.Socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		return nil, errors.New("SocketCAN unavailable")
	}
	ok := false
	defer func() {
		if !ok {
			unix.Close(fd)
		}
	}()
	filters := []unix.CanFilter{}
	for _, spec := range cfg.ReadIDs {
		id, _ := parseID(spec.ID, spec.Extended)
		mask := sffMask | effFlag
		if spec.Extended {
			id |= effFlag
			mask = effMask | effFlag
		}
		filters = append(filters, unix.CanFilter{Id: id, Mask: mask})
	}
	if err = unix.SetsockoptCanRawFilter(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_FILTER, filters); err != nil {
		return nil, errors.New("CAN filters unavailable")
	}
	if cfg.FD {
		if unix.SetsockoptInt(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_FD_FRAMES, 1) != nil {
			return nil, errors.New("CAN FD unsupported by kernel")
		}
	}
	own := 0
	if cfg.ReceiveOwn {
		own = 1
	}
	if unix.SetsockoptInt(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_RECV_OWN_MSGS, own) != nil {
		return nil, errors.New("CAN own-message policy unavailable")
	}
	if cfg.IncludeErrors {
		if unix.SetsockoptInt(fd, unix.SOL_CAN_RAW, unix.CAN_RAW_ERR_FILTER, int(effMask)) != nil {
			return nil, errors.New("CAN error filtering unavailable")
		}
	}
	if unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RXQ_OVFL, 1) != nil {
		return nil, errors.New("CAN receive overflow accounting unavailable")
	}
	if unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 65536) != nil || unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 16384) != nil {
		return nil, errors.New("CAN socket buffer bounds unavailable")
	}
	if err = unix.Bind(fd, &unix.SockaddrCAN{Ifindex: iface.Index}); err != nil {
		return nil, errors.New("CAN interface cannot be bound")
	}
	ok = true
	return &rawDevice{fd: fd, index: iface.Index}, nil
}
func (d *rawDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return unix.Close(d.fd)
}
func (d *rawDevice) Read(ctx context.Context) (canFrame, error) {
	for {
		if ctx.Err() != nil {
			return canFrame{}, ctx.Err()
		}
		d.mu.RLock()
		if d.closed {
			d.mu.RUnlock()
			return canFrame{}, errors.New("CAN socket closed")
		}
		fds := []unix.PollFd{{Fd: int32(d.fd), Events: unix.POLLIN}}
		_, err := unix.Poll(fds, 50)
		if err == unix.EINTR {
			d.mu.RUnlock()
			continue
		}
		if err != nil {
			d.mu.RUnlock()
			return canFrame{}, errors.New("CAN receive poll failed")
		}
		if fds[0].Revents&unix.POLLIN == 0 {
			bad := fds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0
			d.mu.RUnlock()
			if bad {
				return canFrame{}, errors.New("CAN interface/session failed")
			}
			continue
		}
		packet := make([]byte, 72)
		oob := make([]byte, 64)
		n, on, flags, _, err := unix.Recvmsg(d.fd, packet, oob, unix.MSG_DONTWAIT)
		d.mu.RUnlock()
		if err == unix.EAGAIN || err == unix.EINTR {
			continue
		}
		if err != nil || flags&unix.MSG_TRUNC != 0 {
			return canFrame{}, errors.New("CAN packet read failed or truncated")
		}
		frame, err := decodeFrame(packet[:n])
		if err != nil {
			return canFrame{}, err
		}
		messages, err := unix.ParseSocketControlMessage(oob[:on])
		if err != nil {
			return canFrame{}, errors.New("CAN ancillary data invalid")
		}
		for _, m := range messages {
			if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SO_RXQ_OVFL && len(m.Data) >= 4 {
				total := binary.NativeEndian.Uint32(m.Data)
				frame.dropDelta = total - d.drops
				d.drops = total
			}
		}
		return frame, nil
	}
}
func (d *rawDevice) Write(ctx context.Context, frame canFrame) error {
	raw, err := encodeFrame(frame)
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.mu.RLock()
		if d.closed {
			d.mu.RUnlock()
			return errors.New("CAN socket closed")
		}
		fds := []unix.PollFd{{Fd: int32(d.fd), Events: unix.POLLOUT}}
		_, err := unix.Poll(fds, 50)
		if err == unix.EINTR {
			d.mu.RUnlock()
			continue
		}
		if err != nil {
			d.mu.RUnlock()
			return errors.New("CAN send poll failed")
		}
		if fds[0].Revents&unix.POLLOUT == 0 {
			bad := fds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0
			d.mu.RUnlock()
			if bad {
				return errors.New("CAN interface cannot send")
			}
			continue
		}
		if ctx.Err() != nil {
			d.mu.RUnlock()
			return ctx.Err()
		}
		if frame.expiresAt > 0 && time.Now().UnixMilli() >= frame.expiresAt {
			d.mu.RUnlock()
			return errors.New("CAN command expired before socket acceptance")
		}
		n, err := unix.Write(d.fd, raw)
		d.mu.RUnlock()
		if err == unix.EAGAIN || err == unix.EINTR {
			continue
		}
		if err != nil || n != len(raw) {
			return errors.New("CAN socket write failed; outcome unknown")
		}
		return nil
	}
}

func (d *rawDevice) Identity() string { return strconv.Itoa(d.index) }
