package main

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type connection interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

func validateSerial(cfg connectionConfig) (string, uint64, error) {
	if cfg.DataBits != 8 && cfg.Mode == "rtu" || cfg.DataBits != 7 && cfg.DataBits != 8 || cfg.StopBits < 1 || cfg.StopBits > 2 || (cfg.Parity != "none" && cfg.Parity != "even" && cfg.Parity != "odd") {
		return "", 0, errors.New("invalid serial line settings")
	}
	if _, ok := baudRates[cfg.Baud]; !ok {
		return "", 0, errors.New("unsupported baud")
	}
	path, err := filepath.EvalSymlinks(cfg.Device)
	if err != nil || !strings.HasPrefix(path, "/dev/") {
		return "", 0, errors.New("serial device must resolve beneath /dev")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return "", 0, errors.New("serial path must be character device")
	}
	var stat unix.Stat_t
	if unix.Stat(path, &stat) != nil {
		return "", 0, errors.New("cannot identify serial bus")
	}
	return path, uint64(stat.Rdev), nil
}
func dialDevice(ctx context.Context, cfg connectionConfig) (connection, error) {
	if cfg.Mode == "rtu" || cfg.Mode == "ascii" {
		return dialSerial(ctx, cfg)
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
}

var baudRates = map[int]uint32{1200: unix.B1200, 2400: unix.B2400, 4800: unix.B4800, 9600: unix.B9600, 19200: unix.B19200, 38400: unix.B38400, 57600: unix.B57600, 115200: unix.B115200, 230400: unix.B230400}

type serialConnection struct {
	*os.File
	rawFD int
	once  sync.Once
	err   error
}

func (f *serialConnection) Close() error {
	f.once.Do(func() { unix.IoctlSetInt(f.rawFD, unix.TIOCNXCL, 0); f.err = f.File.Close() })
	return f.err
}
func dialSerial(ctx context.Context, cfg connectionConfig) (connection, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	fd, err := unix.Open(cfg.Device, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	exclusive := false
	fail := func(err error) (connection, error) {
		if exclusive {
			unix.IoctlSetInt(fd, unix.TIOCNXCL, 0)
		}
		unix.Close(fd)
		return nil, err
	}
	var identity unix.Stat_t
	if unix.Fstat(fd, &identity) != nil || identity.Mode&unix.S_IFMT != unix.S_IFCHR || (cfg.serialIdentity != 0 && uint64(identity.Rdev) != cfg.serialIdentity) {
		return fail(errors.New("serial device identity changed or is not a character device"))
	}

	if err = unix.IoctlSetInt(fd, unix.TIOCEXCL, 0); err != nil {
		return fail(err)
	}
	exclusive = true
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return fail(err)
	}
	termios.Iflag = unix.IGNPAR
	termios.Oflag = 0
	termios.Lflag = 0
	termios.Cflag = unix.CLOCAL | unix.CREAD | baudRates[cfg.Baud]
	if cfg.DataBits == 7 {
		termios.Cflag |= unix.CS7
	} else {
		termios.Cflag |= unix.CS8
	}
	if cfg.Parity != "none" {
		termios.Cflag |= unix.PARENB
		if cfg.Parity == "odd" {
			termios.Cflag |= unix.PARODD
		}
		termios.Iflag |= unix.INPCK
	}
	if cfg.StopBits == 2 {
		termios.Cflag |= unix.CSTOPB
	}
	termios.Ispeed = baudRates[cfg.Baud]
	termios.Ospeed = baudRates[cfg.Baud]
	termios.Cc[unix.VMIN] = 1
	termios.Cc[unix.VTIME] = 0
	if err = unix.IoctlSetTermios(fd, unix.TCSETS, termios); err != nil {
		return fail(err)
	}
	if err = unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIFLUSH); err != nil {
		return fail(err)
	}
	file := os.NewFile(uintptr(fd), cfg.Device)
	if file == nil {
		return fail(fmt.Errorf("cannot create serial file"))
	}
	return &serialConnection{File: file, rawFD: fd}, nil
}
