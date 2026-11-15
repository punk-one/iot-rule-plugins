//go:build !linux

package main

import "errors"

func openDevice(config) (canDevice, error) { return nil, errors.New("SocketCAN requires Linux") }
