package main

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	s7WLBit   = 0x01
	s7WLByte  = 0x02
	s7WLWord  = 0x04
	s7WLDWord = 0x06
	s7WLReal  = 0x08
)

type s7DBInfo struct {
	Area       int
	DBNumber   int
	Start      int
	Amount     int
	WordLength int
	Bit        int
}

func s7BufSize(wl, amount int) int {
	switch wl {
	case s7WLBit, s7WLByte:
		return amount
	case s7WLWord:
		return amount * 2
	case s7WLDWord, s7WLReal:
		return amount * 4
	default:
		return amount * 4
	}
}

func s7ParseAddress(raw string) (*s7DBInfo, error) {
	v := strings.ToUpper(strings.ReplaceAll(raw, " ", ""))
	if v == "" {
		return nil, fmt.Errorf("empty address")
	}
	info := &s7DBInfo{Bit: -1, Amount: 1}

	switch {
	case strings.HasPrefix(v, "DB"):
		return s7ParseDB(v)
	case strings.HasPrefix(v, "MX"):
		info.Area, info.WordLength = 0x83, s7WLBit
		return s7ParseBit(v[2:], info)
	case strings.HasPrefix(v, "MD"):
		info.Area, info.WordLength = 0x83, s7WLDWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "MW"):
		info.Area, info.WordLength = 0x83, s7WLWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "MB"):
		info.Area, info.WordLength = 0x83, s7WLByte
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "EX"):
		info.Area, info.WordLength = 0x81, s7WLBit
		return s7ParseBit(v[2:], info)
	case strings.HasPrefix(v, "ED"):
		info.Area, info.WordLength = 0x81, s7WLDWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "EW"):
		info.Area, info.WordLength = 0x81, s7WLWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "EB"):
		info.Area, info.WordLength = 0x81, s7WLByte
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "AX"):
		info.Area, info.WordLength = 0x82, s7WLBit
		return s7ParseBit(v[2:], info)
	case strings.HasPrefix(v, "AD"):
		info.Area, info.WordLength = 0x82, s7WLDWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "AW"):
		info.Area, info.WordLength = 0x82, s7WLWord
		return s7ParseNum(v[2:], info)
	case strings.HasPrefix(v, "AB"):
		info.Area, info.WordLength = 0x82, s7WLByte
		return s7ParseNum(v[2:], info)
	default:
		return nil, fmt.Errorf("unsupported address prefix: %s", v)
	}
}

func s7ParseNum(s string, info *s7DBInfo) (*s7DBInfo, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("invalid address number %q: %w", s, err)
	}
	info.Start = n
	return info, nil
}

func s7ParseBit(s string, info *s7DBInfo) (*s7DBInfo, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("bit address must be byte.bit, got %q", s)
	}
	byteAddr, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid byte addr %q: %w", parts[0], err)
	}
	bitNum, err := strconv.Atoi(parts[1])
	if err != nil || bitNum < 0 || bitNum > 7 {
		return nil, fmt.Errorf("invalid bit number %q", parts[1])
	}
	info.Start = byteAddr
	info.Bit = bitNum
	return info, nil
}

func s7ParseDB(v string) (*s7DBInfo, error) {
	info := &s7DBInfo{Area: 0x84, Bit: -1, Amount: 1}
	dot := strings.Index(v, ".")
	if dot < 0 {
		return nil, fmt.Errorf("DB address missing dot: %s", v)
	}
	dbNum, err := strconv.Atoi(v[2:dot])
	if err != nil {
		return nil, fmt.Errorf("invalid DB number in %s: %w", v, err)
	}
	info.DBNumber = dbNum
	rest := v[dot+1:]
	switch {
	case strings.HasPrefix(rest, "DBX"):
		info.WordLength = s7WLBit
		return s7ParseBit(rest[3:], info)
	case strings.HasPrefix(rest, "DBB"):
		info.WordLength = s7WLByte
		return s7ParseNum(rest[3:], info)
	case strings.HasPrefix(rest, "DBW"):
		info.WordLength = s7WLWord
		return s7ParseNum(rest[3:], info)
	case strings.HasPrefix(rest, "DBD"):
		info.WordLength = s7WLDWord
		return s7ParseNum(rest[3:], info)
	default:
		return nil, fmt.Errorf("unknown DB sub-type in %s", v)
	}
}
