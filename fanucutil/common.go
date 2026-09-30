package fanucutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	DefaultPort       = 8193
	DefaultTimeoutSec = 5

	EWOK       = 0
	EWReset    = -2
	EWHandle   = -8
	EWSocket   = -16
	EWProtocol = -17
)

type FocasError struct {
	code    int
	message string
}

func (e *FocasError) Error() string {
	return fmt.Sprintf("%s (%d)", e.message, e.code)
}

func (e *FocasError) Code() int {
	return e.code
}

func NewFocasError(code int, message string) *FocasError {
	return &FocasError{code: code, message: message}
}

func ExtractCode(err error) int {
	if err == nil {
		return EWOK
	}
	type coder interface {
		Code() int
	}
	var codeErr coder
	if errors.As(err, &codeErr) {
		return codeErr.Code()
	}
	return EWOK
}

func IsFatalConnectionError(code int) bool {
	switch code {
	case EWReset, EWHandle, EWSocket, EWProtocol:
		return true
	default:
		return false
	}
}

type SpindleData struct {
	SpindleSpeed int32
	AxisFeedrate int32
	SPGear       int16
	SPLoad       int32
}

func NormalizePort(port int) int {
	if port <= 0 {
		return DefaultPort
	}
	return port
}

func NormalizeTimeout(timeoutSec int) int {
	if timeoutSec <= 0 {
		return DefaultTimeoutSec
	}
	return timeoutSec
}

func EncodeMacroValue(value interface{}, decimalPlaces *int) (int64, int16, error) {
	number, err := normalizeNumericString(value)
	if err != nil {
		return 0, 0, err
	}

	if decimalPlaces != nil {
		if *decimalPlaces < 0 || *decimalPlaces > 127 {
			return 0, 0, fmt.Errorf("decimal_places out of range: %d", *decimalPlaces)
		}
		floatValue, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse numeric value %q: %w", number, err)
		}
		scale := math.Pow10(*decimalPlaces)
		rawValue := int64(math.Round(floatValue * scale))
		if err := validateMacroRawValue(rawValue); err != nil {
			return 0, 0, err
		}
		return rawValue, int16(*decimalPlaces), nil
	}

	sign := int64(1)
	if strings.HasPrefix(number, "-") {
		sign = -1
		number = strings.TrimPrefix(number, "-")
	} else if strings.HasPrefix(number, "+") {
		number = strings.TrimPrefix(number, "+")
	}

	if strings.ContainsAny(number, "eE") {
		floatValue, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse numeric value %q: %w", number, err)
		}
		number = strconv.FormatFloat(floatValue, 'f', -1, 64)
		if strings.HasPrefix(number, "-") {
			sign = -1
			number = strings.TrimPrefix(number, "-")
		}
	}

	parts := strings.SplitN(number, ".", 2)
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = strings.TrimRight(parts[1], "0")
	}
	if intPart == "" {
		intPart = "0"
	}

	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return 0, 0, nil
	}

	decimals := len(fracPart)
	for len(digits) > 9 && strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		decimals--
	}
	if len(digits) > 9 {
		return 0, 0, fmt.Errorf("value %q exceeds FANUC macro precision", number)
	}

	rawValue, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse scaled numeric value %q: %w", digits, err)
	}
	rawValue *= sign
	if err := validateMacroRawValue(rawValue); err != nil {
		return 0, 0, err
	}
	return rawValue, int16(decimals), nil
}

func DeduplicateInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(values))
	result := make([]int, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func scaledValue(value float64, decimalPlaces int) float64 {
	if decimalPlaces >= 0 {
		return value / math.Pow10(decimalPlaces)
	}
	return value * math.Pow10(-decimalPlaces)
}

func normalizeNumericString(value interface{}) (string, error) {
	switch typed := value.(type) {
	case json.Number:
		return typed.String(), nil
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return "", fmt.Errorf("empty numeric string")
		}
		if _, err := strconv.ParseFloat(trimmed, 64); err != nil {
			return "", fmt.Errorf("unsupported numeric string %q: %w", trimmed, err)
		}
		return trimmed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return "", fmt.Errorf("unsupported float value %v", typed)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), nil
	case int:
		return strconv.FormatInt(int64(typed), 10), nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return strconv.FormatUint(typed, 10), nil
	default:
		return "", fmt.Errorf("unsupported value type %T", value)
	}
}

func validateMacroRawValue(value int64) error {
	if value > 999999999 || value < -999999999 {
		return fmt.Errorf("scaled macro value %d exceeds FANUC range", value)
	}
	return nil
}
