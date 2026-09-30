//go:build !fanuc || !linux || !amd64

package fanucutil

import "fmt"

func unsupportedPlatformError() error {
	return fmt.Errorf("fanuc FOCAS runtime is only supported on linux/amd64")
}

func AcquireLibrary() error {
	return unsupportedPlatformError()
}

func ReleaseLibrary() error {
	return nil
}

func ConnectWithTimeout(host string, port int, timeoutSec int) (int, error) {
	return 0, unsupportedPlatformError()
}

func WithConnection(host string, port int, timeoutSec int, fn func(handle int) error) error {
	return unsupportedPlatformError()
}

func FreeHandle(handle int) error {
	return unsupportedPlatformError()
}

func ReadCNCID(handle int) (string, error) {
	return "", unsupportedPlatformError()
}

func ReadStatus(handle int) (map[string]int16, error) {
	return nil, unsupportedPlatformError()
}

func ReadAlarm(handle int) (int32, error) {
	return 0, unsupportedPlatformError()
}

func ReadProgram(handle int) (int, string, error) {
	return 0, "", unsupportedPlatformError()
}

func ReadMacro(handle int, number int) (float64, error) {
	return 0, unsupportedPlatformError()
}

func ReadParam(handle int, address int) (int, error) {
	return 0, unsupportedPlatformError()
}

func ReadSpindle(handle int) (SpindleData, error) {
	return SpindleData{}, unsupportedPlatformError()
}

func ReadDiagnose(handle int, number int) (int8, error) {
	return 0, unsupportedPlatformError()
}

func WriteMacro(handle int, number int, value interface{}, decimalPlaces *int) error {
	return unsupportedPlatformError()
}
