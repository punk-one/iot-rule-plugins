//go:build fanuc && linux && amd64

package fanucutil

/*
#cgo CFLAGS: -I${SRCDIR}/../fanuc_libs
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../fanuc_libs -Wl,-rpath,${SRCDIR}/../fanuc_libs -l:libfwlib32-linux-x64.so.1.0.5

#include <stdlib.h>
#include "fwlib32.h"

static short fanuc_max_spindles() { return MAX_SPINDLE; }
*/
import "C"

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

var libraryState struct {
	mu      sync.Mutex
	refs    int
	started bool
}

func AcquireLibrary() error {
	libraryState.mu.Lock()
	defer libraryState.mu.Unlock()

	if libraryState.refs > 0 {
		libraryState.refs++
		return nil
	}

	logFile := C.CString("fanuc-focas.log")
	defer C.free(unsafe.Pointer(logFile))

	ret := C.cnc_startupprocess(C.long(0), logFile)
	if ret != C.EW_OK {
		return NewFocasError(int(ret), "cnc_startupprocess failed")
	}

	libraryState.started = true
	libraryState.refs = 1
	return nil
}

func ReleaseLibrary() error {
	libraryState.mu.Lock()
	defer libraryState.mu.Unlock()

	if libraryState.refs == 0 {
		return nil
	}
	libraryState.refs--
	if libraryState.refs > 0 || !libraryState.started {
		return nil
	}

	ret := C.cnc_exitprocess()
	libraryState.started = false
	if ret != C.EW_OK {
		return NewFocasError(int(ret), "cnc_exitprocess failed")
	}
	return nil
}

func ConnectWithTimeout(host string, port int, timeoutSec int) (int, error) {
	timeoutSec = NormalizeTimeout(timeoutSec)
	port = NormalizePort(port)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec+2)*time.Second)
	defer cancel()

	type result struct {
		handle int
		err    error
	}
	resultCh := make(chan result, 1)

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		handle, err := connectOnCurrentThread(host, port, timeoutSec)
		select {
		case resultCh <- result{handle: handle, err: err}:
		case <-ctx.Done():
			if handle != 0 {
				_ = FreeHandle(handle)
			}
		}
	}()

	select {
	case res := <-resultCh:
		return res.handle, res.err
	case <-ctx.Done():
		return 0, fmt.Errorf("connect %s:%d timeout after %ds", host, port, timeoutSec+2)
	}
}

func WithConnection(host string, port int, timeoutSec int, fn func(handle int) error) error {
	timeoutSec = NormalizeTimeout(timeoutSec)
	port = NormalizePort(port)

	doneCh := make(chan error, 1)
	goTimeout := time.Duration(timeoutSec+2) * time.Second

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		var sessionErr error
		defer func() {
			if ret := C.cnc_exitthread(); ret != C.EW_OK && int(ret) != EWHandle && sessionErr == nil {
				sessionErr = NewFocasError(int(ret), "cnc_exitthread failed")
			}
			doneCh <- sessionErr
		}()

		handle, err := connectOnCurrentThread(host, port, timeoutSec)
		if err != nil {
			sessionErr = err
			return
		}
		defer func() {
			if releaseErr := FreeHandle(handle); releaseErr != nil && sessionErr == nil {
				sessionErr = releaseErr
			}
		}()

		sessionErr = fn(handle)
	}()

	timer := time.NewTimer(goTimeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case err := <-doneCh:
		return err
	case <-timer.C:
		return fmt.Errorf("connect %s:%d timeout after %ds", host, port, timeoutSec+2)
	}
}

func connectOnCurrentThread(host string, port int, timeoutSec int) (int, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var handle C.ushort
	ret := C.cnc_allclibhndl3(cHost, C.ushort(port), C.long(timeoutSec), &handle)
	if ret != C.EW_OK {
		return 0, NewFocasError(int(ret), "cnc_allclibhndl3 failed")
	}
	return int(handle), nil
}

func FreeHandle(handle int) error {
	if handle == 0 {
		return nil
	}
	ret := C.cnc_freelibhndl(C.ushort(handle))
	if ret != C.EW_OK && int(ret) != EWHandle {
		return NewFocasError(int(ret), "cnc_freelibhndl failed")
	}
	return nil
}

func ReadCNCID(handle int) (string, error) {
	var cncIDs [4]C.ulong
	ret := C.cnc_rdcncid(C.ushort(handle), (*C.ulong)(&cncIDs[0]))
	if ret != C.EW_OK {
		return "", NewFocasError(int(ret), "cnc_rdcncid failed")
	}
	return fmt.Sprintf("%08x-%08x-%08x-%08x",
		uint32(cncIDs[0]), uint32(cncIDs[1]), uint32(cncIDs[2]), uint32(cncIDs[3])), nil
}

func ReadStatus(handle int) (map[string]int16, error) {
	var status C.ODBST
	ret := C.cnc_statinfo(C.ushort(handle), &status)
	if ret != C.EW_OK {
		return nil, NewFocasError(int(ret), "cnc_statinfo failed")
	}
	return map[string]int16{
		"hdck":      int16(status.hdck),
		"tmmode":    int16(status.tmmode),
		"aut":       int16(status.aut),
		"run":       int16(status.run),
		"motion":    int16(status.motion),
		"mstb":      int16(status.mstb),
		"emergency": int16(status.emergency),
		"alarm":     int16(status.alarm),
		"edit":      int16(status.edit),
	}, nil
}

func ReadAlarm(handle int) (int32, error) {
	var alarm C.long
	ret := C.cnc_alarm2(C.ushort(handle), &alarm)
	if ret != C.EW_OK {
		return 0, NewFocasError(int(ret), "cnc_alarm2 failed")
	}
	return int32(alarm), nil
}

func ReadProgram(handle int) (int, string, error) {
	var program C.ODBPRO
	ret := C.cnc_rdprgnum(C.ushort(handle), &program)
	if ret != C.EW_OK {
		return 0, "", NewFocasError(int(ret), "cnc_rdprgnum failed")
	}

	var running C.ODBEXEPRG
	ret = C.cnc_exeprgname(C.ushort(handle), &running)
	if ret != C.EW_OK {
		return int(program.data), "", NewFocasError(int(ret), "cnc_exeprgname failed")
	}

	return int(program.data), C.GoString(&running.name[0]), nil
}

func ReadMacro(handle int, number int) (float64, error) {
	var macro C.ODBM
	ret := C.cnc_rdmacro(C.ushort(handle), C.short(number), C.short(unsafe.Sizeof(macro)), &macro)
	if ret != C.EW_OK {
		return 0, NewFocasError(int(ret), "cnc_rdmacro failed")
	}

	if macro.mcr_val == 0 && macro.dec_val == -1 {
		return 0, nil
	}
	return scaledValue(float64(macro.mcr_val), int(macro.dec_val)), nil
}

func ReadParam(handle int, address int) (int, error) {
	var buf C.IODBPSD
	ret := C.cnc_rdparam(C.ushort(handle), C.short(address), C.short(-1), C.short(unsafe.Sizeof(buf)), &buf)
	if ret != C.EW_OK {
		return 0, NewFocasError(int(ret), "cnc_rdparam failed")
	}

	if buf._type == 0 {
		raw := (*[8]byte)(unsafe.Pointer(&buf))
		value := int(raw[4]) | int(raw[5])<<8 | int(raw[6])<<16 | int(raw[7])<<24
		return value, nil
	}

	realParam := (*C.REALPRM)(unsafe.Pointer(&buf.u[0]))
	return int(math.Round(scaledValue(float64(realParam.prm_val), int(realParam.dec_val)))), nil
}

func ReadSpindle(handle int) (SpindleData, error) {
	var result SpindleData

	var spindle C.ODBACT
	ret := C.cnc_acts(C.ushort(handle), &spindle)
	if ret != C.EW_OK {
		return result, NewFocasError(int(ret), "cnc_acts failed")
	}
	result.SpindleSpeed = int32(spindle.data)

	var feed C.ODBACT
	ret = C.cnc_actf(C.ushort(handle), &feed)
	if ret != C.EW_OK {
		return result, NewFocasError(int(ret), "cnc_actf failed")
	}
	result.AxisFeedrate = int32(feed.data)

	var gear C.ODBSPN
	ret = C.cnc_rdspgear(C.ushort(handle), C.short(-1), &gear)
	if ret != C.EW_OK {
		return result, NewFocasError(int(ret), "cnc_rdspgear failed")
	}
	result.SPGear = int16(gear.data[0])

	count := C.fanuc_max_spindles()
	if count <= 0 {
		return result, nil
	}
	spindles := make([]C.ODBSPLOAD, int(count))
	ret = C.cnc_rdspmeter(C.ushort(handle), C.short(0), &count, (*C.ODBSPLOAD)(unsafe.Pointer(&spindles[0])))
	if ret != C.EW_OK {
		return result, NewFocasError(int(ret), "cnc_rdspmeter failed")
	}
	result.SPLoad = int32(spindles[0].spload.data)

	return result, nil
}

func ReadDiagnose(handle int, number int) (int8, error) {
	var diagnose C.ODBDGN
	ret := C.cnc_diagnoss(C.ushort(handle), C.short(number), C.short(1), C.short(unsafe.Sizeof(diagnose)), (*C.ODBDGN)(unsafe.Pointer(&diagnose)))
	if ret != C.EW_OK {
		return 0, NewFocasError(int(ret), "cnc_diagnoss failed")
	}

	if diagnose._type == 1 {
		buf := (*[8]byte)(unsafe.Pointer(&diagnose))
		return int8(buf[4]), nil
	}

	realParam := (*C.REALPRM)(unsafe.Pointer(&diagnose.u[0]))
	return int8(math.Round(scaledValue(float64(realParam.prm_val), int(realParam.dec_val)))), nil
}

func WriteMacro(handle int, number int, value interface{}, decimalPlaces *int) error {
	rawValue, decimals, err := EncodeMacroValue(value, decimalPlaces)
	if err != nil {
		return err
	}

	ret := C.cnc_wrmacro(
		C.ushort(handle),
		C.short(number),
		C.short(unsafe.Sizeof(C.ODBM{})),
		C.long(rawValue),
		C.short(decimals),
	)
	if ret != C.EW_OK {
		return NewFocasError(int(ret), "cnc_wrmacro failed")
	}
	return nil
}
