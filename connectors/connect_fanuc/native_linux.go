//go:build linux && cgo && (amd64 || arm)

package main

/*
#cgo LDFLAGS: -ldl
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
// Minimal ABI declarations, no proprietary header or native library is redistributed.
typedef struct {
 void *library;
 short (*startup)(long,const char*);
 short (*shutdown)(void);
 short (*thread_exit)(void);
 short (*alloc)(const char*,unsigned short,long,unsigned short*);
 short (*release)(unsigned short);
 short (*path)(unsigned short,short);
 short (*status)(unsigned short,void*);
 short (*program)(unsigned short,void*);
 short (*feed)(unsigned short,void*);
 short (*spindle)(unsigned short,void*);
 short (*alarm)(unsigned short,void*);
 short (*macro_read)(unsigned short,short,short,void*);
 short (*position)(unsigned short,short,short*,void*);
 short (*tool)(unsigned short,short,short*,void*);
 short (*pmc)(unsigned short,short,short,unsigned short,unsigned short,unsigned short,void*);
 short (*macro_write)(unsigned short,short,short,long,short);
} focas_api;
#define LOAD(field,name) *(void **)(&api->field)=dlsym(api->library,name)
static focas_api* api_open(const char* path) {
 focas_api*api=calloc(1,sizeof(*api));if(!api)return NULL;
 api->library=dlopen(path,RTLD_NOW|RTLD_LOCAL);if(!api->library){free(api);return NULL;}
 LOAD(startup,"cnc_startupprocess");LOAD(shutdown,"cnc_exitprocess");LOAD(thread_exit,"cnc_exitthread");LOAD(alloc,"cnc_allclibhndl3");LOAD(release,"cnc_freelibhndl");LOAD(path,"cnc_setpath");LOAD(status,"cnc_statinfo");LOAD(program,"cnc_rdprgnum");LOAD(feed,"cnc_actf");LOAD(spindle,"cnc_acts");LOAD(alarm,"cnc_alarm2");LOAD(macro_read,"cnc_rdmacro");LOAD(macro_write,"cnc_wrmacro");LOAD(position,"cnc_rdposition");LOAD(tool,"cnc_rdtool");LOAD(pmc,"pmc_rdpmcrng");
 if(!api->startup||!api->shutdown||!api->thread_exit||!api->alloc||!api->release||!api->path||!api->status||!api->program||!api->feed||!api->spindle||!api->alarm||api->startup(0,"/dev/null")!=0){dlclose(api->library);free(api);return NULL;}
 return api;
}
static void api_close(focas_api*a){if(a){a->shutdown();dlclose(a->library);free(a);}}
static short api_connect(focas_api*a,const char*host,unsigned short port,long timeout,short path,unsigned short*out){short rc=a->alloc(host,port,timeout,out);if(rc!=0)return rc;rc=a->path(*out,path);if(rc!=0){a->release(*out);*out=0;}return rc;}
static void api_free(focas_api*a,unsigned short handle){a->release(handle);}
static void api_thread_exit(focas_api*a){a->thread_exit();}
static short api_read(focas_api*a,unsigned short handle,int op,short number,int32_t*out,short*dec){unsigned char buffer[128]={0};short rc=-32760;switch(op){case 1:rc=a->status(handle,buffer);if(rc==0){for(int i=0;i<9;i++){short value;memcpy(&value,buffer+i*2,2);out[i]=value;}}return rc;case 2:rc=a->program(handle,buffer);if(rc==0){unsigned short p,m;memcpy(&p,buffer+4,2);memcpy(&m,buffer+6,2);out[0]=p;out[1]=m;}return rc;case 3:rc=a->feed(handle,buffer);break;case 4:rc=a->spindle(handle,buffer);break;case 5:rc=a->alarm(handle,buffer);if(rc==0)memcpy(out,buffer,4);return rc;case 6:if(!a->macro_read)return -32760;rc=a->macro_read(handle,number,12,buffer);if(rc==0){memcpy(out,buffer+4,4);memcpy(dec,buffer+8,2);}return rc;};if(rc==0)memcpy(out,buffer+4,4);return rc;}
static short api_read_extra(focas_api*a,unsigned short h,int op,short number,short area,unsigned char*out,short*count){
 if(op==1){if(!a->position)return -32760;return a->position(h,-1,count,out);}
 if(op==2){if(!a->tool)return -32760;return a->tool(h,number,count,out);}
 if(op==3){if(!a->pmc)return -32760;return a->pmc(h,area,0,(unsigned short)number,(unsigned short)number,9,out);}
 return -32760;
}
static short api_write_macro(focas_api*a,unsigned short handle,short number,int32_t raw,short decimals){if(!a->macro_write)return -32760;return a->macro_write(handle,number,12,(long)raw,decimals);}
*/
import "C"
import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"
	"unsafe"
)

type nativeLibrary struct{ api *C.focas_api }

func openNative(path string) (*nativeLibrary, error) {
	text := C.CString(path)
	defer C.free(unsafe.Pointer(text))
	api := C.api_open(text)
	if api == nil {
		return nil, errors.New("FOCAS library cannot load required symbols or initialize")
	}
	return &nativeLibrary{api: api}, nil
}
func (n *nativeLibrary) close()             { C.api_close(n.api) }
func (n *nativeLibrary) exitThread()        { C.api_thread_exit(n.api) }
func (n *nativeLibrary) free(handle uint16) { C.api_free(n.api, C.ushort(handle)) }
func (n *nativeLibrary) connect(cfg config, ctx context.Context) (uint16, error) {
	deadline, _ := ctx.Deadline()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, errors.New("deadline before native connect")
	}
	seconds := max(1, int(math.Ceil(remaining.Seconds())))
	host := C.CString(cfg.Host)
	defer C.free(unsafe.Pointer(host))
	var handle C.ushort
	if code := C.api_connect(n.api, host, C.ushort(cfg.Port), C.long(seconds), C.short(cfg.Path), &handle); code != 0 {
		return 0, errors.New("FOCAS connection or path setup failed")
	}
	return uint16(handle), nil
}
func (n *nativeLibrary) read(handle uint16, p point) (any, error) {
	if p.Query == "positions" || p.Query == "tool_management" || p.Query == "pmc_byte" {
		return n.readExtra(handle, p)
	}
	operation := map[string]int{"status": 1, "program": 2, "feed": 3, "spindle": 4, "alarm": 5, "macro": 6}[p.Query]
	var values [9]C.int32_t
	var decimals C.short
	if C.api_read(n.api, C.ushort(handle), C.int(operation), C.short(p.Number), &values[0], &decimals) != 0 {
		return nil, errors.New("native read failed")
	}
	switch p.Query {
	case "status":
		names := []string{"hdck", "tmmode", "aut", "run", "motion", "mstb", "emergency", "alarm", "edit"}
		out := map[string]any{}
		for i, name := range names {
			out[name] = int32(values[i])
		}
		return out, nil
	case "program":
		return map[string]any{"running": int32(values[0]), "main": int32(values[1])}, nil
	case "macro":
		if int32(values[0]) == math.MinInt32 || decimals < -9 || decimals > 9 {
			return nil, errors.New("undefined or unsupported macro precision")
		}
		if decimals < 0 {
			scaled := int64(values[0]) * int64(math.Pow10(-int(decimals)))
			if scaled > 9007199254740991 || scaled < -9007199254740991 {
				return strconv.FormatInt(scaled, 10), nil
			}
		}
		return float64(values[0]) * math.Pow10(-int(decimals)), nil
	default:
		return int32(values[0]), nil
	}
}
func (n *nativeLibrary) writeMacro(handle uint16, number int, raw int32, decimals int) error {
	if C.api_write_macro(n.api, C.ushort(handle), C.short(number), C.int32_t(raw), C.short(decimals)) != 0 {
		return errors.New("native macro write failed")
	}
	return nil
}

// Fixed-width focas32_compat layout; optional symbols never prevent basic reads.
func (n *nativeLibrary) readExtra(handle uint16, p point) (any, error) {
	op := map[string]int{"positions": 1, "tool_management": 2, "pmc_byte": 3}[p.Query]
	var raw [1536]C.uchar
	count := C.short(1)
	if op == 1 {
		count = 32
	}
	if C.api_read_extra(n.api, C.ushort(handle), C.int(op), C.short(p.Number), C.short(p.PMCArea), &raw[0], &count) != 0 {
		return nil, errors.New("optional native read unsupported or failed")
	}
	b := C.GoBytes(unsafe.Pointer(&raw[0]), C.int(len(raw)))
	switch op {
	case 1:
		if count < 1 || count > 32 {
			return nil, errors.New("invalid native axis count")
		}
		axes := make([]any, 0, int(count))
		for i := 0; i < int(count); i++ {
			axis := map[string]any{}
			for j, k := range []string{"absolute", "machine", "relative", "distance"} {
				e := b[i*48+j*12 : i*48+j*12+12]
				dec := int16(binary.LittleEndian.Uint16(e[4:6]))
				if dec < -9 || dec > 9 {
					return nil, errors.New("unsupported native position precision")
				}
				name := e[10]
				if name < 32 || name > 126 {
					return nil, errors.New("invalid native axis name")
				}
				rawValue := int32(binary.LittleEndian.Uint32(e))
				axis[k] = map[string]any{"raw": rawValue, "decimals": dec, "value": float64(rawValue) * math.Pow10(-int(dec)), "unit_code": int16(binary.LittleEndian.Uint16(e[6:8])), "display_flag": int16(binary.LittleEndian.Uint16(e[8:10])), "name": string([]byte{name}), "suffix_code": e[11]}
			}
			axes = append(axes, axis)
		}
		return axes, nil
	case 2:
		if count != 1 {
			return nil, errors.New("invalid native tool count")
		}
		out := map[string]any{"index": p.Number}
		for i, k := range []string{"tool_code", "life_count", "max_life", "remaining_life"} {
			out[k] = int32(binary.LittleEndian.Uint32(b[i*4 : i*4+4]))
		}
		out["life_status"] = b[16]
		out["customer_bits"] = b[17]
		out["tool_info"] = binary.LittleEndian.Uint16(b[18:20])
		out["h_code"] = int16(binary.LittleEndian.Uint16(b[20:22]))
		out["d_code"] = int16(binary.LittleEndian.Uint16(b[22:24]))
		return out, nil
	case 3:
		if int16(binary.LittleEndian.Uint16(b[:2])) != int16(p.PMCArea) || binary.LittleEndian.Uint16(b[2:4]) != 0 || int(binary.LittleEndian.Uint16(b[4:6])) != p.Number || int(binary.LittleEndian.Uint16(b[6:8])) != p.Number {
			return nil, errors.New("native PMC response range mismatch")
		}
		return int(b[8]), nil
	}
	return nil, errors.New("unknown native read")
}
