package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestMacroDecimalNoRounding(t *testing.T) {
	for _, tc := range []struct {
		value string
		dec   int
		want  int32
	}{{"12.34", 2, 1234}, {"-0.5", 1, -5}, {"0", 0, 0}} {
		actual, err := macroValue(tc.value, tc.dec)
		if err != nil || actual != tc.want {
			t.Fatal(actual, err)
		}
	}
	for _, v := range []string{"1.234", "1000000000", "invalid", "1/3"} {
		if _, err := macroValue(v, 2); err == nil {
			t.Fatal("unsafe macro accepted")
		}
	}
}
func TestNativeDynamicABINoVendorLibraryRequired(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "fake.c")
	library := filepath.Join(dir, "fake.so")
	if err := os.WriteFile(source, []byte(fakeNative), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("cc", "-shared", "-fPIC", "-o", library, source).CombinedOutput(); err != nil {
		t.Fatalf("fake ABI fixture failed: %s", output)
	}
	native, err := openNative(library)
	if err != nil {
		t.Fatal(err)
	}
	defer native.close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handle, err := native.connect(config{Host: "127.0.0.1", Port: 8193, Path: 1}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer native.free(handle)
	value, err := native.read(handle, point{Query: "feed"})
	if err != nil || value != int32(1234) {
		t.Fatal("FOCAS 32-bit data offset", value, err)
	}
	value, err = native.read(handle, point{Query: "status"})
	if err != nil || value.(map[string]any)["run"] != int32(3) {
		t.Fatal(value, err)
	}
	if err = native.writeMacro(handle, 100, -1234, 2); err != nil {
		t.Fatal(err)
	}
}

const fakeNative = `
#include <stdint.h>
#include <string.h>
short cnc_startupprocess(long a,const char*b){return 0;} short cnc_exitprocess(void){return 0;} short cnc_exitthread(void){return 0;}
short cnc_allclibhndl3(const char*h,unsigned short p,long t,unsigned short*out){*out=7;return 0;}short cnc_freelibhndl(unsigned short h){return 0;}short cnc_setpath(unsigned short h,short p){return 0;}
short cnc_statinfo(unsigned short h,void*out){int16_t values[9]={0,1,2,3,4,5,6,7,8};memcpy(out,values,sizeof(values));return 0;}
short cnc_rdprgnum(unsigned short h,void*out){int16_t values[4]={0,0,12,34};memcpy(out,values,sizeof(values));return 0;}
short cnc_actf(unsigned short h,void*out){int32_t value=1234;memcpy((char*)out+4,&value,4);return 0;}short cnc_acts(unsigned short h,void*out){return cnc_actf(h,out);}short cnc_alarm2(unsigned short h,void*out){int32_t value=5;memcpy(out,&value,4);return 0;}
short cnc_rdmacro(unsigned short h,short num,short length,void*out){int32_t value=-1234;int16_t dec=2;memcpy((char*)out+4,&value,4);memcpy((char*)out+8,&dec,2);return 0;}
short cnc_wrmacro(unsigned short h,short num,short length,long raw,short dec){return num==100&&length==12&&raw==-1234&&dec==2?0:-1;}
`

func TestOperatorInstalledLibraryLoadOnly(t *testing.T) {
	path := os.Getenv("FANUC_NATIVE_TEST_LIBRARY")
	if path == "" {
		t.Skip("operator library not provided; synthetic ABI fixture is the default")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if err = validateLibrary(config{Library: path, SHA256: hex.EncodeToString(sum[:]), ABI: "focas32_compat"}); err != nil {
		t.Fatal(err)
	}
	native, err := openNative(path)
	if err != nil {
		t.Fatal(err)
	}
	native.close()
}
