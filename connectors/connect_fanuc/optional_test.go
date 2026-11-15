//go:build linux && cgo && (amd64 || arm)

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const optionalNative = `
short cnc_rdposition(unsigned short h,short type,short*count,void*out){if(type!=-1||*count!=32)return -1;*count=2;for(int i=0;i<2;i++)for(int j=0;j<4;j++){char*p=(char*)out+i*48+j*12;int32_t value=-12345-i;int16_t dec=3,unit=0,disp=1;memcpy(p,&value,4);memcpy(p+4,&dec,2);memcpy(p+6,&unit,2);memcpy(p+8,&disp,2);p[10]='X'+i;p[11]='1';}return 0;}
short cnc_rdtool(unsigned short h,short index,short*count,void*out){if(index!=7||*count!=1)return -1;int32_t values[4]={42,12,100,88};memcpy(out,values,16);((char*)out)[16]=3;return 0;}
short pmc_rdpmcrng(unsigned short h,short area,short type,unsigned short start,unsigned short end,unsigned short length,void*out){if(area!=9||type!=0||start!=20||end!=20||length!=9)return -1;int16_t header[4]={area,type,start,end};memcpy(out,header,8);((unsigned char*)out)[8]=255;return 0;}
`

func TestOptionalReadOnlyNativeABI(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "fixture.c")
	lib := filepath.Join(dir, "fixture.so")
	if e := os.WriteFile(src, []byte(fakeNative+optionalNative), 0600); e != nil {
		t.Fatal(e)
	}
	if b, e := exec.Command("cc", "-shared", "-fPIC", "-o", lib, src).CombinedOutput(); e != nil {
		t.Fatal(string(b), e)
	}
	n, e := openNative(lib)
	if e != nil {
		t.Fatal(e)
	}
	defer n.close()
	v, e := n.read(7, point{Query: "positions"})
	if e != nil {
		t.Fatal(e)
	}
	a := v.([]any)
	if len(a) != 2 || a[0].(map[string]any)["absolute"].(map[string]any)["value"] != -12.345 {
		t.Fatal(v)
	}
	v, e = n.read(7, point{Query: "tool_management", Number: 7})
	if e != nil || v.(map[string]any)["remaining_life"] != int32(88) {
		t.Fatal(v, e)
	}
	v, e = n.read(7, point{Query: "pmc_byte", PMCArea: 9, Number: 20})
	if e != nil || v != 255 {
		t.Fatal(v, e)
	}
}

func TestPMCReadRequiresExplicitRange(t *testing.T) {
	p := point{Query: "pmc_byte", PMCArea: 9, Number: 20}
	if pmcReadable(config{}, p) {
		t.Fatal("default allowed")
	}
	cfg := config{ReadPMCRanges: []pmcRange{{Area: 9, Start: 10, End: 20}}}
	if !pmcReadable(cfg, p) {
		t.Fatal("allowlisted denied")
	}
	for _, p := range []point{{PMCArea: 8, Number: 20}, {PMCArea: 9, Number: 21}, {PMCArea: 9, Number: -1}} {
		if pmcReadable(cfg, p) {
			t.Fatal("range escaped")
		}
	}
}
