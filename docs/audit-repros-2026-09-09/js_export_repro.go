//go:build ignore

// Run from processors/javascript_transform:
// go run ../../docs/audit-repros-2026-09-09/js_export_repro.go
// Bounded diagnostic: getter deliberately runs for 100 ms; no infinite loop or I/O.
package main

import (
	"fmt"
	"github.com/dop251/goja"
	"time"
)

func main() {
	vm := goja.New()
	program, err := goja.Compile("audit.js", "(function(){return {payload:{get value(){var until=Date.now()+100; while(Date.now()<until){}; return 42;}}};})()", true)
	if err != nil {
		panic(err)
	}
	timer := time.AfterFunc(5*time.Millisecond, func() { vm.Interrupt("timeout") })
	value, err := vm.RunProgram(program)
	stopped := timer.Stop()
	if !stopped {
		vm.ClearInterrupt()
	}
	if err != nil {
		panic(err)
	}
	start := time.Now()
	_ = value.Export()
	fmt.Printf("execution_timer_stopped_before_export=%v export_elapsed_ms=%d configured_timeout_ms=5\n", stopped, time.Since(start).Milliseconds())
	vm = goja.New()
	value, err = vm.RunString("({payload:{get value(){throw new Error('audit getter');}}})")
	if err != nil {
		panic(err)
	}
	func() {
		defer func() { fmt.Printf("export_throw_panics=%v\n", recover() != nil) }()
		_ = value.Export()
	}()
}
