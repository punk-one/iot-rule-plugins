package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func main() {
	sdk.ServeFactory(func() sdk.Plugin { return &ChangeDetectPlugin{} }, sdk.ServeOptions{})
}
