package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func main() {
	sdk.ServeConnector(NewConnector())
}
