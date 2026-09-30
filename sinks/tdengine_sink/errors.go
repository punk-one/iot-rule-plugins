package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func tdengineSinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func tdengineSinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
