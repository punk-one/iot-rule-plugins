package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func timescaleSinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func timescaleSinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
