package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func timescaleSourceRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func timescaleSourceFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
