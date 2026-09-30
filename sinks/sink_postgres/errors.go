package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func postgresSinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func postgresSinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
