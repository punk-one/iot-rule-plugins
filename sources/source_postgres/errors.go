package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func postgresSourceRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func postgresSourceFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
