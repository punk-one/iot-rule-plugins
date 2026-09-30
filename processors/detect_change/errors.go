package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func detectChangeFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}

func detectChangeRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}
