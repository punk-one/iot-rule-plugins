package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func s7SourceRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func s7SourceFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
