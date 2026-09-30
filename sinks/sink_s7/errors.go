package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func s7SinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func s7SinkSkippable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.SkippableError(err)
}

func s7SinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
