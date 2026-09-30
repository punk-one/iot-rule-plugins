package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func redisSinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func redisSinkSkippable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.SkippableError(err)
}

func redisSinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
