package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func redisSourceRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func redisSourceSkippable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.SkippableError(err)
}

func redisSourceFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
