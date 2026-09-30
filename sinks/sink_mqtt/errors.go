package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func mqttSinkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func mqttSinkSkippable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.SkippableError(err)
}

func mqttSinkFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
