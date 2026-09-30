package main

import sdk "github.com/punk-one/iot-rule-plugin-sdk"

func mqttSourceRetryable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.RetryableError(err)
}

func mqttSourceSkippable(err error) error {
	if err == nil {
		return nil
	}
	return sdk.SkippableError(err)
}

func mqttSourceFatal(err error) error {
	if err == nil {
		return nil
	}
	return sdk.FatalError(err)
}
