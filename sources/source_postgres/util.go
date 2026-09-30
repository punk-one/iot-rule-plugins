package main

import (
	"strconv"
	"strings"
	"time"
)

func resolveRowTime(value interface{}) time.Time {
	switch typed := value.(type) {
	case nil:
		return time.Time{}
	case time.Time:
		return typed
	case string:
		candidate := strings.TrimSpace(typed)
		if candidate == "" {
			return time.Time{}
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05",
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, candidate); err == nil {
				return parsed
			}
		}
		if unixValue, err := strconv.ParseInt(candidate, 10, 64); err == nil {
			return time.Unix(unixValue, 0)
		}
	case int64:
		return time.Unix(typed, 0)
	case int32:
		return time.Unix(int64(typed), 0)
	case int:
		return time.Unix(int64(typed), 0)
	case float64:
		return time.Unix(int64(typed), 0)
	case float32:
		return time.Unix(int64(typed), 0)
	}
	return time.Time{}
}
