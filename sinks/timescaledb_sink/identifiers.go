package main

import (
	"errors"
	"regexp"
	"strings"
)

const maxSQLIdentifierBytes = 63

var sqlIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

func validSQLTableName(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return false
	}
	for _, part := range parts {
		if len(part) > maxSQLIdentifierBytes || !sqlIdentifier.MatchString(part) {
			return false
		}
	}
	return true
}

func validateSQLColumns(row map[string]interface{}) error {
	names := make(map[string]struct{}, len(row))
	for name := range row {
		if len(name) > maxSQLIdentifierBytes || !sqlIdentifier.MatchString(name) {
			return errors.New("invalid SQL column name")
		}
		folded := strings.ToLower(name)
		if _, duplicate := names[folded]; duplicate {
			return errors.New("duplicate SQL column name")
		}
		names[folded] = struct{}{}
	}
	return nil
}
