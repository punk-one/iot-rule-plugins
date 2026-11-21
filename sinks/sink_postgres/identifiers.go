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

func normalizeSQLConflictKeys(input []string) ([]string, error) {
	if len(input) == 0 {
		return nil, nil
	}
	if len(input) > 32 {
		return nil, errors.New("output.conflict_key_columns accepts at most 32 columns")
	}
	keys := make([]string, 0, len(input))
	seen := make(map[string]bool, len(input))
	for _, key := range input {
		if len(key) > maxSQLIdentifierBytes || !sqlIdentifier.MatchString(key) {
			return nil, errors.New("invalid output.conflict_key_columns identifier")
		}
		// The existing INSERT uses unquoted column names; PostgreSQL folds them to lowercase.
		key = strings.ToLower(key)
		if seen[key] {
			return nil, errors.New("duplicate output.conflict_key_columns identifier")
		}
		seen[key] = true
		keys = append(keys, key)
	}
	return keys, nil
}

func normalizeConflictKeyValues(row map[string]interface{}, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	nonNull := make(map[string]string, len(row))
	for name, value := range row {
		if value != nil {
			nonNull[strings.ToLower(name)] = name
		}
	}
	for _, key := range keys {
		name, ok := nonNull[key]
		if !ok {
			return errors.New("event is missing a non-null PostgreSQL conflict key column")
		}
		// A batch union must not emit two unquoted names that PostgreSQL folds
		// to the same conflict column.
		if name != key {
			row[key] = row[name]
			delete(row, name)
		}
	}
	return nil
}
