package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type fieldOperation struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	From  string          `json:"from,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`

	targetParts []string
	sourceParts []string
	literal     any
}

func compileOperations(operations []fieldOperation) error {
	if len(operations) > 128 {
		return fmt.Errorf("process.operations supports at most 128 edits")
	}
	for index := range operations {
		if err := operations[index].compile(); err != nil {
			return fmt.Errorf("process.operations[%d]: %w", index, err)
		}
	}
	return nil
}

func editParts(path string) ([]string, error) {
	parts := strings.Split(path, ".")
	if len(path) > 256 || len(parts) > 16 {
		return nil, fmt.Errorf("edit path exceeds 256 bytes or 16 segments")
	}
	for _, part := range parts {
		if part == "" || strings.TrimSpace(part) != part || strings.ContainsAny(part, "\r\n\x00") {
			return nil, fmt.Errorf("edit path contains an empty or invalid object key")
		}
	}
	return parts, nil
}

func (op *fieldOperation) compile() error {
	var err error
	op.targetParts, err = editParts(op.Path)
	if err != nil {
		return err
	}
	switch op.Op {
	case "set":
		if op.From != "" || len(op.Value) == 0 {
			return fmt.Errorf("set requires value and does not take from")
		}
		decoder := json.NewDecoder(bytes.NewReader(op.Value))
		decoder.UseNumber()
		return decoder.Decode(&op.literal)
	case "delete":
		if op.From != "" || len(op.Value) > 0 {
			return fmt.Errorf("delete accepts only a path")
		}
	case "move":
		if len(op.Value) > 0 {
			return fmt.Errorf("move accepts from and path, without value")
		}
		op.sourceParts, err = editParts(op.From)
		if err != nil {
			return err
		}
		if op.From == op.Path || strings.HasPrefix(op.Path, op.From+".") || strings.HasPrefix(op.From, op.Path+".") {
			return fmt.Errorf("move paths must not overlap")
		}
	default:
		return fmt.Errorf("unknown edit operation %q", op.Op)
	}
	return nil
}

func editParent(payload map[string]any, parts []string, create bool) (map[string]any, error) {
	parent := payload
	for _, part := range parts[:len(parts)-1] {
		value, exists := parent[part]
		if !exists {
			if !create {
				return nil, nil
			}
			child := make(map[string]any)
			parent[part] = child
			parent = child
			continue
		}
		child, ok := value.(map[string]any)
		if !ok || child == nil {
			return nil, fmt.Errorf("cannot traverse non-object key %q", part)
		}
		parent = child
	}
	return parent, nil
}

func (op fieldOperation) apply(payload map[string]any) error {
	targetKey := op.targetParts[len(op.targetParts)-1]
	switch op.Op {
	case "set":
		target, err := editParent(payload, op.targetParts, true)
		if err != nil {
			return err
		}
		// Each event needs its own copy of mutable literal values.
		target[targetKey] = (sdk.Event{Payload: map[string]any{"value": op.literal}}).Clone().Payload["value"]
	case "delete":
		target, err := editParent(payload, op.targetParts, false)
		if err != nil {
			return err
		}
		delete(target, targetKey)
	case "move":
		source, err := editParent(payload, op.sourceParts, false)
		if err != nil {
			return err
		}
		sourceKey := op.sourceParts[len(op.sourceParts)-1]
		value, exists := source[sourceKey]
		if !exists {
			return nil
		}
		target, err := editParent(payload, op.targetParts, true)
		if err != nil {
			return err
		}
		target[targetKey] = value
		delete(source, sourceKey)
	}
	return nil
}
