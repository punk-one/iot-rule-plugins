package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"
)

type deliveryHeader struct {
	Version     int    `json:"version"`
	ID          string `json:"id"`
	Origin      string `json:"origin_instance"`
	Rule        string `json:"rule_id"`
	Buffer      string `json:"buffer_node_id"`
	Record      string `json:"record_id"`
	Event       string `json:"event_id"`
	KeyID       string `json:"key_id"`
	PayloadHash string `json:"payload_sha256"`
	IssuedAt    int64  `json:"issued_at"`
	Signature   string `json:"signature,omitempty"`
}
type envelope struct {
	TransportEventID string         `json:"event_id,omitempty"`
	Delivery         deliveryHeader `json:"delivery"`
	Payload          map[string]any `json:"payload"`
}
type receipt struct {
	Delivery    deliveryHeader `json:"delivery"`
	Committer   string         `json:"committer"`
	CommittedAt int64          `json:"committed_at"`
	Status      string         `json:"status"`
	Signature   string         `json:"signature,omitempty"`
}

func sign(secret, purpose string, value any) string {
	raw, _ := json.Marshal(value)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(purpose))
	mac.Write([]byte{0})
	mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}
func equalSignature(actual, expected string) bool {
	a, err := hex.DecodeString(actual)
	if err != nil {
		return false
	}
	b, _ := hex.DecodeString(expected)
	return hmac.Equal(a, b)
}
func identity(h deliveryHeader) string {
	hash := sha256.Sum256([]byte(h.Origin + "\x00" + h.Rule + "\x00" + h.Buffer + "\x00" + h.Record))
	return hex.EncodeToString(hash[:])
}
func boundedID(v string) bool { return v != "" && len(v) <= 256 && !strings.ContainsAny(v, "\r\n\x00") }
func validHeader(h deliveryHeader, key, origin string, now int64, maxAge int64) bool {
	return h.Version == 1 && boundedID(h.Origin) && h.Origin == origin && boundedID(h.Rule) && boundedID(h.Buffer) && boundedID(h.Record) && boundedID(h.Event) && h.KeyID == key && h.IssuedAt > 0 && h.IssuedAt <= now+30000 && now-h.IssuedAt <= maxAge && len(h.PayloadHash) == 64 && h.ID == identity(h)
}
func normalize(v any, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("payload depth exceeds 64")
	}
	switch value := v.(type) {
	case json.Number:
		n, err := value.Float64()
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || math.Abs(n) > 9007199254740991 {
			return nil, errors.New("signed payload numbers must be finite and within safe precision; encode large integers as strings")
		}
		if n == 0 {
			n = 0
		}
		return n, nil
	case []any:
		if len(value) > 4096 {
			return nil, errors.New("payload array limit")
		}
		for i, v := range value {
			converted, err := normalize(v, depth+1)
			if err != nil {
				return nil, err
			}
			value[i] = converted
		}
		return value, nil
	case map[string]any:
		if len(value) > 4096 {
			return nil, errors.New("payload object limit")
		}
		for k, v := range value {
			converted, err := normalize(v, depth+1)
			if err != nil {
				return nil, err
			}
			value[k] = converted
		}
		return value, nil
	default:
		return v, nil
	}
}
func canonicalPayload(payload map[string]any) (map[string]any, string, error) {
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > 1048576 {
		return nil, "", errors.New("signed payload exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return nil, "", errors.New("invalid payload JSON")
	}
	value, err = normalize(value, 0)
	if err != nil {
		return nil, "", err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, "", errors.New("payload object required")
	}
	raw, err = json.Marshal(object)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(raw)
	return object, hex.EncodeToString(hash[:]), nil
}
func verifyEnvelope(e envelope, secret, key, origin string, maxAge int64) error {
	h := e.Delivery
	if !validHeader(h, key, origin, time.Now().UnixMilli(), maxAge) {
		return errors.New("delivery scope/identity/age invalid")
	}
	actual := h.Signature
	h.Signature = ""
	if !equalSignature(actual, sign(secret, "delivery-v1", h)) {
		return errors.New("delivery signature invalid")
	}
	_, digest, err := canonicalPayload(e.Payload)
	if err != nil || digest != h.PayloadHash {
		return errors.New("delivery payload hash mismatch")
	}
	return nil
}
func verifyReceipt(r receipt, secret, key, origin, committer string, maxAge int64) error {
	now := time.Now().UnixMilli()
	if !validHeader(r.Delivery, key, origin, now, maxAge) || r.Committer != committer || r.Status != "committed" || r.CommittedAt <= 0 || r.CommittedAt > now+30000 || now-r.CommittedAt > maxAge || r.CommittedAt < r.Delivery.IssuedAt-30000 {
		return errors.New("commit receipt scope/status/age invalid")
	}
	header := r.Delivery
	original := header.Signature
	header.Signature = ""
	if !equalSignature(original, sign(secret, "delivery-v1", header)) {
		return errors.New("receipt delivery header signature invalid")
	}
	signature := r.Signature
	r.Signature = ""
	if !equalSignature(signature, sign(secret, "commit-receipt-v1", r)) {
		return errors.New("commit receipt signature invalid")
	}
	return nil
}
func decodeObject(payload map[string]any, target any) error {
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > 2097152 {
		return errors.New("envelope exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return errors.New("invalid versioned delivery envelope")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("trailing envelope JSON")
	}
	return nil
}
func secretValid(secret string) bool { return len(secret) >= 32 && len(secret) <= 512 }
