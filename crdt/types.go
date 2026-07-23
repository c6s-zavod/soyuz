package crdt

import (
	"encoding/json"
	"errors"
)

// Record represents a versioned Last-Write-Wins CRDT entry.
type Record struct {
	Value     json.RawMessage `json:"value,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Epoch     int64           `json:"epoch,omitempty"`
	Tombstone bool            `json:"tombstone,omitempty"`
}

// UnmarshalValue parses the record's Value field into target v.
func (r *Record) UnmarshalValue(v any) error {
	if len(r.Value) == 0 {
		return errors.New("empty record value")
	}

	return json.Unmarshal(r.Value, v)
}

// MarshalValue serializes v into the record's Value field.
func (r *Record) MarshalValue(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	r.Value = raw

	return nil
}

// Payload represents a full CRDT state snapshot across namespaces.
type Payload struct {
	Namespaces map[string]map[string]Record `json:"namespaces"`
}
