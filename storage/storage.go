// Package storage defines the persistence layer interfaces and helpers for key-value stores.
package storage

import (
	"encoding/json"
	"errors"
)

// ErrNotFound is returned when a key is not found in the storage.
var ErrNotFound = errors.New("storage: key not found")

// Storage defines the interface for local persistence of raw bytes across namespaces (buckets).
type Storage interface {
	// Put writes the raw byte value under the specified namespace and key.
	Put(namespace string, key string, value []byte) error

	// Get retrieves the raw byte value for a key in the specified namespace.
	Get(namespace string, key string) ([]byte, error)

	// Delete removes a key from the specified namespace.
	Delete(namespace string, key string) error

	// List returns all raw key-value pairs in the specified namespace.
	List(namespace string) (map[string][]byte, error)

	// Close releases any locks or resources held by the storage engine.
	Close() error
}

// Put marshals value to JSON and writes it.
func Put[T any](s Storage, namespace string, key string, value T) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return s.Put(namespace, key, data)
}

// Get retrieves raw bytes and unmarshals them into T.
//
//nolint:ireturn
func Get[T any](s Storage, namespace string, key string) (T, error) {
	var target T
	data, err := s.Get(namespace, key)
	if err != nil {
		return target, err
	}
	if err := json.Unmarshal(data, &target); err != nil {
		return target, err
	}

	return target, nil
}

// List retrieves all raw key-value pairs and unmarshals them into a map of T.
func List[T any](s Storage, namespace string) (map[string]T, error) {
	raw, err := s.List(namespace)
	if err != nil {
		return nil, err
	}
	res := make(map[string]T, len(raw))
	for k, v := range raw {
		var val T
		if err := json.Unmarshal(v, &val); err != nil {
			return nil, err
		}
		res[k] = val
	}

	return res, nil
}
