// Package bolt implements storage.Storage using bbolt (BoltDB).
package bolt

import (
	"fmt"
	"time"

	"github.com/c6s-zavod/soyuz/storage"
	bbolt "go.etcd.io/bbolt"
)

// Storage implements the storage.Storage interface using BoltDB.
type Storage struct {
	db *bbolt.DB
}

// New opens a BoltDB database at the specified path.
func New(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening bolt database at %s: %w", path, err)
	}

	return &Storage{db: db}, nil
}

// Put writes a raw byte value under the specified namespace (bucket).
func (s *Storage) Put(namespace string, key string, value []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(namespace))
		if err != nil {
			return fmt.Errorf("getting or creating bucket %s: %w", namespace, err)
		}

		if err := bucket.Put([]byte(key), value); err != nil {
			return fmt.Errorf("putting key %s: %w", key, err)
		}

		return nil
	})
}

// Get retrieves a raw byte value for a key in the specified namespace.
func (s *Storage) Get(namespace string, key string) ([]byte, error) {
	var res []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return storage.ErrNotFound
		}

		val := bucket.Get([]byte(key))
		if val == nil {
			return storage.ErrNotFound
		}

		res = make([]byte, len(val))
		copy(res, val)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Delete removes a key from the specified namespace.
func (s *Storage) Delete(namespace string, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return nil
		}

		if err := bucket.Delete([]byte(key)); err != nil {
			return fmt.Errorf("deleting key %s: %w", key, err)
		}

		return nil
	})
}

// List returns all raw key-value pairs in the specified namespace.
func (s *Storage) List(namespace string) (map[string][]byte, error) {
	res := map[string][]byte{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			res[string(k)] = valCopy

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Close closes the underlying BoltDB database.
func (s *Storage) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing bolt database: %w", err)
	}

	return nil
}
