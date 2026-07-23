package crdt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Store is a thread-safe LWW (Last-Write-Wins) CRDT key/value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]map[string]Record
}

// New constructs an empty in-memory CRDT store.
func New() *Store {
	return &Store{
		data: make(map[string]map[string]Record),
	}
}

// Set inserts or updates a key in the given namespace using LWW logic.
func (s *Store) Set(namespace, key string, value any) error {
	var raw json.RawMessage
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshaling crdt value: %w", err)
		}
		raw = b
	}

	rec := Record{
		Value:     raw,
		Timestamp: time.Now().UnixNano(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ns, ok := s.data[namespace]
	if !ok {
		ns = make(map[string]Record)
		s.data[namespace] = ns
	}

	existing, found := ns[key]
	if !found || isLWWWinner(rec, existing) {
		ns[key] = rec
	}

	return nil
}

// Get fetches a key from the given namespace if it exists and is not tombstoned.
func (s *Store) Get(namespace, key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ns, ok := s.data[namespace]
	if !ok {
		return Record{}, false
	}

	rec, ok := ns[key]
	if !ok || rec.Tombstone {
		return Record{}, false
	}

	return rec, true
}

// Delete writes a tombstone for key in the given namespace.
func (s *Store) Delete(namespace, key string) {
	rec := Record{
		Timestamp: time.Now().UnixNano(),
		Tombstone: true,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ns, ok := s.data[namespace]
	if !ok {
		ns = make(map[string]Record)
		s.data[namespace] = ns
	}

	existing, found := ns[key]
	if !found || isLWWWinner(rec, existing) {
		ns[key] = rec
	}
}

// List returns all non-tombstoned key/record pairs in namespace.
func (s *Store) List(namespace string) map[string]Record {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]Record)
	ns, ok := s.data[namespace]
	if !ok {
		return out
	}

	for k, rec := range ns {
		if !rec.Tombstone {
			out[k] = rec
		}
	}

	return out
}

// Snapshot returns a copy of all namespaces and records for serialization/gossip.
func (s *Store) Snapshot() Payload {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nsMap := make(map[string]map[string]Record, len(s.data))
	for ns, items := range s.data {
		itemsMap := make(map[string]Record, len(items))
		for k, rec := range items {
			itemsMap[k] = rec
		}
		nsMap[ns] = itemsMap
	}

	return Payload{Namespaces: nsMap}
}

// Merge merges an incoming snapshot into the local store using LWW rule.
func (s *Store) Merge(p Payload) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for nsName, items := range p.Namespaces {
		ns, ok := s.data[nsName]
		if !ok {
			ns = make(map[string]Record, len(items))
			s.data[nsName] = ns
		}

		for k, incoming := range items {
			existing, found := ns[k]
			if !found || isLWWWinner(incoming, existing) {
				ns[k] = incoming
			}
		}
	}
}

// isLWWWinner reports whether incoming beats existing under Last-Write-Wins rules.
func isLWWWinner(incoming, existing Record) bool {
	if incoming.Epoch != existing.Epoch {
		return incoming.Epoch > existing.Epoch
	}
	if incoming.Timestamp != existing.Timestamp {
		return incoming.Timestamp > existing.Timestamp
	}

	// Tie-break: content byte comparison (deterministic)
	return bytes.Compare(incoming.Value, existing.Value) > 0
}

// Namespaces returns a list of active namespace names in the store.
func (s *Store) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}
