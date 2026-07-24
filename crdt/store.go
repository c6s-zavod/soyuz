// Package crdt implements a thread-safe Last-Write-Wins (LWW) key-value store.
package crdt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Store is a thread-safe LWW (Last-Write-Wins) CRDT key/value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]map[string]Record

	// changeHook, if set, is invoked (outside the lock) after Set or Merge with
	// the namespace and the keys whose value actually changed (won LWW).
	changeHook func(namespace string, keys []string)
}

// New constructs an empty in-memory CRDT store.
func New() *Store {
	return &Store{
		data: make(map[string]map[string]Record),
	}
}

// SetChangeHook registers a callback invoked after Set/Merge with the keys whose
// value won LWW and changed. It fires for both local writes and anti-entropy
// merges, so a subscriber sees every record that enters the store regardless of
// path. The hook runs outside the store lock; it must not block indefinitely.
func (s *Store) SetChangeHook(fn func(namespace string, keys []string)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.changeHook = fn
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

	ns, ok := s.data[namespace]
	if !ok {
		ns = make(map[string]Record)
		s.data[namespace] = ns
	}

	existing, found := ns[key]
	changed := !found || isLWWWinner(rec, existing)
	if changed {
		ns[key] = rec
	}
	hook := s.changeHook
	s.mu.Unlock()

	if changed && hook != nil {
		hook(namespace, []string{key})
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
		maps.Copy(itemsMap, items)
		nsMap[ns] = itemsMap
	}

	return Payload{Namespaces: nsMap}
}

// Merge merges an incoming snapshot into the local store using LWW rule.
func (s *Store) Merge(p Payload) {
	s.mu.Lock()

	var changed map[string][]string

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
				if changed == nil {
					changed = make(map[string][]string)
				}
				changed[nsName] = append(changed[nsName], k)
			}
		}
	}

	hook := s.changeHook
	s.mu.Unlock()

	if hook != nil {
		for nsName, keys := range changed {
			hook(nsName, keys)
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
