package bolt

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/c6s-zavod/soyuz/storage"
)

func TestBoltStorage(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	st, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to open Bolt storage: %v", err)
	}
	defer st.Close()

	ns := "/test"
	key := "key1"
	val := []byte("value1")

	// Get missing key
	_, err = st.Get(ns, key)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Put key
	if err := st.Put(ns, key, val); err != nil {
		t.Fatalf("failed to Put key: %v", err)
	}

	// Get key
	got, err := st.Get(ns, key)
	if err != nil {
		t.Fatalf("failed to Get key: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %q, got %q", val, got)
	}

	// List keys
	list, err := st.List(ns)
	if err != nil {
		t.Fatalf("failed to List keys: %v", err)
	}
	if len(list) != 1 || !bytes.Equal(list[key], val) {
		t.Fatalf("expected 1 item matching key1, got %v", list)
	}

	// Delete key
	if err := st.Delete(ns, key); err != nil {
		t.Fatalf("failed to Delete key: %v", err)
	}

	// Get after delete
	_, err = st.Get(ns, key)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
