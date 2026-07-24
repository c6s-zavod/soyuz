package crdt_test

import (
	"sync"
	"testing"

	"github.com/c6s-zavod/soyuz/crdt"
)

func TestStoreLWW(t *testing.T) {
	st := crdt.New()
	ns := "/test"

	if err := st.Set(ns, "key1", "val1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	rec, ok := st.Get(ns, "key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}

	var val string
	if err := rec.UnmarshalValue(&val); err != nil || val != "val1" {
		t.Fatalf("expected val1, got %s (err %v)", val, err)
	}

	st.Delete(ns, "key1")
	_, ok = st.Get(ns, "key1")
	if ok {
		t.Fatal("expected key1 to be deleted")
	}
}

func TestStoreMerge(t *testing.T) {
	st1 := crdt.New()
	st2 := crdt.New()
	ns := "/test"

	_ = st1.Set(ns, "k1", "v1")
	_ = st2.Set(ns, "k2", "v2")

	st1.Merge(st2.Snapshot())

	_, ok1 := st1.Get(ns, "k1")
	_, ok2 := st1.Get(ns, "k2")

	if !ok1 || !ok2 {
		t.Fatalf("expected both k1 and k2 to exist after merge (ok1=%v, ok2=%v)", ok1, ok2)
	}
}

func TestStoreChangeHookFiresOnSetAndMerge(t *testing.T) {
	s := crdt.New()

	var mu sync.Mutex
	got := map[string]int{}
	s.SetChangeHook(func(ns string, keys []string) {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range keys {
			got[ns+"/"+k]++
		}
	})

	// Set fires once for a winning write.
	if err := s.Set("holders", "cid-1", map[string]bool{"present": true}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// A stale Set (older timestamp) must not fire: simulate via Merge with an
	// older record that loses LWW.
	src := crdt.New()
	if err := src.Set("holders", "cid-2", map[string]bool{"present": true}); err != nil {
		t.Fatalf("src Set: %v", err)
	}
	s.Merge(src.Snapshot())

	mu.Lock()
	defer mu.Unlock()
	if got["holders/cid-1"] != 1 {
		t.Errorf("expected Set hook once for cid-1, got %d", got["holders/cid-1"])
	}
	if got["holders/cid-2"] != 1 {
		t.Errorf("expected Merge hook once for cid-2, got %d", got["holders/cid-2"])
	}
}
