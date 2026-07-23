package crdt_test

import (
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
