package gossip_test

import (
	"testing"

	"github.com/c6s-zavod/soyuz/crdt"
	"github.com/c6s-zavod/soyuz/gossip"
)

func TestWatermarkCalculationAndDelta(t *testing.T) {
	st1 := crdt.New()
	st2 := crdt.New()
	ns := "/test"

	_ = st1.Set(ns, "k1", "v1")
	st2.Merge(st1.Snapshot())

	wm1 := gossip.ComputeWatermarks(st1.Snapshot())
	wm2 := gossip.ComputeWatermarks(st2.Snapshot())

	if !gossip.WatermarksEqual(wm1, wm2) {
		t.Errorf("expected watermarks for identical stores to be equal")
	}

	delta1 := gossip.ComputeDelta(st1.Snapshot(), wm2)
	if len(delta1.Namespaces) > 0 {
		t.Errorf("expected 0 deltas when watermarks match, got %d namespaces", len(delta1.Namespaces))
	}

	// Mutate st1 with a newer record
	_ = st1.Set(ns, "k2", "v2-new")
	snapshot1New := st1.Snapshot()
	wm1New := gossip.ComputeWatermarks(snapshot1New)

	delta2 := gossip.ComputeDelta(snapshot1New, wm2)
	if len(delta2.Namespaces) == 0 {
		t.Fatalf("expected deltas when st1 has newer records")
	}

	st2.Merge(delta2)
	wm2After := gossip.ComputeWatermarks(st2.Snapshot())

	if !gossip.WatermarksEqual(wm1New, wm2After) {
		t.Errorf("expected watermarks to match after applying delta")
	}
}
