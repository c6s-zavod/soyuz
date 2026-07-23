package gossip_test

import (
	"testing"

	"github.com/c6s-zavod/soyuz/crdt"
	"github.com/c6s-zavod/soyuz/gossip"
)

func TestMerkleDigestCalculation(t *testing.T) {
	st1 := crdt.New()
	st2 := crdt.New()

	_ = st1.Set("/ns", "k1", "v1")
	st2.Merge(st1.Snapshot())

	d1 := gossip.ComputeDigestMap(st1.Snapshot())
	d2 := gossip.ComputeDigestMap(st2.Snapshot())

	mismatches := gossip.MismatchedBuckets(d1, d2)
	if len(mismatches) != 0 {
		t.Errorf("expected identical stores to have zero mismatched buckets, got %d", len(mismatches))
	}

	_ = st2.Set("/ns", "k2", "v2-different")
	d3 := gossip.ComputeDigestMap(st2.Snapshot())

	mismatchesDiff := gossip.MismatchedBuckets(d1, d3)
	if len(mismatchesDiff) == 0 {
		t.Errorf("expected mismatched buckets when store 2 has additional keys")
	}
}
