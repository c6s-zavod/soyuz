package placement_test

import (
	"reflect"
	"testing"

	"github.com/c6s-zavod/soyuz/placement"
)

func TestRendezvousPlacement(t *testing.T) {
	peers := []string{"node-1", "node-2", "node-3", "node-4"}
	key := "block-sha256-abcdef1234567890"

	res1 := placement.SelectReplicas(key, peers, 2)
	res2 := placement.SelectReplicas(key, peers, 2)

	if len(res1) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(res1))
	}

	if !reflect.DeepEqual(res1, res2) {
		t.Errorf("expected deterministic replica selection: %v != %v", res1, res2)
	}
}
