package peerwire

import "testing"

func TestPreferIncomingConverges(t *testing.T) {
	// On a simultaneous cross-dial both endpoints must agree on a single
	// surviving link: the one dialed by the lower-ID node.
	const low, high = "node-a", "node-z"

	lowHub := New(low, "s")
	highHub := New(high, "s")

	// Lower-ID node keeps its outbound link, drops a competing inbound upgrade.
	if lowHub.preferIncoming(high, true /*old outbound*/) {
		t.Error("lower-ID node should keep its outbound link, not replace it with inbound")
	}
	if !lowHub.preferIncoming(high, false /*old inbound*/) {
		t.Error("lower-ID node should replace a stale inbound with its outbound")
	}

	// Higher-ID node keeps the inbound link (dialed by the lower-ID peer).
	if highHub.preferIncoming(low, false /*old inbound*/) {
		t.Error("higher-ID node should keep the inbound link, not replace it with outbound")
	}
	if !highHub.preferIncoming(low, true /*old outbound*/) {
		t.Error("higher-ID node should replace a stale outbound with the inbound")
	}

	// The pair must not both keep the same direction: exactly one side keeps
	// its outbound link, otherwise the mesh would flap.
	lowKeepsOutbound := !lowHub.preferIncoming(high, true)
	highKeepsOutbound := !highHub.preferIncoming(low, true)
	if lowKeepsOutbound == highKeepsOutbound {
		t.Error("both endpoints resolved to the same direction; mesh would flap")
	}
}
