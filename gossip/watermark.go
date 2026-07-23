package gossip

import (
	"github.com/c6s-zavod/soyuz/crdt"
)

// WatermarkEntry holds the highest (Epoch, Timestamp) seen for a namespace.
type WatermarkEntry struct {
	Epoch     int64 `json:"epoch"`
	Timestamp int64 `json:"timestamp"`
}

// WatermarkMap maps namespace name to its highest (Epoch, Timestamp) watermark.
type WatermarkMap map[string]WatermarkEntry

// ComputeWatermarks scans a CRDT payload and returns the highest (Epoch, Timestamp) per namespace.
func ComputeWatermarks(payload crdt.Payload) WatermarkMap {
	wm := make(WatermarkMap, len(payload.Namespaces))

	for ns, items := range payload.Namespaces {
		var maxEpoch, maxTS int64
		for _, rec := range items {
			if rec.Epoch > maxEpoch {
				maxEpoch = rec.Epoch
				maxTS = rec.Timestamp
			} else if rec.Epoch == maxEpoch && rec.Timestamp > maxTS {
				maxTS = rec.Timestamp
			}
		}
		wm[ns] = WatermarkEntry{
			Epoch:     maxEpoch,
			Timestamp: maxTS,
		}
	}

	return wm
}

// ComputeDelta returns a payload containing only records strictly newer than remoteWatermarks.
func ComputeDelta(payload crdt.Payload, remoteWM WatermarkMap) crdt.Payload {
	deltaMap := make(map[string]map[string]crdt.Record)

	for ns, items := range payload.Namespaces {
		remoteEntry, exists := remoteWM[ns]
		deltaItems := make(map[string]crdt.Record)

		for key, rec := range items {
			if !exists || isNewerThanWatermark(rec, remoteEntry) {
				deltaItems[key] = rec
			}
		}

		if len(deltaItems) > 0 {
			deltaMap[ns] = deltaItems
		}
	}

	return crdt.Payload{Namespaces: deltaMap}
}

// isNewerThanWatermark checks if a record beats the given watermark threshold.
func isNewerThanWatermark(rec crdt.Record, wm WatermarkEntry) bool {
	if rec.Epoch != wm.Epoch {
		return rec.Epoch > wm.Epoch
	}

	return rec.Timestamp > wm.Timestamp
}

// WatermarksEqual checks if two WatermarkMaps are identical.
func WatermarksEqual(wm1, wm2 WatermarkMap) bool {
	if len(wm1) != len(wm2) {
		return false
	}
	for ns, e1 := range wm1 {
		e2, ok := wm2[ns]
		if !ok || e1 != e2 {
			return false
		}
	}

	return true
}
