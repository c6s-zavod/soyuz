// Package placement provides deterministic Rendezvous Hashing (HRW) for replica selection.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

type peerScore struct {
	peerID string
	score  uint64
}

// SelectReplicas selects count peers from activePeers with highest random weight score for key.
// Selection is deterministic across nodes seeing the same activePeers set.
func SelectReplicas(key string, activePeers []string, count int) []string {
	if len(activePeers) == 0 || count <= 0 {
		return []string{}
	}

	scores := make([]peerScore, 0, len(activePeers))

	for _, peer := range activePeers {
		h := sha256.New()
		h.Write([]byte(key))
		h.Write([]byte(peer))
		digest := h.Sum(nil)

		// Convert first 8 bytes of digest to uint64 score
		score := binary.BigEndian.Uint64(digest[:8])
		scores = append(scores, peerScore{
			peerID: peer,
			score:  score,
		})
	}

	// Sort descending by score; tie-break by peerID string comparison
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score != scores[j].score {
			return scores[i].score > scores[j].score
		}

		return scores[i].peerID < scores[j].peerID
	})

	if count > len(scores) {
		count = len(scores)
	}

	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = scores[i].peerID
	}

	return result
}
