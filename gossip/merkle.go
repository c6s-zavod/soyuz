package gossip

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/c6s-zavod/soyuz/crdt"
)

const BucketCount = 256

// DigestMap contains SHA-256 digests for each of the 256 key range buckets.
type DigestMap [BucketCount][32]byte

// ComputeDigestMap calculates bucket digests for all records in the CRDT store payload.
func ComputeDigestMap(payload crdt.Payload) DigestMap {
	var buckets [BucketCount][][]byte

	for ns, items := range payload.Namespaces {
		for key, rec := range items {
			if rec.Tombstone {
				continue
			}

			fullKey := ns + ":" + key
			h := sha256.Sum256([]byte(fullKey))
			bucketIdx := h[0] // Use first byte to place into 0-255 bucket

			// Format record fingerprint: fullKey + timestamp + epoch + value
			tsBytes := make([]byte, 16)
			binary.BigEndian.PutUint64(tsBytes[0:8], uint64(rec.Timestamp))
			binary.BigEndian.PutUint64(tsBytes[8:16], uint64(rec.Epoch))

			entry := append([]byte(fullKey), tsBytes...)
			entry = append(entry, rec.Value...)
			buckets[bucketIdx] = append(buckets[bucketIdx], entry)
		}
	}

	var digests DigestMap
	for i := 0; i < BucketCount; i++ {
		if len(buckets[i]) == 0 {
			continue
		}

		// Sort entries inside bucket for deterministic hashing
		sort.Slice(buckets[i], func(a, b int) bool {
			return string(buckets[i][a]) < string(buckets[i][b])
		})

		h := sha256.New()
		for _, entry := range buckets[i] {
			h.Write(entry)
		}
		copy(digests[i][:], h.Sum(nil))
	}

	return digests
}

// MismatchedBuckets compares local and remote DigestMaps and returns bucket indices that differ.
func MismatchedBuckets(local, remote DigestMap) []uint8 {
	mismatches := make([]uint8, 0)
	for i := 0; i < BucketCount; i++ {
		if local[i] != remote[i] {
			mismatches = append(mismatches, uint8(i))
		}
	}

	return mismatches
}
