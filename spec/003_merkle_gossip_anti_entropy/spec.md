# SDD Spec 003: 256-Bucket Merkle Digest Anti-Entropy Gossip

## Metadata
* **Status:** `COMPLETED`
* **Author:** Antigravity (Consigliere)
* **Created:** 2026-07-23
* **Last Updated:** 2026-07-23
* **Approver:** Codefather

---

## Phase 1: Proposal (Rough Idea)

### 1.1 Problem Statement
Nodes must synchronize CRDT state updates periodically without transmitting full state data across the network when no changes have occurred.

### 1.2 Proposed Solution
Implement package `soyuz/gossip`:
- Flat 256-bucket key space partitioning (`ComputeDigestMap`).
- SHA-256 bucket digest calculation for fast range comparison.
- `Gossiper` runner loop driving background anti-entropy rounds against all active peerwire hub peers.

### 1.3 Scope & Requirements
* **In Scope:**
  * SHA-256 bucket partitioning and digest computation.
  * `MismatchedBuckets` comparison between local and remote digest maps.
  * Gossip message dispatch (`MsgGossipPush`) over `peerwire.Hub`.
* **Out of Scope:**
  * Hierarchical Merkle trees (deferred to future scaling revision).

---

## Phase 2: System Design (SDD)

### 2.1 Architecture & Components

```mermaid
graph TD
    Gossiper[soyuz/gossip.Gossiper] -->|ComputeDigestMap| Merkle[Merkle Engine: 256 Buckets]
    Gossiper -->|MsgGossipPush| Hub[soyuz/peerwire.Hub]
    Hub -->|Peer Frame| Peer[Remote Peer Gossiper]
    Peer -->|Merge| RemoteStore[Remote CRDT Store]
```

### 2.2 Data Structures & Interfaces

```go
const BucketCount = 256

type DigestMap [BucketCount][32]byte

type Gossiper struct {
    store    *crdt.Store
    hub      *peerwire.Hub
    selfID   string
    interval time.Duration
}
```

---

## Phase 3: Implementation Plan (IP)

### 3.1 Task Breakdown
- [x] **Task 1:** Build Merkle digest calculation (`ComputeDigestMap`) and bucket comparison (`MismatchedBuckets`).
  - **Files:** `gossip/merkle.go`
  - **Verification:** `GOWORK=off go test -v ./gossip`
- [x] **Task 2:** Build `Gossiper` round execution and payload handler.
  - **Files:** `gossip/gossip.go`, `gossip/gossip_test.go`
  - **Verification:** `GOWORK=off go test -v ./gossip`

---

## Phase 4: Execution & Verification
- [x] All per-task verification steps pass.
- [x] Linter / vet clean.
- [x] Unit tests pass.

---

## Phase 5: Completed
- [x] All Phase 4 items `[x]`.
- [x] Spec document reflects actual implementation.
- [x] `spec/README.md` updated to `COMPLETED`.
- [x] Approved by the User.
