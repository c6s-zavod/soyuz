# SDD Spec: Injectable Peerwire Transport Dialer

## Metadata
* **Status:** `COMPLETED`
* **Author:** Claude
* **Created:** 2026-08-28
* **Last Updated:** 2026-08-28
* **Approver:** Codefather

---

## Phase 1: Proposal

### 1.1 Problem Statement

`Hub.Dial` builds its WebSocket with the library's default HTTP client ([hub.go](../../peerwire/hub.go)). There is no seam through which a caller can supply its own, and consequently **no way to sever a link between two hubs from inside a test**.

Everything this library exists to provide is defined by what happens when the mesh stops being fully connected: anti-entropy gossip converging after a split, epoch watermarks ordering writes from a partitioned peer, the CRDT merge that consumers build distributed locking on top of. None of it can be tested here, because a test cannot construct the condition. `soyuz/gossip` sits at 27.7% coverage for that reason — the reachable paths are the ones where every peer answers.

The consumer this blocks is `c6s`, whose headline guarantees are all partition behaviour: majority-quorum locking, self-fencing on quorum loss, epoch fencing of a deposed holder's writes. Those are advertised and unverified. A recent split-brain bug in its lock — a voter granting two candidates the same epoch — sat in every release until it was found by reading, precisely because no test could produce the conditions that expose it.

The alternative to a dialler seam is manipulating the host's firewall from a test: needs root, cannot run on a developer workstation, cannot run in ordinary CI, and severs links far more coarsely than the scenarios require.

**Cost of doing nothing:** the properties that justify this library stay unverified, and every consumer inherits the same blind spot.

### 1.2 Proposed Solution

Give `Hub` an optional `*http.Client` used for outbound dials, defaulting to today's behaviour when unset. A test supplies a client whose transport it controls and can then refuse, delay, or fail connections to chosen peers.

The seam is deliberately the extension point the underlying library already offers: `websocket.DialOptions` accepts an `HTTPClient`, so this exposes a parameter that is taken anyway. No new interface, no wrapper type, no change to the frame protocol or the handshake.

A partition becomes an ordinary Go value a test constructs, rather than a state of the host machine.

### 1.3 Scope & Requirements

* **In Scope:**
  * An optional per-`Hub` HTTP client applied to outbound WebSocket dials.
  * Unchanged behaviour when unset — the current default client.
  * Tests covering a link severed and restored, and the resulting gossip convergence.
* **Out of Scope:**
  * Any change to the peerwire frame protocol, the HMAC handshake, or `Conn` multiplexing.
  * Inbound connection control. `HandleHTTP` is mounted on a caller-supplied mux; a consumer that wants to reject inbound peers already can.
  * A general fault-injection framework. This is one field justified by one testable property.
  * Production use of the seam — proxying, custom TLS, pooling policy. If a real need appears it gets its own spec; shipping it as a supported production knob now would commit us to semantics nothing requires.
  * Changes in `c6s`. The harness consuming this is c6s spec 021.

---

## Phase 2: System Design

### 2.1 Interface

```go
// SetDialClient overrides the HTTP client used for outbound peer dials.
//
// Intended for tests: supplying a client whose transport refuses selected
// peers is how a partition is constructed without touching the host's network.
// Passing nil restores the default client.
func (h *Hub) SetDialClient(c *http.Client)
```

One field on `Hub`, guarded by the existing mutex, read under `RLock` in `Dial` alongside `wsPath` — which already establishes the pattern for a dial-time setting.

`websocket.DialOptions.HTTPClient` is left nil when unset, which is exactly what the library sees today. The unset path is byte-identical to current behaviour, not merely equivalent.

### 2.2 Why a Client and Not a Dialer Function

`websocket.Dial` accepts an `*http.Client`, not a dial function. Taking a client means the seam is the library's own parameter passed through, so there is no adapter to keep correct. A test that wants connection-level control supplies a client with a custom `Transport`; one that wants request-level control supplies one with a custom `RoundTripper`. Both are standard library shapes.

### 2.3 Concurrency

`Dial` already coalesces concurrent dials to the same peer through `h.dialing`. Reading the client under the same lock that guards `wsPath` adds no new ordering. The client itself is safe for concurrent use, as `http.Client` documents.

### 2.4 Alternatives Considered

| Alternative | Why rejected |
| :--- | :--- |
| A `Dialer` interface on `Hub` | A new abstraction wrapping a parameter the underlying library already accepts, with an adapter to keep correct |
| Package-level variable | Global state; two hubs in one test process could not be partitioned independently, which is the whole point |
| Build-tagged test-only field | The seam would not exist in the binary consumers run, so the tested code path would differ from the shipped one |
| Firewall manipulation in tests | Needs root, cannot run on a workstation or ordinary CI, and cannot produce an asymmetric split |

---

## Phase 3: Implementation Plan

### 3.1 Task Breakdown

- [x] **Task 1:** The seam
  - **Files:** `peerwire/hub.go`
  - **Detail:** `dialClient` field, `SetDialClient`, read under `RLock` in `Dial` and passed as `DialOptions.HTTPClient`.
  - **Verification:** `go build ./... && go vet ./...`

- [x] **Task 2:** Partition tests
  - **Files:** `peerwire/partition_test.go` (new)
  - **Detail:** Two hubs over `httptest` servers; a client whose transport refuses a chosen peer. Assert a dial fails while severed, succeeds once restored, and that the default path is unaffected.
  - **Verification:** `go test -race ./peerwire/`

- [x] **Task 3:** Documentation
  - **Files:** `peerwire` doc comments
  - **Detail:** State that the seam is test infrastructure, not a production tuning knob.
  - **Verification:** Manual read-through.

**Final gate:** `make fmt && make lint && make test`.

### 3.2 Risks & Mitigation

| Risk | Detection | Mitigation |
| :--- | :--- | :--- |
| The seam is adopted as a production knob | Consumers setting it outside tests | Doc comment states the intent; no config path exposes it |
| Behaviour drift when unset | Existing peerwire tests | Unset leaves `HTTPClient` nil, which is what the library receives today |
| A test client leaks between hubs | Cross-test interference | Per-`Hub` field, never package-level |

---

## Phase 4: Execution & Verification

### Progress Log

| Task | Result |
| :--- | :--- |
| 1 — The seam | `dialClient` field and `SetDialClient` on `Hub`; `Dial` passes it as `DialOptions.HTTPClient`. |
| 2 — Partition tests | `peerwire/partition_test.go`: sever and restore, per-hub isolation, nil reset, the unset production path, and concurrent set/dial under `-race`. |
| 3 — Documentation | Doc comment states the seam is test infrastructure and not a production knob. |

### Deviation from the Plan

**A latent data race was fixed alongside.** `Dial` read `h.wsPath` without holding the lock that `SetDialPath` writes under, so a concurrent path change raced the read. Adding the client meant taking a read lock at that point anyway; both fields are now read together under `RLock`. Not part of this spec's purpose, but not sensible to add a second unsynchronised field beside the first.

### Verification

- [x] `gofmt -s -w .`, `go vet ./...`, `golangci-lint run ./...` — 0 issues.
- [x] `go test -race ./...` — all packages pass, no races.
- [x] Consumers still build: `c6s` (23 packages, tests pass) and `s3d`.
- [x] Approved by the Codefather.

### Coverage

`peerwire` 55.1% → 57.2%.

---

## Phase 5: Completed

- [x] All Phase 4 items complete.
- [x] No regressions in either consumer.
- [x] Spec reflects the implementation.
- [x] `spec/README.md` updated to `COMPLETED`.
- [x] Approved by the Codefather.

**Consumer:** c6s spec 021 Tier 1, the multi-node harness this seam exists to make possible. The gossip and watermark scenarios that motivated it — `soyuz/gossip` at 27.7% because no test could split the mesh — remain to be written here and are the natural follow-on.
