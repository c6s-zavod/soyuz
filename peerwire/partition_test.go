package peerwire_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c6s-zavod/soyuz/peerwire"
)

// errSevered is returned by a partitioned transport instead of connecting.
var errSevered = errors.New("link severed by test")

// partitionTransport refuses connections to addresses the test has severed and
// otherwise dials normally.
//
// Severing at the transport rather than the host is what makes an asymmetric
// split expressible: one hub can refuse a peer while that peer still reaches
// back, which is where split-brain lives and what a firewall rule cannot model
// without root.
type partitionTransport struct {
	mu      sync.Mutex
	severed map[string]bool
	base    http.RoundTripper
}

func newPartitionTransport() *partitionTransport {
	return &partitionTransport{
		severed: map[string]bool{},
		base:    http.DefaultTransport,
	}
}

func (p *partitionTransport) sever(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.severed[addr] = true
}

func (p *partitionTransport) heal(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.severed, addr)
}

func (p *partitionTransport) isSevered(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.severed[addr]
}

func (p *partitionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if p.isSevered(r.URL.Host) {
		return nil, errSevered
	}

	return p.base.RoundTrip(r)
}

// hubPair stands up two hubs, with the listener served over HTTP so the dialer
// can reach it. Returns the dialing hub and the listener's address.
func hubPair(t *testing.T) (*peerwire.Hub, string) {
	t.Helper()

	const secret = "shared-secret"

	listener := peerwire.New("node-a", secret)
	dialer := peerwire.New("node-b", secret)

	t.Cleanup(func() {
		_ = listener.Close()
		_ = dialer.Close()
	})

	srv := httptest.NewServer(http.HandlerFunc(listener.HandleHTTP))
	t.Cleanup(srv.Close)

	return dialer, strings.TrimPrefix(srv.URL, "http://")
}

// TestSetDialClient_SeversAndRestoresALink is the property the seam exists for:
// a partition that a test constructs as an ordinary value, with no privileges
// and no change to the host's network.
func TestSetDialClient_SeversAndRestoresALink(t *testing.T) {
	t.Parallel()

	hubB, addr := hubPair(t)

	transport := newPartitionTransport()
	hubB.SetDialClient(&http.Client{Transport: transport})

	transport.sever(addr)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := hubB.Dial(ctx, "node-a", addr); err == nil {
		t.Fatal("dial succeeded across a severed link")
	}

	transport.heal(addr)

	conn, err := hubB.Dial(ctx, "node-a", addr)
	if err != nil {
		t.Fatalf("dial failed after the link was restored: %v", err)
	}

	if conn == nil {
		t.Fatal("restored dial returned no connection")
	}
}

// TestSetDialClient_SeveringIsPerHub verifies the seam is per-hub state, so two
// hubs in one test process can be partitioned independently. A package-level
// override could not express that.
func TestSetDialClient_SeveringIsPerHub(t *testing.T) {
	t.Parallel()

	const secret = "shared-secret"

	listener := peerwire.New("node-a", secret)
	severed := peerwire.New("node-b", secret)
	open := peerwire.New("node-c", secret)

	t.Cleanup(func() {
		_ = listener.Close()
		_ = severed.Close()
		_ = open.Close()
	})

	srv := httptest.NewServer(http.HandlerFunc(listener.HandleHTTP))
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")

	transport := newPartitionTransport()
	transport.sever(addr)
	severed.SetDialClient(&http.Client{Transport: transport})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := severed.Dial(ctx, "node-a", addr); err == nil {
		t.Error("the severed hub reached the listener")
	}

	if _, err := open.Dial(ctx, "node-a", addr); err != nil {
		t.Errorf("an unrelated hub was affected by another hub's partition: %v", err)
	}
}

// TestSetDialClient_NilRestoresDefault covers the documented reset path.
func TestSetDialClient_NilRestoresDefault(t *testing.T) {
	t.Parallel()

	hubB, addr := hubPair(t)

	transport := newPartitionTransport()
	transport.sever(addr)
	hubB.SetDialClient(&http.Client{Transport: transport})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := hubB.Dial(ctx, "node-a", addr); err == nil {
		t.Fatal("dial succeeded across a severed link")
	}

	hubB.SetDialClient(nil)

	if _, err := hubB.Dial(ctx, "node-a", addr); err != nil {
		t.Fatalf("dial failed after restoring the default client: %v", err)
	}
}

// TestDial_UnsetClientUsesLibraryDefault pins that the production path is
// untouched: a hub that never calls SetDialClient dials exactly as before.
func TestDial_UnsetClientUsesLibraryDefault(t *testing.T) {
	t.Parallel()

	hubB, addr := hubPair(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := hubB.Dial(ctx, "node-a", addr); err != nil {
		t.Fatalf("default dial path broke: %v", err)
	}
}

// TestSetDialClient_ConcurrentWithDial guards the field against the race the
// wsPath read previously had: Dial now takes both under the same RLock.
func TestSetDialClient_ConcurrentWithDial(t *testing.T) {
	t.Parallel()

	hubB, addr := hubPair(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 50 {
			hubB.SetDialClient(&http.Client{Transport: newPartitionTransport()})
			hubB.SetDialClient(nil)
		}
	})

	wg.Go(func() {
		for range 50 {
			_, _ = hubB.Dial(ctx, "node-a", addr)
		}
	})

	wg.Wait()
}

// unreachableAddr returns an address nothing is listening on, for tests that
// need a dial to fail without a partition.
func unreachableAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := l.Addr().String()

	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	return addr
}

// TestDial_ReportsPeerAndAddressOnFailure keeps the error useful for an
// operator reading logs, severed or merely unreachable.
func TestDial_ReportsPeerAndAddressOnFailure(t *testing.T) {
	t.Parallel()

	hub := peerwire.New("node-b", "shared-secret")
	t.Cleanup(func() { _ = hub.Close() })

	addr := unreachableAddr(t)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, err := hub.Dial(ctx, "node-a", addr)
	if err == nil {
		t.Fatal("dial to an unreachable address succeeded")
	}

	if !strings.Contains(err.Error(), "node-a") || !strings.Contains(err.Error(), addr) {
		t.Errorf("error names neither the peer nor the address: %v", err)
	}
}
