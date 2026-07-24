package peerwire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	defaultHMACSkew   = 5 * time.Second
	reconcileInterval = 2 * time.Second
	dialTimeout       = 500 * time.Millisecond
	// HeaderWorkerID carries the sender's node/worker ID on the handshake.
	HeaderWorkerID = "X-C6S-Worker-ID"
	// defaultWSPath is the WebSocket handshake route the Dialer targets.
	defaultWSPath    = "/peerwire"
	inboundQueueSize = 100
)

// MeshRegistry supplies the set of peers the mesh controller should stay
// connected to. Implementations decode their own record type (worker,
// warehouse, ...) out of the CRDT and return peerID -> dial address, excluding
// the local node and any tombstoned or address-less entries.
type MeshRegistry interface {
	ActiveMembers() map[string]string
}

// Envelope wraps a frame received from a peer.
type Envelope struct {
	PeerID string
	Frame  Frame
}

// Hub manages active peer connections, dials peers, and upgrades incoming requests.
type Hub struct {
	selfID string
	secret string
	wsPath string

	mu       sync.RWMutex
	peers    map[string]*Conn
	dialing  map[string]chan struct{}
	registry MeshRegistry

	inbound chan Envelope
}

// New constructs a new Hub connection manager.
func New(selfID, secret string) *Hub {
	return &Hub{
		selfID:  selfID,
		secret:  secret,
		wsPath:  defaultWSPath,
		peers:   map[string]*Conn{},
		dialing: map[string]chan struct{}{},
		inbound: make(chan Envelope, inboundQueueSize),
	}
}

// SetDialPath overrides the WebSocket handshake route used when dialing peers
// (default "/peerwire"). Both endpoints must serve HandleHTTP on this path.
func (h *Hub) SetDialPath(path string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.wsPath = path
}

// SelfID returns the local node ID.
func (h *Hub) SelfID() string {
	return h.selfID
}

// Inbound returns the receive channel for inbound peer frames.
func (h *Hub) Inbound() <-chan Envelope {
	return h.inbound
}

// SetRegistry binds a mesh registry so the controller can reconcile peers.
func (h *Hub) SetRegistry(r MeshRegistry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.registry = r
}

// StartMeshController launches the background mesh reconciliation loop, which
// dials newly-active members and drops connections to departed ones.
func (h *Hub) StartMeshController(ctx context.Context) {
	go h.meshLoop(ctx)
}

func (h *Hub) meshLoop(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.reconcileMesh(ctx)
		}
	}
}

// reconcileMesh dials newly-active members and prunes departed ones.
func (h *Hub) reconcileMesh(ctx context.Context) {
	h.mu.RLock()
	reg := h.registry
	h.mu.RUnlock()

	if reg == nil {
		return
	}

	members := reg.ActiveMembers()

	h.mu.Lock()
	for id, conn := range h.peers {
		if _, ok := members[id]; !ok {
			slog.Info("Closing connection to departed member", slog.String("peer", id))
			_ = conn.Close()
			delete(h.peers, id)
		}
	}
	h.mu.Unlock()

	for id, addr := range members {
		h.mu.RLock()
		_, connected := h.peers[id]
		h.mu.RUnlock()

		if connected {
			continue
		}

		go func(peerID, peerAddr string) {
			dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()

			if _, err := h.Dial(dialCtx, peerID, peerAddr); err != nil {
				slog.Debug("Failed to mesh-dial peer", slog.String("peer", peerID), slog.Any("error", err))
			}
		}(id, addr)
	}
}

// GenerateAuthToken creates an HMAC token for authentication handshakes.
func (h *Hub) GenerateAuthToken(peerID string, ts time.Time) string {
	mac := hmac.New(sha256.New, []byte(h.secret))
	_, _ = fmt.Fprintf(mac, "%s:%s:%d", h.selfID, peerID, ts.Unix())

	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyAuthToken validates an incoming HMAC handshake token.
func (h *Hub) VerifyAuthToken(remoteID, token string, timestamp int64) bool {
	now := time.Now().Unix()
	skew := now - timestamp
	if skew < 0 {
		skew = -skew
	}
	if skew > int64(defaultHMACSkew.Seconds()) {
		return false
	}

	mac := hmac.New(sha256.New, []byte(h.secret))
	_, _ = fmt.Fprintf(mac, "%s:%s:%d", remoteID, h.selfID, timestamp)
	expected := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(token), []byte(expected))
}

// Dial connects to a remote peer via WebSocket, keyed by the peer's node ID.
// Concurrent dials to the same peer are coalesced. Returns the live connection.
func (h *Hub) Dial(ctx context.Context, peerID, addr string) (*Conn, error) {
	h.mu.Lock()
	if c, ok := h.peers[peerID]; ok {
		h.mu.Unlock()

		return c, nil
	}
	if ch, ok := h.dialing[peerID]; ok {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
		h.mu.RLock()
		c, ok := h.peers[peerID]
		h.mu.RUnlock()
		if ok {
			return c, nil
		}

		return nil, errors.New("dial failed on concurrent attempt")
	}

	ch := make(chan struct{})
	h.dialing[peerID] = ch
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.dialing, peerID)
		close(ch)
		h.mu.Unlock()
	}()

	now := time.Now()
	token := h.GenerateAuthToken(peerID, now)
	url := fmt.Sprintf("ws://%s%s", addr, h.wsPath)

	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			HeaderWorkerID:     []string{h.selfID},
			"X-C6S-Auth-Token": []string{token},
			"X-C6S-Auth-Time":  []string{strconv.FormatInt(now.Unix(), 10)},
		},
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	wsConn, _, err := websocket.Dial(dialCtx, url, opts) //nolint:bodyclose
	if err != nil {
		return nil, fmt.Errorf("websocket dial to %s (%s): %w", peerID, addr, err)
	}

	conn := h.registerConn(peerID, addr, true, wsConn) //nolint:contextcheck
	slog.Info("Successfully established peer connection", slog.String("peer", peerID), slog.String("addr", addr))

	return conn, nil
}

// AddConn registers an already-established peer connection, applying cross-dial
// deduplication just like a freshly dialed or accepted one.
func (h *Hub) AddConn(conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.peers[conn.peerID]; ok {
		if !h.preferIncoming(conn.peerID, old.outbound) {
			_ = conn.Close()

			return
		}
		_ = old.Close()
	}
	h.peers[conn.peerID] = conn
	h.watchClose(conn)
}

// registerConn wraps a websocket into a Conn and registers it with dedup.
func (h *Hub) registerConn(peerID, addr string, outbound bool, wsConn *websocket.Conn) *Conn {
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.peers[peerID]; ok {
		if !h.preferIncoming(peerID, old.outbound) {
			slog.Debug("Dropping redundant peer connection on cross-dial", slog.String("peer", peerID))
			_ = wsConn.Close(websocket.StatusNormalClosure, "redundant connection")

			return old
		}
		_ = old.Close()
	}

	c := NewConn(peerID, addr, outbound, wsConn, h.inbound)
	h.peers[peerID] = c
	h.watchClose(c)

	return c
}

// watchClose removes a peer from the registry once its connection drops.
func (h *Hub) watchClose(c *Conn) {
	go func() {
		<-c.closed
		h.removePeer(c.peerID, c)
	}()
}

// preferIncoming reports whether a newly established connection should replace
// the existing one for the same peer. Both endpoints keep the link dialed by
// the lower-ID node, so a simultaneous cross-dial converges on one connection.
func (h *Hub) preferIncoming(peerID string, oldOutbound bool) bool {
	keepOutbound := h.selfID < peerID

	return oldOutbound != keepOutbound
}

// removePeer removes a connection from the registry if it is still the current one.
func (h *Hub) removePeer(peerID string, c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if current, ok := h.peers[peerID]; ok && current == c {
		delete(h.peers, peerID)
		slog.Info("Peer connection removed", slog.String("peer", peerID))
	}
}

// RemovePeer closes and removes a peer connection by ID.
func (h *Hub) RemovePeer(peerID string) {
	h.mu.Lock()
	conn, ok := h.peers[peerID]
	if ok {
		delete(h.peers, peerID)
	}
	h.mu.Unlock()

	if ok && conn != nil {
		_ = conn.Close()
	}
}

// Conn returns the live connection to a peer, if any.
func (h *Hub) Conn(peerID string) (*Conn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	c, ok := h.peers[peerID]

	return c, ok
}

// Send delivers a frame to a specific peer.
func (h *Hub) Send(ctx context.Context, peerID string, f Frame) error {
	h.mu.RLock()
	conn, ok := h.peers[peerID]
	h.mu.RUnlock()

	if !ok {
		return fmt.Errorf("peer %s not connected", peerID)
	}

	return conn.Send(ctx, f)
}

// Request sends a frame and awaits a response from a peer.
func (h *Hub) Request(ctx context.Context, peerID string, f Frame) (Frame, error) {
	h.mu.RLock()
	conn, ok := h.peers[peerID]
	h.mu.RUnlock()

	if !ok {
		return Frame{}, fmt.Errorf("peer %s not connected", peerID)
	}

	return conn.Request(ctx, f)
}

// ActivePeers returns IDs of connected peers.
func (h *Hub) ActivePeers() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]string, 0, len(h.peers))
	for id := range h.peers {
		out = append(out, id)
	}

	return out
}

// Close shuts down all active peer connections.
func (h *Hub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, conn := range h.peers {
		if conn != nil {
			_ = conn.Close()
		}
		delete(h.peers, id)
	}

	return nil
}

// HandleHTTP handles inbound WebSocket handshake requests.
func (h *Hub) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	remoteID := r.Header.Get(HeaderWorkerID)
	token := r.Header.Get("X-C6S-Auth-Token")
	timeStr := r.Header.Get("X-C6S-Auth-Time")

	if remoteID == "" || token == "" || timeStr == "" {
		http.Error(w, "missing auth headers", http.StatusUnauthorized)

		return
	}

	ts, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil || !h.VerifyAuthToken(remoteID, token, ts) {
		http.Error(w, "invalid auth token", http.StatusUnauthorized)

		return
	}

	wsConn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.Error("Failed to accept websocket connection", slog.Any("error", err))

		return
	}

	h.registerConn(remoteID, r.RemoteAddr, false, wsConn) //nolint:contextcheck
}
