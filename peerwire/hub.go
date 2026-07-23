package peerwire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	HeaderWorkerID    = "X-C6S-Worker-ID"
	inboundQueueSize  = 100
)

// Envelope wraps a frame received from a peer.
type Envelope struct {
	PeerID string
	Frame  Frame
}

// Hub manages active peer connections, dials peers, and upgrades incoming requests.
type Hub struct {
	selfID string
	secret string

	mu      sync.RWMutex
	peers   map[string]*Conn
	dialing map[string]chan struct{}

	inbound chan Envelope
}

// New constructs a new Hub connection manager.
func New(selfID, secret string) *Hub {
	return &Hub{
		selfID:  selfID,
		secret:  secret,
		peers:   map[string]*Conn{},
		dialing: map[string]chan struct{}{},
		inbound: make(chan Envelope, inboundQueueSize),
	}
}

// SelfID returns the local node ID.
func (h *Hub) SelfID() string {
	return h.selfID
}

// Inbound returns the receive channel for inbound peer frames.
func (h *Hub) Inbound() <-chan Envelope {
	return h.inbound
}

// GenerateAuthToken creates an HMAC token for authentication handshakes.
func (h *Hub) GenerateAuthToken(peerID string, ts time.Time) string {
	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write([]byte(fmt.Sprintf("%s:%s:%d", h.selfID, peerID, ts.Unix())))

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
	mac.Write([]byte(fmt.Sprintf("%s:%s:%d", remoteID, h.selfID, timestamp)))
	expected := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(token), []byte(expected))
}

// Dial connects to a remote peer address via WebSocket.
func (h *Hub) Dial(ctx context.Context, peerID, addr string) error {
	h.mu.Lock()
	if _, ok := h.peers[peerID]; ok {
		h.mu.Unlock()

		return nil
	}
	if ch, ok := h.dialing[peerID]; ok {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
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
	url := fmt.Sprintf("ws://%s/peerwire", addr)

	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			HeaderWorkerID:       []string{h.selfID},
			"X-C6S-Auth-Token":   []string{token},
			"X-C6S-Auth-Time":    []string{strconv.FormatInt(now.Unix(), 10)},
		},
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	wsConn, _, err := websocket.Dial(dialCtx, url, opts)
	if err != nil {
		return fmt.Errorf("websocket dial to %s (%s): %w", peerID, addr, err)
	}

	conn := NewConn(peerID, addr, true, wsConn, h.inbound)

	h.mu.Lock()
	if existing, ok := h.peers[peerID]; ok {
		_ = existing.Close()
	}
	h.peers[peerID] = conn
	h.mu.Unlock()

	slog.Info("Successfully established peer connection", slog.String("peer", peerID), slog.String("addr", addr))

	return nil
}

// AddConn directly registers an established peer connection.
func (h *Hub) AddConn(conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if existing, ok := h.peers[conn.PeerID()]; ok {
		_ = existing.Close()
	}
	h.peers[conn.PeerID()] = conn
}

// RemovePeer closes and removes a peer connection.
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

	conn := NewConn(remoteID, r.RemoteAddr, false, wsConn, h.inbound)
	h.AddConn(conn)
}
