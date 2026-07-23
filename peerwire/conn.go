package peerwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Default timeouts for peerwire communication.
const (
	DefaultPeerTimeout = 5 * time.Second
	pingTimeout        = 500 * time.Millisecond
	pingInterval       = 5 * time.Second
)

// Conn wraps a single peer's WebSocket connection and manages multiplexed frames.
type Conn struct {
	peerID   string
	addr     string
	wsConn   *websocket.Conn
	inbound  chan<- Envelope
	outbound bool

	writeMu sync.Mutex

	reqMu     sync.Mutex
	pending   map[uint32]chan Frame
	nextReqID uint32

	closeOnce sync.Once
	closed    chan struct{}
}

// NewConn wraps an active websocket connection. outbound reports whether this
// node dialed the peer (true) or accepted an inbound upgrade (false).
func NewConn(peerID, addr string, outbound bool, wsConn *websocket.Conn, inbound chan<- Envelope) *Conn {
	c := &Conn{
		peerID:    peerID,
		addr:      addr,
		wsConn:    wsConn,
		inbound:   inbound,
		outbound:  outbound,
		pending:   map[uint32]chan Frame{},
		nextReqID: 1,
		closed:    make(chan struct{}),
	}

	go c.readLoop()
	go c.pingLoop()

	return c
}

// PeerID returns the peer's identifier.
func (c *Conn) PeerID() string {
	return c.peerID
}

// pingLoop probes the peer for connection liveness.
func (c *Conn) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			_, err := c.Request(ctx, Frame{Type: MsgPing})
			cancel()

			if err != nil {
				slog.Warn("Peer ping failed; closing connection", slog.String("peer", c.peerID), slog.Any("error", err))
				_ = c.Close()

				return
			}
		}
	}
}

// Send writes a frame asynchronously without waiting for a response.
func (c *Conn) Send(ctx context.Context, f Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	w, err := c.wsConn.Writer(ctx, websocket.MessageBinary)
	if err != nil {
		return fmt.Errorf("getting message writer: %w", err)
	}
	defer w.Close()

	if err := WriteFrame(w, f); err != nil {
		return fmt.Errorf("writing frame: %w", err)
	}

	return nil
}

// Request sends a request frame and synchronously blocks waiting for the response.
func (c *Conn) Request(ctx context.Context, f Frame) (Frame, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultPeerTimeout)
	defer cancel()

	reqID := atomic.AddUint32(&c.nextReqID, 1)
	f.RequestID = reqID

	ch := make(chan Frame, 1)

	c.reqMu.Lock()
	c.pending[reqID] = ch
	c.reqMu.Unlock()

	defer func() {
		c.reqMu.Lock()
		delete(c.pending, reqID)
		c.reqMu.Unlock()
	}()

	if err := c.Send(ctx, f); err != nil {
		return Frame{}, fmt.Errorf("sending request frame: %w", err)
	}

	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-c.closed:
		return Frame{}, errors.New("connection closed")
	case resp := <-ch:
		return resp, nil
	}
}

// readLoop pulls binary frames from the WebSocket connection.
func (c *Conn) readLoop() {
	defer c.Close()

	ctx := context.Background()

	for {
		typ, r, err := c.wsConn.Reader(ctx)
		if err != nil {
			var closeErr websocket.CloseError
			if errors.As(err, &closeErr) || errors.Is(err, io.EOF) {
				slog.Debug("Connection closed by peer", slog.String("peer", c.peerID))
			} else {
				slog.Warn("Failed to read message from peer", slog.String("peer", c.peerID), slog.Any("error", err))
			}

			return
		}

		if typ != websocket.MessageBinary {
			continue
		}

		f, err := ReadFrame(r)
		if err != nil {
			slog.Warn("Failed to decode frame", slog.String("peer", c.peerID), slog.Any("error", err))

			return
		}
		_, _ = io.Copy(io.Discard, r)

		if c.handleInternalFrame(ctx, f) {
			continue
		}

		if isResponse(f.Type) {
			c.reqMu.Lock()
			ch, ok := c.pending[f.RequestID]
			if ok {
				ch <- f
			}
			c.reqMu.Unlock()

			continue
		}

		select {
		case <-c.closed:
			return
		case c.inbound <- Envelope{PeerID: c.peerID, Frame: f}:
		}
	}
}

// handleInternalFrame handles ping/pong message types.
func (c *Conn) handleInternalFrame(ctx context.Context, f Frame) bool {
	//nolint:exhaustive
	switch f.Type {
	case MsgPing:
		ctx, cancel := context.WithTimeout(ctx, pingTimeout)
		defer cancel()

		if err := c.Send(ctx, Frame{Type: MsgPong, RequestID: f.RequestID}); err != nil {
			slog.Warn("Failed to send pong response", slog.String("peer", c.peerID), slog.Any("error", err))
		}

		return true
	case MsgPong:
		return false
	default:
		return false
	}
}

// Close tears down connection resources.
func (c *Conn) Close() error {
	var err error

	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.wsConn.Close(websocket.StatusNormalClosure, "closing connection")

		c.reqMu.Lock()
		for _, ch := range c.pending {
			close(ch)
		}
		c.pending = map[uint32]chan Frame{}
		c.reqMu.Unlock()
	})

	return err
}

// isResponse checks whether a message type is a response to an earlier request.
func isResponse(t MsgType) bool {
	return t == MsgVimpelVoteResp || t == MsgVimpelHeartbeatResp || t == MsgStateResp || t == MsgBlockResp || t == MsgPong
}
