package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/c6s-zavod/soyuz/crdt"
	"github.com/c6s-zavod/soyuz/peerwire"
)

const DefaultSyncInterval = 5 * time.Second

// Gossiper runs periodic anti-entropy rounds across the cluster.
type Gossiper struct {
	store    *crdt.Store
	hub      *peerwire.Hub
	selfID   string
	interval time.Duration
}

// New constructs a Gossiper bound to a CRDT store and Peerwire hub.
func New(store *crdt.Store, hub *peerwire.Hub, selfID string) *Gossiper {
	return &Gossiper{
		store:    store,
		hub:      hub,
		selfID:   selfID,
		interval: DefaultSyncInterval,
	}
}

// Run executes anti-entropy rounds until ctx is cancelled.
func (g *Gossiper) Run(ctx context.Context) error {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			g.Round(ctx)
		}
	}
}

// Round runs anti-entropy sync against all connected peers.
func (g *Gossiper) Round(ctx context.Context) {
	slog.Debug("Starting anti-entropy round")
	payload := g.store.Snapshot()

	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("Failed to marshal snapshot for gossip", slog.Any("error", err))

		return
	}

	frame := peerwire.Frame{
		Type:    peerwire.MsgGossipPush,
		Payload: body,
	}

	for _, peerID := range g.hub.ActivePeers() {
		if peerID == g.selfID {
			continue
		}

		if err := g.hub.Send(ctx, peerID, frame); err != nil {
			slog.Error("Failed to send gossip to peer", slog.String("peer", peerID), slog.Any("error", err))
		} else {
			slog.Debug("Gossip frame sent to peer", slog.String("peer", peerID))
		}
	}
}

// HandleGossipPayload merges an incoming gossip payload into the local store.
func (g *Gossiper) HandleGossipPayload(payloadBytes []byte) error {
	var payload crdt.Payload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("unmarshaling gossip payload: %w", err)
	}

	g.store.Merge(payload)

	return nil
}
