// Package gossip implements epoch-based watermark anti-entropy synchronization.
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

// StateReqPayload carries local watermarks when requesting peer state.
type StateReqPayload struct {
	Watermarks WatermarkMap `json:"watermarks"`
}

// StateRespPayload carries deltas newer than requester watermarks.
type StateRespPayload struct {
	Delta crdt.Payload `json:"delta"`
}

// Gossiper runs background Epoch-watermark anti-entropy rounds across the cluster.
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

// PushDelta immediately broadcasts a single record update to all active peers (active push).
func (g *Gossiper) PushDelta(ctx context.Context, namespace, key string, rec crdt.Record) error {
	payload := crdt.Payload{
		Namespaces: map[string]map[string]crdt.Record{
			namespace: {key: rec},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling delta push: %w", err)
	}

	frame := peerwire.Frame{
		Type:    peerwire.MsgGossipPush,
		Payload: body,
	}

	for _, peerID := range g.hub.ActivePeers() {
		if peerID == g.selfID {
			continue
		}
		_ = g.hub.Send(ctx, peerID, frame)
	}

	return nil
}

// Round runs an Epoch-watermark anti-entropy round against all connected peers.
func (g *Gossiper) Round(ctx context.Context) {
	slog.Debug("Starting Epoch-watermark anti-entropy round")
	localSnapshot := g.store.Snapshot()
	localWM := ComputeWatermarks(localSnapshot)

	reqPayload := StateReqPayload{Watermarks: localWM}
	reqBytes, err := json.Marshal(reqPayload)
	if err != nil {
		slog.Error("Failed to marshal state req watermarks", slog.Any("error", err))

		return
	}

	frame := peerwire.Frame{
		Type:    peerwire.MsgStateReq,
		Payload: reqBytes,
	}

	for _, peerID := range g.hub.ActivePeers() {
		if peerID == g.selfID {
			continue
		}

		respFrame, err := g.hub.Request(ctx, peerID, frame)
		if err != nil {
			slog.Debug("Watermark sync request failed", slog.String("peer", peerID), slog.Any("error", err))

			continue
		}

		if respFrame.Type != peerwire.MsgStateResp || len(respFrame.Payload) == 0 {
			continue
		}

		var respPayload StateRespPayload
		if err := json.Unmarshal(respFrame.Payload, &respPayload); err != nil {
			slog.Error("Failed to unmarshal state response delta", slog.String("peer", peerID), slog.Any("error", err))

			continue
		}

		if len(respPayload.Delta.Namespaces) > 0 {
			g.store.Merge(respPayload.Delta)
			slog.Debug("Merged watermark delta from peer", slog.String("peer", peerID))
		}
	}
}

// HandleStateReq processes an incoming MsgStateReq frame and returns missing deltas.
func (g *Gossiper) HandleStateReq(reqBytes []byte) ([]byte, error) {
	var reqPayload StateReqPayload
	if err := json.Unmarshal(reqBytes, &reqPayload); err != nil {
		return nil, fmt.Errorf("unmarshaling state req: %w", err)
	}

	localSnapshot := g.store.Snapshot()
	deltaPayload := ComputeDelta(localSnapshot, reqPayload.Watermarks)

	respPayload := StateRespPayload{Delta: deltaPayload}

	return json.Marshal(respPayload)
}

// HandleGossipPayload merges an incoming direct gossip or delta push payload into the local store.
func (g *Gossiper) HandleGossipPayload(payloadBytes []byte) error {
	var payload crdt.Payload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("unmarshaling gossip payload: %w", err)
	}

	g.store.Merge(payload)

	return nil
}
