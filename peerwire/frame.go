// Package peerwire implements the binary multiplexed peer protocol over WebSockets.
package peerwire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MsgType represents the type of a peerwire message frame.
type MsgType byte

const (
	// MsgAuthHandshake carries pre-shared token handshake authentication.
	MsgAuthHandshake MsgType = 0x00
	// MsgGossipPush carries a local CRDT snapshot or delta to merge into a peer.
	MsgGossipPush MsgType = 0x01
	// MsgStateReq requests a Merkle range bucket digest or full CRDT state snapshot.
	MsgStateReq MsgType = 0x02
	// MsgStateResp returns Merkle digests / state snapshot to the requester.
	MsgStateResp MsgType = 0x03
	// MsgBlockPush pushes a CAS block replica to a peer.
	MsgBlockPush MsgType = 0x04
	// MsgBlockReq requests a CAS block payload by hash.
	MsgBlockReq MsgType = 0x05
	// MsgBlockResp returns a CAS block payload.
	MsgBlockResp MsgType = 0x06
	// MsgPing checks connection liveness.
	MsgPing MsgType = 0x07
	// MsgPong responds to connection liveness check.
	MsgPong MsgType = 0x08
	// MsgVimpelVoteReq requests a lock vote.
	MsgVimpelVoteReq MsgType = 0x09
	// MsgVimpelVoteResp grants or denies a lock vote.
	MsgVimpelVoteResp MsgType = 0x0a
	// MsgVimpelHeartbeatReq maintains a lock lease.
	MsgVimpelHeartbeatReq MsgType = 0x0b
	// MsgVimpelHeartbeatResp acknowledges a lock heartbeat.
	MsgVimpelHeartbeatResp MsgType = 0x0c
	// MsgHolderNotify announces local presence or deletion of a CAS cid to its homes.
	MsgHolderNotify MsgType = 0x0d
	// MsgHolderQuery asks a home node which peers hold a given cid.
	MsgHolderQuery MsgType = 0x0e
	// MsgHolderResp returns the known live holders of a cid.
	MsgHolderResp MsgType = 0x0f
)

// maxPayloadSize limits incoming frame size to 16MB to prevent memory exhaustion.
const maxPayloadSize = 16 * 1024 * 1024

// headerSize is the binary header length in bytes.
const headerSize = 9

// Frame is the binary transmission unit over peer WebSockets.
type Frame struct {
	Type      MsgType
	RequestID uint32
	Payload   []byte
}

// WriteFrame encodes and writes a binary frame to the writer.
func WriteFrame(w io.Writer, f Frame) error {
	header := make([]byte, headerSize)
	header[0] = byte(f.Type)
	binary.BigEndian.PutUint32(header[1:5], f.RequestID)
	//nolint:gosec
	binary.BigEndian.PutUint32(header[5:9], uint32(len(f.Payload)))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("writing frame header: %w", err)
	}

	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return fmt.Errorf("writing frame payload: %w", err)
		}
	}

	return nil
}

// ReadFrame reads and decodes a binary frame from the reader.
func ReadFrame(r io.Reader) (Frame, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, err
	}

	t := MsgType(header[0])
	reqID := binary.BigEndian.Uint32(header[1:5])
	length := binary.BigEndian.Uint32(header[5:9])

	if length > maxPayloadSize {
		return Frame{}, fmt.Errorf("payload length %d exceeds max %d", length, maxPayloadSize)
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, fmt.Errorf("reading payload of length %d: %w", length, err)
		}
	}

	return Frame{
		Type:      t,
		RequestID: reqID,
		Payload:   payload,
	}, nil
}
