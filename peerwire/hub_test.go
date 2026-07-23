package peerwire_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/c6s-zavod/soyuz/peerwire"
)

func TestFrameReadWrite(t *testing.T) {
	orig := peerwire.Frame{
		Type:      peerwire.MsgStateReq,
		RequestID: 42,
		Payload:   []byte("test payload data"),
	}

	var buf bytes.Buffer
	if err := peerwire.WriteFrame(&buf, orig); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	decoded, err := peerwire.ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if decoded.Type != orig.Type {
		t.Errorf("expected type %v, got %v", orig.Type, decoded.Type)
	}
	if decoded.RequestID != orig.RequestID {
		t.Errorf("expected requestID %v, got %v", orig.RequestID, decoded.RequestID)
	}
	if !bytes.Equal(decoded.Payload, orig.Payload) {
		t.Errorf("expected payload %s, got %s", orig.Payload, decoded.Payload)
	}
}

func TestAuthTokenVerification(t *testing.T) {
	senderHub := peerwire.New("node-1", "secret-key")
	receiverHub := peerwire.New("node-2", "secret-key")
	now := time.Now()

	token := senderHub.GenerateAuthToken("node-2", now)
	if !receiverHub.VerifyAuthToken("node-1", token, now.Unix()) {
		t.Error("expected valid token verification to succeed")
	}

	if receiverHub.VerifyAuthToken("node-1", token, now.Add(-10*time.Minute).Unix()) {
		t.Error("expected expired token verification to fail")
	}
}
