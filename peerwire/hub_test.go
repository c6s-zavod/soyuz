package peerwire_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestHubHandshakeAndCommunication(t *testing.T) {
	secret := "shared-secret"
	hubA := peerwire.New("node-a", secret)
	hubB := peerwire.New("node-b", secret)
	defer func() { _ = hubA.Close() }()
	defer func() { _ = hubB.Close() }()

	srv := httptest.NewServer(http.HandlerFunc(hubA.HandleHTTP))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connB, err := hubB.Dial(ctx, "node-a", addr)
	if err != nil {
		t.Fatalf("Hub B failed to dial Hub A: %v", err)
	}

	// Hub A should register the inbound connection keyed by Hub B's node ID.
	var connA *peerwire.Conn
	for range 50 {
		var ok bool
		connA, ok = hubA.Conn("node-b")
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if connA == nil {
		t.Fatal("Hub A failed to register connection from Hub B")
	}

	reqPayload := []byte("hello from b")
	respPayload := []byte("hello back from a")

	go func() {
		select {
		case <-ctx.Done():
			return
		case env := <-hubA.Inbound():
			if env.Frame.Type == peerwire.MsgVimpelVoteReq {
				_ = connA.Send(ctx, peerwire.Frame{
					Type:      peerwire.MsgVimpelVoteResp,
					RequestID: env.Frame.RequestID,
					Payload:   respPayload,
				})
			}
		}
	}()

	resp, err := connB.Request(ctx, peerwire.Frame{Type: peerwire.MsgVimpelVoteReq, Payload: reqPayload})
	if err != nil {
		t.Fatalf("Hub B request failed: %v", err)
	}
	if resp.Type != peerwire.MsgVimpelVoteResp {
		t.Errorf("expected response type MsgVimpelVoteResp, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, respPayload) {
		t.Errorf("expected response payload %q, got %q", respPayload, resp.Payload)
	}
}
