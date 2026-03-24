package filedbms

import "testing"

func TestNormalizeValue(t *testing.T) {
	got := normalizeValue([]byte("alpha"))
	name, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if name != "alpha" {
		t.Fatalf("expected alpha, got %s", name)
	}
}

func TestTryReplyDropsWhenChannelIsFull(t *testing.T) {
	reply := make(chan Result, 1)
	reply <- Result{Rows: []map[string]any{{"id": 1}}}

	tryReply(reply, Result{Err: nil})

	if len(reply) != 1 {
		t.Fatalf("expected channel length to stay 1, got %d", len(reply))
	}
}
