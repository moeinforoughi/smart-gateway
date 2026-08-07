package bus

import (
	"context"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func TestBusFanOutAndNonBlocking(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub1, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ev := domain.Event{Type: domain.EvtScoreUpdated, At: time.Now(),
		Payload: map[string]any{"node_id": "n1"}}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}

	for i, ch := range []<-chan domain.Event{sub1, sub2} {
		select {
		case got := <-ch:
			if got.Type != domain.EvtScoreUpdated {
				t.Fatalf("subscriber %d got %s", i, got.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive the event", i)
		}
		select {
		case got := <-ch:
			if got.Type != domain.EvtScoreUpdated {
				t.Fatalf("subscriber %d second event got %s", i, got.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive the second event", i)
		}
	}
}

func TestBusPublishNeverBlocksProducer(t *testing.T) {
	b := New()
	// A subscriber that never drains its (buffered) channel must not stall a
	// publisher once the buffer is full — the drop is intentional.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := b.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10_000; i++ {
			_ = b.Publish(ctx, domain.Event{Type: domain.EvtNodeProbed})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a slow subscriber")
	}
}

func TestBusUnsubscribesOnCancel(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to close on cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber channel did not close")
	}
}
