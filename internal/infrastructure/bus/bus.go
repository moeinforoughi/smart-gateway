// Package bus implements the in-memory domain event bus (architecture §6,
// M4). Publish fans out to every subscriber with non-blocking semantics so a
// slow consumer can never stall a producer (health monitor, scorer, routing).
// Dropped events are acceptable: subscribers re-derive state from the store.
package bus

import (
	"context"
	"sync"

	"smart-gateway/internal/domain"
)

// Bus is a fan-out EventSource + Bus. It is safe for concurrent use.
type Bus struct {
	mu   sync.RWMutex
	subs map[chan domain.Event]struct{}
}

// New returns an empty bus.
func New() *Bus {
	return &Bus{subs: make(map[chan domain.Event]struct{})}
}

// Publish delivers e to every subscriber channel, dropping it for any
// subscriber whose buffer is full (never blocks the producer).
func (b *Bus) Publish(_ context.Context, e domain.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
		}
	}
	return nil
}

// Subscribe registers a subscriber. The returned channel is closed when ctx
// is cancelled and the subscription is removed.
func (b *Bus) Subscribe(ctx context.Context) (<-chan domain.Event, error) {
	ch := make(chan domain.Event, 256)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		delete(b.subs, ch)
		close(ch)
		b.mu.Unlock()
	}()
	return ch, nil
}
