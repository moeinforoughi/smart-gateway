package speed

import (
	"context"
	"sync"

	"smart-gateway/internal/domain"
)

// fakeBus records published events.
type fakeBus struct {
	mu  sync.Mutex
	evt []domain.Event
}

func (f *fakeBus) Publish(_ context.Context, e domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evt = append(f.evt, e)
	return nil
}

func (f *fakeBus) types() []domain.EventType {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.EventType
	for _, e := range f.evt {
		out = append(out, e.Type)
	}
	return out
}
