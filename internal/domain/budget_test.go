package domain

import (
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func TestBudgetAllowThenNote(t *testing.T) {
	clk := &fakeClock{now: time.Unix(0, 0)}
	b := NewBudget(1000, clk)

	if !b.Allow(1000) {
		t.Fatal("Allow(1000) should fit in a 1000-byte budget")
	}
	if b.Allow(1001) {
		t.Fatal("Allow(1001) must exceed the cap")
	}
	b.Note(1000)
	if got := b.Remaining(); got != 0 {
		t.Fatalf("Remaining = %d, want 0", got)
	}
	if got := b.Used(); got != 1000 {
		t.Fatalf("Used = %d, want 1000", got)
	}
}

func TestBudgetClampsUsage(t *testing.T) {
	b := NewBudget(100, &fakeClock{now: time.Unix(0, 0)})
	b.Note(500) // over the cap
	if got := b.Used(); got != 100 {
		t.Fatalf("Used = %d, want clamped to 100", got)
	}
}

func TestBudgetNonPositiveIgnored(t *testing.T) {
	b := NewBudget(100, &fakeClock{now: time.Unix(0, 0)})
	if !b.Allow(0) {
		t.Fatal("Allow(0) is free")
	}
	b.Note(-5)
	if got := b.Used(); got != 0 {
		t.Fatalf("Used = %d after negative Note, want 0", got)
	}
}

func TestBudgetRollsOverAfter24h(t *testing.T) {
	clk := &fakeClock{now: time.Unix(0, 0)}
	b := NewBudget(100, clk)
	b.Note(100)
	if got := b.Remaining(); got != 0 {
		t.Fatalf("Remaining before rollover = %d, want 0", got)
	}

	// Just before 24h: still exhausted.
	clk.now = time.Unix(0, 0).Add(24*time.Hour - time.Second)
	if b.Allow(1) {
		t.Fatal("Allow(1) before the window rolls must still be denied")
	}

	// At exactly 24h: the window rolls and the budget resets.
	clk.now = time.Unix(0, 0).Add(24 * time.Hour)
	if !b.Allow(1) {
		t.Fatal("window should roll over at 24h")
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used after rollover = %d, want 0", got)
	}
}

func TestBudgetMaxZero(t *testing.T) {
	b := NewBudget(0, &fakeClock{now: time.Unix(0, 0)})
	if b.Allow(1) {
		t.Fatal("a zero-cap budget must deny everything")
	}
	if got := b.Remaining(); got != 0 {
		t.Fatalf("Remaining of zero-cap budget = %d, want 0", got)
	}
}
