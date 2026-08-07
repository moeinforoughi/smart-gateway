// Package repository implements durable storage for subscriptions, nodes and
// node health measurements (FR-300).
//
// SQLite runs in WAL mode with foreign keys enabled. The driver is the
// cgo-free, pure-Go github.com/ncruces/go-sqlite3, so builds stay race-clean
// on every platform. The Repo is safe for concurrent use; write throughput is
// serialized by SQLite itself.
package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"smart-gateway/internal/domain"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// migrations is an ordered list of schema DDL; PRAGMA user_version tracks how
// many have been applied.
var migrations = []string{schemaV1}

// Repo is the SQLite-backed store. It satisfies SubscriptionStore, NodeStore
// and HealthStore.
type Repo struct {
	db    *sql.DB
	clock domain.Clock
}

// New opens (creating when needed) the SQLite database at path, applies
// migrations and sets connection pragmas. The parent directory is created.
func New(path string, clock domain.Clock) (*Repo, error) {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create data dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	r := &Repo{db: db, clock: clock}
	if err := r.configure(); err != nil {
		db.Close()
		return nil, err
	}
	if err := r.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return r, nil
}

func (r *Repo) configure() error {
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := r.db.Exec(stmt); err != nil {
			return fmt.Errorf("apply pragma %s: %w", stmt, err)
		}
	}
	return nil
}

func (r *Repo) migrate() error {
	var version int
	if err := r.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	for ; version < len(migrations); version++ {
		tx, err := r.db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", version+1, err)
		}
		if _, err := tx.Exec(migrations[version]); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %d: %w", version+1, err)
		}
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", version+1)); err != nil {
			tx.Rollback()
			return fmt.Errorf("stamp migration %d: %w", version+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", version+1, err)
		}
	}
	return nil
}

// Close releases the underlying connection pool.
func (r *Repo) Close() error { return r.db.Close() }

// ---- serialization helpers ----

func encJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func decJSON[T any](s string, out *T) {
	if s == "" {
		return
	}
	_ = json.Unmarshal([]byte(s), out)
}

func timeText(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func parseTime(s *string) time.Time {
	if s == nil || *s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, *s)
	return t
}

func durationText(d time.Duration) string { return d.String() }

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}
