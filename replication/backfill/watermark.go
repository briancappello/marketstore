// Package backfill implements the replica-side pull backfill: it enumerates the
// master's buckets, reads historical ranges via the master's Query API, writes
// them locally (idempotent), and tracks a per-bucket watermark so it only
// fetches what is outstanding.
package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Watermarks stores the last-synced epoch per TimeBucketKey, persisted as JSON.
//
// Set only mutates memory; call Flush to persist. Persisting per Set is what it
// used to do, and it dominated the replica's entire write load: the file holds
// every bucket, so each advance re-marshalled and rewrote the whole map. At 35k
// buckets that is ~1 MB per advanced bucket, ~9.7 GB for a single reconcile
// pass -- 99.5% of all bytes written, against ~14 MB of actual market data, and
// quadratic in bucket count.
//
// Batching costs nothing in correctness: a watermark is a resume hint, not a
// record of truth. A crash between Flushes just re-pulls a little more, and
// backfill writes are idempotent by epoch.
type Watermarks struct {
	mu    sync.Mutex
	path  string
	m     map[string]int64
	dirty bool
}

// NewWatermarks loads watermarks from path, or starts empty if it does not exist.
func NewWatermarks(path string) (*Watermarks, error) {
	w := &Watermarks{path: path, m: map[string]int64{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return w, nil
		}
		return nil, fmt.Errorf("read watermarks %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &w.m); err != nil {
		return nil, fmt.Errorf("parse watermarks %s: %w", path, err)
	}
	return w, nil
}

// Get returns the last-synced epoch for tbk, or 0 if none is recorded.
func (w *Watermarks) Get(tbk string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.m[tbk]
}

// Set advances the watermark for tbk to epoch (never regresses). It does not
// touch the disk; the caller persists a whole pass at once via Flush.
func (w *Watermarks) Set(tbk string, epoch int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if epoch <= w.m[tbk] {
		return nil
	}
	w.m[tbk] = epoch
	w.dirty = true
	return nil
}

// Flush persists pending advances, and is a no-op when nothing changed.
func (w *Watermarks) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.dirty {
		return nil
	}
	if err := w.persistLocked(); err != nil {
		return err
	}
	w.dirty = false
	return nil
}

func (w *Watermarks) persistLocked() error {
	data, err := json.Marshal(w.m)
	if err != nil {
		return fmt.Errorf("marshal watermarks: %w", err)
	}
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write watermarks: %w", err)
	}
	return os.Rename(tmp, w.path) // atomic replace
}
