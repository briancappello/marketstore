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
type Watermarks struct {
	mu   sync.Mutex
	path string
	m    map[string]int64
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

// Set advances the watermark for tbk to epoch (never regresses) and persists.
func (w *Watermarks) Set(tbk string, epoch int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if epoch <= w.m[tbk] {
		return nil
	}
	w.m[tbk] = epoch
	return w.persistLocked()
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
