package ledger

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

const snapshotVersion = 1

type snapshot struct {
	Version      int
	HistoryStart uint64
	LastSeq      uint64
	Buckets      []Bucket
}

// WriteSnapshot saves the ledger to path atomically. The ledger is locked only
// while its ranges are copied, not while they are encoded and written, so
// Record (on the master's write path) waits milliseconds at most.
func (l *Ledger) WriteSnapshot(path string) error {
	l.mu.Lock()
	snap := snapshot{Version: snapshotVersion, HistoryStart: l.historyStart, LastSeq: l.lastSeq,
		Buckets: make([]Bucket, 0, len(l.buckets))}
	for _, b := range l.buckets {
		c := *b
		c.Ranges = append([]Range(nil), b.Ranges...)
		snap.Buckets = append(snap.Buckets, c)
	}
	l.mu.Unlock()

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("write ledger snapshot: %w", err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	err = gob.NewEncoder(w).Encode(&snap)
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write ledger snapshot: %w", err)
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// LoadSnapshot reads a ledger saved by WriteSnapshot. A missing file returns
// an error satisfying os.IsNotExist.
func LoadSnapshot(path string, maxRanges int) (*Ledger, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var snap snapshot
	if err := gob.NewDecoder(bufio.NewReaderSize(f, 1<<20)).Decode(&snap); err != nil {
		return nil, fmt.Errorf("read ledger snapshot %s: %w", path, err)
	}
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("ledger snapshot %s: version %d, want %d", path, snap.Version, snapshotVersion)
	}
	l := New(maxRanges)
	l.historyStart, l.lastSeq = snap.HistoryStart, snap.LastSeq
	for i := range snap.Buckets {
		b := snap.Buckets[i]
		l.buckets[b.Key] = &b
	}
	return l, nil
}
