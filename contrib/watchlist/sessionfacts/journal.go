package sessionfacts

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Entry is one (symbol, trading date) whose facts must be recomputed.
type Entry struct {
	Symbol string
	// Date is midnight of the trading date in America/New_York.
	Date time.Time
}

func (e Entry) key() string { return e.Symbol + "\t" + e.Date.Format("2006-01-02") }

// Journal is the durable set of dirty (symbol, date) entries. An entry is
// appended to the file as soon as it is added, so a restart does not lose
// it, and is removed only after its recompute has been written.
type Journal struct {
	mu      sync.Mutex
	path    string
	entries map[string]Entry
}

// OpenJournal loads the journal at path, creating its directory if needed.
// A missing file is an empty journal.
func OpenJournal(path string) (*Journal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create journal dir: %w", err)
	}
	j := &Journal{path: path, entries: map[string]Entry{}}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return j, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		sym, date, ok := strings.Cut(sc.Text(), "\t")
		if !ok {
			continue // a torn last line from a crash mid-append
		}
		d, err := time.ParseInLocation("2006-01-02", date, calendar.Nasdaq.Tz())
		if err != nil {
			continue
		}
		e := Entry{Symbol: sym, Date: d}
		j.entries[e.key()] = e
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	return j, nil
}

// Add records e. It is a no-op when e is already pending.
func (j *Journal) Add(e Entry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	k := e.key()
	if _, ok := j.entries[k]; ok {
		return nil
	}
	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	defer f.Close()
	if _, err = f.WriteString(k + "\n"); err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	j.entries[k] = e
	return nil
}

// AddAll records entries with a single append. Entries already pending are
// skipped. It returns how many were added.
func (j *Journal) AddAll(entries []Entry) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var b strings.Builder
	var added []Entry
	for _, e := range entries {
		k := e.key()
		if _, ok := j.entries[k]; ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('\n')
		added = append(added, e)
	}
	if len(added) == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, fmt.Errorf("append journal: %w", err)
	}
	defer f.Close()
	if _, err = f.WriteString(b.String()); err != nil {
		return 0, fmt.Errorf("append journal: %w", err)
	}
	for _, e := range added {
		j.entries[e.key()] = e
	}
	return len(added), nil
}

// Pending returns the pending entries, sorted by symbol then date.
func (j *Journal) Pending() []Entry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Entry, 0, len(j.entries))
	for _, e := range j.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Symbol != out[b].Symbol {
			return out[a].Symbol < out[b].Symbol
		}
		return out[a].Date.Before(out[b].Date)
	})
	return out
}

// Remove drops done entries and rewrites the file atomically with the ones
// still pending, including any added while done was being processed.
func (j *Journal) Remove(done []Entry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range done {
		delete(j.entries, e.key())
	}
	tmp := j.path + ".tmp"
	var b strings.Builder
	for k := range j.entries {
		b.WriteString(k)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("rewrite journal: %w", err)
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return fmt.Errorf("rewrite journal: %w", err)
	}
	return nil
}
