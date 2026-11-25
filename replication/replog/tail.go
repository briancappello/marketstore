package replog

import "sync"

const defaultHotTailBytes = 256 << 20

// hotTail keeps the newest entries in memory, bounded by bytes rather than by
// count, so a replica that keeps up is served without reading the disk. It has
// its own lock so readers never contend with the log's append lock.
type hotTail struct {
	mu      sync.RWMutex
	cap     int64
	entries []Entry // consecutive sequences; entries[start:] are live
	start   int
	bytes   int64
	notify  chan struct{}
}

func newHotTail(capBytes int64) *hotTail {
	if capBytes <= 0 {
		capBytes = defaultHotTailBytes
	}
	return &hotTail{cap: capBytes, notify: make(chan struct{})}
}

// add appends e, evicting the oldest entries beyond the byte cap, and wakes
// every waiting reader.
func (t *hotTail) add(e Entry) {
	t.mu.Lock()
	if n := len(t.entries); n > t.start && t.entries[n-1].Seq+1 != e.Seq {
		// The sequence jumped (a failed append left a hole): the tail only
		// ever holds a consecutive run.
		t.reset()
	}
	t.entries = append(t.entries, e)
	t.bytes += e.Size()
	for t.bytes > t.cap && len(t.entries)-t.start > 1 {
		t.bytes -= t.entries[t.start].Size()
		t.entries[t.start] = Entry{}
		t.start++
	}
	if t.start > 1024 && t.start > len(t.entries)/2 {
		t.entries = append([]Entry(nil), t.entries[t.start:]...)
		t.start = 0
	}
	ch := t.notify
	t.notify = make(chan struct{})
	t.mu.Unlock()
	close(ch)
}

func (t *hotTail) reset() {
	t.entries, t.start, t.bytes = nil, 0, 0
}

// wait returns a channel that is closed by the next add.
func (t *hotTail) wait() <-chan struct{} {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.notify
}

// read returns entries from `from` if the tail holds it, else nil.
func (t *hotTail) read(from uint64, maxEntries int, maxBytes int64) []Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	live := t.entries[t.start:]
	if len(live) == 0 || from < live[0].Seq || from > live[len(live)-1].Seq {
		return nil
	}
	live = live[from-live[0].Seq:]
	var bytes int64
	n := 0
	for n < len(live) && n < maxEntries && (n == 0 || maxBytes <= 0 || bytes < maxBytes) {
		bytes += live[n].Size()
		n++
	}
	return append([]Entry(nil), live[:n]...)
}

// dropBefore evicts entries older than seq, so the tail never serves what
// retention has deleted.
func (t *hotTail) dropBefore(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.start < len(t.entries) && t.entries[t.start].Seq < seq {
		t.bytes -= t.entries[t.start].Size()
		t.entries[t.start] = Entry{}
		t.start++
	}
}

// first is the oldest sequence in the tail, or 0.
func (t *hotTail) first() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.entries) == t.start {
		return 0
	}
	return t.entries[t.start].Seq
}
