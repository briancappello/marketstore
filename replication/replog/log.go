// Package replog is the master's replication log: every transaction group the
// master commits, in commit order, each with a sequence number exactly one
// greater than the previous entry's. Replica streams read it from their own
// cursors, so a slow or disconnected replica falls behind instead of losing
// transaction groups.
//
// The log is a directory of append-only segment files. The active segment is
// named "<first seq>.seg"; once it is full it is sealed by renaming it to
// "<first seq>-<last seq>.seg". Only unsealed segments are scanned at open, so
// startup does not read the whole log. A sealed segment is indexed the first
// time a reader needs it.
//
// On disk an entry is a 32-byte little-endian header followed by the data:
//
//	seq u64 | tgID i64 | wallclock (unix ns) i64 | len u32 | crc32c u32 | data
//
// The CRC covers the first 28 header bytes and the data.
package replog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	goio "io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	headerLen     = 32
	segmentSuffix = ".seg"
	// sparseEvery is the index stride: a segment remembers the offset of every
	// sparseEvery-th entry and scans forward from there.
	sparseEvery = 64
	// maxEntryLen rejects a corrupt length field before it allocates.
	maxEntryLen = 1 << 31

	defaultSegmentBytes = 256 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Entry is one logged transaction group.
type Entry struct {
	Seq       uint64
	TGID      int64
	Wallclock int64 // unix nanoseconds when the master logged it
	Data      []byte
}

// Size is the entry's size on disk.
func (e Entry) Size() int64 { return headerLen + int64(len(e.Data)) }

// Options configures a Log. Zero values select defaults; MaxBytes and MaxAge
// of zero disable that retention limit.
type Options struct {
	SegmentBytes int64
	MaxBytes     int64
	MaxAge       time.Duration
	// HotTailBytes caps the in-memory tail of newest entries.
	HotTailBytes int64
	Now          func() time.Time
}

// GapReason says why a sequence cannot be served.
type GapReason int

const (
	// GapRetention: the entry was deleted by retention.
	GapRetention GapReason = iota + 1
	// GapBroken: the entry was never written, because an append failed.
	GapBroken
)

func (r GapReason) String() string {
	switch r {
	case GapRetention:
		return "retention"
	case GapBroken:
		return "log_broken"
	}
	return "unknown"
}

// GapError reports that a requested sequence is not in the log. Next is the
// first sequence after it that the log can serve.
type GapError struct {
	Requested uint64
	Next      uint64
	Reason    GapReason
}

func (e *GapError) Error() string {
	return fmt.Sprintf("replication log: sequence %d unavailable (%s); next available %d",
		e.Requested, e.Reason, e.Next)
}

type segment struct {
	first uint64
	f     *os.File

	// Guarded by Log.mu.
	last      uint64 // 0 while empty
	size      int64  // bytes written
	firstWall int64
	sealed    bool
	removed   bool

	// idxMu guards index and indexed for sealed segments, which are indexed
	// lazily by readers. The active segment is indexed at open and by Append
	// under Log.mu, and readers snapshot it under Log.mu.
	idxMu   sync.Mutex
	indexed bool
	index   []int64 // offset of entry first + k*sparseEvery
}

// Log is the replication log. Append is called by a single writer (the WAL
// flush, which is serialized); reads may run concurrently from any number of
// goroutines and never hold the lock Append takes while they do I/O.
type Log struct {
	dir  string
	opts Options

	mu       sync.RWMutex
	segments []*segment // ordered by first; the last one is active
	nextSeq  uint64
	lastTGID int64
	total    int64
	dirty    bool // active segment written since the last Sync
	toSeal   []*segment

	tail      *hotTail
	diskReads atomic.Int64
}

// Open opens or creates the log in dir.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create replication log dir: %w", err)
	}
	l := &Log{dir: dir, opts: opts, nextSeq: 1, tail: newHotTail(opts.HotTailBytes)}
	if err := l.load(); err != nil {
		l.closeFiles()
		return nil, err
	}
	return l, nil
}

func (l *Log) load() error {
	des, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read replication log dir: %w", err)
	}
	type named struct {
		first, last uint64
		sealed      bool
		name        string
	}
	var found []named
	for _, de := range des {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		base := strings.TrimSuffix(name, segmentSuffix)
		firstStr, lastStr, sealed := strings.Cut(base, "-")
		first, err := strconv.ParseUint(firstStr, 10, 64)
		if err != nil || first == 0 {
			continue
		}
		n := named{first: first, sealed: sealed, name: name}
		if sealed {
			if n.last, err = strconv.ParseUint(lastStr, 10, 64); err != nil || n.last < first {
				continue
			}
		}
		found = append(found, n)
	}
	sort.Slice(found, func(i, j int) bool { return found[i].first < found[j].first })

	for i, n := range found {
		path := filepath.Join(l.dir, n.name)
		f, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("open replication log segment %s: %w", n.name, err)
		}
		s := &segment{first: n.first, f: f}
		l.segments = append(l.segments, s)
		if n.sealed {
			fi, err := f.Stat()
			if err != nil {
				return fmt.Errorf("stat replication log segment %s: %w", n.name, err)
			}
			s.last, s.size, s.sealed = n.last, fi.Size(), true
			h, err := readHeader(f, 0)
			if err != nil {
				return fmt.Errorf("read replication log segment %s: %w", n.name, err)
			}
			s.firstWall = h.wall
		} else {
			if err := l.scanAndRepair(s); err != nil {
				return err
			}
			if i < len(found)-1 && s.last != 0 {
				// A crash between rotating and sealing: seal it now.
				s.sealed = true
				l.toSeal = append(l.toSeal, s)
			}
		}
		l.total += s.size
	}
	// Drop empty segments that are not the active one.
	kept := l.segments[:0]
	for i, s := range l.segments {
		if s.last == 0 && i < len(l.segments)-1 {
			_ = s.f.Close()
			_ = os.Remove(s.f.Name())
			continue
		}
		kept = append(kept, s)
	}
	l.segments = kept

	if n := len(l.segments); n > 0 {
		last := l.segments[n-1]
		switch {
		case last.last != 0:
			l.nextSeq = last.last + 1
		default:
			l.nextSeq = last.first
		}
		if tg, ok, err := l.lastEntryTGID(); err != nil {
			return err
		} else if ok {
			l.lastTGID = tg
		}
	}
	return l.sealPending()
}

// scanAndRepair indexes an unsealed segment, verifying every entry, and
// truncates it at the first entry that is torn, corrupt or out of sequence:
// that is where the previous process stopped writing.
func (l *Log) scanAndRepair(s *segment) error {
	fi, err := s.f.Stat()
	if err != nil {
		return fmt.Errorf("stat replication log segment: %w", err)
	}
	var off int64
	it := newScanner(s.f, 0, fi.Size())
	for {
		e, err := it.next(true)
		if err != nil || e.Seq != s.first+uint64(it.count) {
			break
		}
		if it.count%sparseEvery == 0 {
			s.index = append(s.index, off)
		}
		if it.count == 0 {
			s.firstWall = e.Wallclock
		}
		s.last = e.Seq
		it.count++
		off = it.off
	}
	if off < fi.Size() {
		if err := s.f.Truncate(off); err != nil {
			return fmt.Errorf("truncate torn replication log tail: %w", err)
		}
	}
	s.size = off
	s.indexed = true
	return nil
}

// lastEntryTGID returns the tgID of the newest entry on disk.
func (l *Log) lastEntryTGID() (int64, bool, error) {
	for i := len(l.segments) - 1; i >= 0; i-- {
		s := l.segments[i]
		if s.last == 0 {
			continue
		}
		var tg int64
		err := l.readSegment(s, s.last, s.last, s.size, 1, 0, func(e Entry) { tg = e.TGID })
		if err != nil {
			return 0, false, err
		}
		return tg, true, nil
	}
	return 0, false, nil
}

// NextSeq is the sequence the next Append will get.
func (l *Log) NextSeq() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextSeq
}

// LastSeq is the newest logged sequence, or 0 for an empty log.
func (l *Log) LastSeq() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextSeq - 1
}

// LastTGID is the transaction-group ID of the newest logged entry, or 0.
func (l *Log) LastTGID() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTGID
}

// FirstSeq is the oldest sequence on disk, or 0 when the disk log is empty.
func (l *Log) FirstSeq() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, s := range l.segments {
		if s.last != 0 {
			return s.first
		}
	}
	return 0
}

// Bytes is the size of the log on disk.
func (l *Log) Bytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.total
}

// OldestWallclock is the log time of the oldest entry on disk (unix ns), or 0.
func (l *Log) OldestWallclock() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, s := range l.segments {
		if s.last != 0 {
			return s.firstWall
		}
	}
	return 0
}

// Append logs one transaction group and returns its sequence number. The log
// keeps data (in its memory tail), so the caller must not modify it afterwards.
func (l *Log) Append(tgID int64, data []byte) (uint64, error) {
	if int64(len(data)) >= maxEntryLen {
		return 0, fmt.Errorf("replication log: entry of %d bytes is too large", len(data))
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	e := Entry{Seq: l.nextSeq, TGID: tgID, Wallclock: l.opts.Now().UnixNano(), Data: data}
	s, err := l.activeFor(e.Size())
	if err == nil {
		err = writeEntry(s, e)
	}
	if err != nil {
		return 0, err
	}
	if s.last == 0 {
		s.firstWall = e.Wallclock
	}
	if k := e.Seq - s.first; k%sparseEvery == 0 {
		s.index = append(s.index, s.size)
	}
	s.last = e.Seq
	s.size += e.Size()
	l.total += e.Size()
	l.dirty = true
	l.nextSeq++
	l.lastTGID = tgID
	l.tail.add(e)
	return e.Seq, nil
}

// activeFor returns the segment the next entry goes to, rotating when the
// active one would exceed SegmentBytes. Caller holds l.mu.
func (l *Log) activeFor(entrySize int64) (*segment, error) {
	n := len(l.segments)
	if n > 0 {
		s := l.segments[n-1]
		if !s.sealed && s.first == l.nextSeq-uint64(countOf(s)) &&
			(s.last == 0 || s.size+entrySize <= l.opts.SegmentBytes) {
			return s, nil
		}
		if !s.sealed && s.last != 0 {
			s.sealed = true
			l.toSeal = append(l.toSeal, s)
		}
	}
	path := filepath.Join(l.dir, fmt.Sprintf("%020d%s", l.nextSeq, segmentSuffix))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create replication log segment: %w", err)
	}
	s := &segment{first: l.nextSeq, f: f, indexed: true}
	if n > 0 && l.segments[n-1].last == 0 {
		// Replace an empty active segment that no longer starts at nextSeq.
		old := l.segments[n-1]
		_ = old.f.Close()
		_ = os.Remove(old.f.Name())
		l.segments[n-1] = s
	} else {
		l.segments = append(l.segments, s)
	}
	return s, nil
}

// countOf is the number of entries in s. Caller holds l.mu.
func countOf(s *segment) int {
	if s.last == 0 {
		return 0
	}
	return int(s.last - s.first + 1)
}

func writeEntry(s *segment, e Entry) error {
	buf := make([]byte, headerLen+len(e.Data))
	binary.LittleEndian.PutUint64(buf[0:], e.Seq)
	binary.LittleEndian.PutUint64(buf[8:], uint64(e.TGID))
	binary.LittleEndian.PutUint64(buf[16:], uint64(e.Wallclock))
	binary.LittleEndian.PutUint32(buf[24:], uint32(len(e.Data)))
	copy(buf[headerLen:], e.Data)
	crc := crc32.Update(crc32.Checksum(buf[:28], crcTable), crcTable, buf[headerLen:])
	binary.LittleEndian.PutUint32(buf[28:], crc)
	// One write per entry and no user-space buffer, so a checkpoint's syncfs
	// makes every entry appended before it durable.
	if _, err := s.f.WriteAt(buf, s.size); err != nil {
		// Drop whatever part of the entry reached the file.
		_ = s.f.Truncate(s.size)
		return fmt.Errorf("append to replication log: %w", err)
	}
	return nil
}

// Sync makes every appended entry durable and seals rotated segments.
func (l *Log) Sync() error {
	l.mu.Lock()
	var active *os.File
	if l.dirty && len(l.segments) > 0 {
		active = l.segments[len(l.segments)-1].f
		l.dirty = false
	}
	l.mu.Unlock()
	if active != nil {
		if err := active.Sync(); err != nil {
			return fmt.Errorf("sync replication log: %w", err)
		}
	}
	return l.sealPending()
}

// sealPending fsyncs rotated segments and renames them to their sealed name.
func (l *Log) sealPending() error {
	l.mu.Lock()
	pending := l.toSeal
	l.toSeal = nil
	l.mu.Unlock()
	var firstErr error
	for _, s := range pending {
		l.mu.RLock()
		last, removed := s.last, s.removed
		l.mu.RUnlock()
		if removed {
			continue
		}
		err := s.f.Sync()
		if err == nil {
			sealed := filepath.Join(l.dir, fmt.Sprintf("%020d-%020d%s", s.first, last, segmentSuffix))
			err = os.Rename(s.f.Name(), sealed)
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("seal replication log segment %d: %w", s.first, err)
		}
	}
	return firstErr
}

// EnforceRetention deletes the oldest segments while the log is over MaxBytes
// or a segment holds only entries older than MaxAge. The active segment is
// never deleted. Retention does not depend on any reader's progress.
func (l *Log) EnforceRetention() {
	var cutoff int64
	if l.opts.MaxAge > 0 {
		cutoff = l.opts.Now().Add(-l.opts.MaxAge).UnixNano()
	}
	l.mu.Lock()
	var doomed []*segment
	for len(l.segments) > 1 {
		s, next := l.segments[0], l.segments[1]
		overSize := l.opts.MaxBytes > 0 && l.total > l.opts.MaxBytes
		// Entries are logged in time order, so every entry in s is at least as
		// old as the first entry of the segment after it.
		tooOld := cutoff != 0 && next.last != 0 && next.firstWall < cutoff
		if !overSize && !tooOld {
			break
		}
		s.removed = true
		l.total -= s.size
		l.segments = l.segments[1:]
		doomed = append(doomed, s)
	}
	if len(doomed) > 0 {
		if first := l.firstNonEmpty(); first != nil {
			l.tail.dropBefore(first.first)
		}
	}
	l.mu.Unlock()
	for _, s := range doomed {
		_ = s.f.Close()
		_ = os.Remove(s.f.Name())
		_ = removeSealedName(l.dir, s)
	}
}

// removeSealedName removes s under its sealed name, in case it was sealed
// after its handle's name was captured.
func removeSealedName(dir string, s *segment) error {
	matches, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%020d-*%s", s.first, segmentSuffix)))
	if err != nil {
		return err
	}
	for _, m := range matches {
		_ = os.Remove(m)
	}
	return nil
}

// Close syncs and closes the log.
func (l *Log) Close() error {
	err := l.Sync()
	l.closeFiles()
	return err
}

func (l *Log) closeFiles() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		_ = s.f.Close()
	}
	l.segments = nil
}

// Read returns up to maxEntries entries (and about maxBytes, but always at
// least one) starting at from. It returns no entries when from is past the
// newest sequence, and a *GapError when from is not in the log. It never
// holds the lock Append needs while it reads the disk.
func (l *Log) Read(from uint64, maxEntries int, maxBytes int64) ([]Entry, error) {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	if es := l.tail.read(from, maxEntries, maxBytes); es != nil {
		return es, nil
	}
	var out []Entry
	err := l.readDisk(from, maxEntries, maxBytes, func(e Entry) { out = append(out, e) })
	return out, err
}

func (l *Log) readDisk(from uint64, maxEntries int, maxBytes int64, fn func(Entry)) error {
	l.mu.RLock()
	if from >= l.nextSeq {
		l.mu.RUnlock()
		return nil
	}
	var (
		s    *segment
		last uint64
		lim  int64
		gap  *GapError
	)
	for i, seg := range l.segments {
		if seg.last == 0 {
			continue
		}
		if from < seg.first {
			reason := GapBroken
			if i == 0 || l.firstNonEmpty() == seg {
				reason = GapRetention
			}
			gap = &GapError{Requested: from, Next: seg.first, Reason: reason}
			break
		}
		if from <= seg.last {
			s, last, lim = seg, seg.last, seg.size
			break
		}
	}
	if s == nil && gap == nil {
		// Past every segment but below nextSeq: those appends failed.
		gap = &GapError{Requested: from, Next: l.nextSeq, Reason: GapBroken}
	}
	var index []int64
	if s != nil && !s.sealed {
		index = s.index[:len(s.index):len(s.index)]
	}
	l.mu.RUnlock()
	if gap != nil {
		return gap
	}
	l.diskReads.Add(1)
	if index != nil {
		return readFromIndex(s, index, from, last, lim, maxEntries, maxBytes, fn)
	}
	return l.readSegment(s, from, last, lim, maxEntries, maxBytes, fn)
}

// firstNonEmpty is the oldest segment holding entries. Caller holds l.mu.
func (l *Log) firstNonEmpty() *segment {
	for _, s := range l.segments {
		if s.last != 0 {
			return s
		}
	}
	return nil
}

// readSegment reads a sealed (or loading) segment, indexing it on first use.
func (l *Log) readSegment(s *segment, from, last uint64, lim int64, maxEntries int, maxBytes int64,
	fn func(Entry),
) error {
	s.idxMu.Lock()
	if !s.indexed {
		if err := indexSealed(s, lim); err != nil {
			s.idxMu.Unlock()
			return err
		}
	}
	index := s.index
	s.idxMu.Unlock()
	return readFromIndex(s, index, from, last, lim, maxEntries, maxBytes, fn)
}

func indexSealed(s *segment, lim int64) error {
	it := newScanner(s.f, 0, lim)
	var off int64
	for {
		_, err := it.next(false)
		if errors.Is(err, goio.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("index replication log segment %d: %w", s.first, err)
		}
		if it.count%sparseEvery == 0 {
			s.index = append(s.index, off)
		}
		it.count++
		off = it.off
	}
	s.indexed = true
	return nil
}

func readFromIndex(s *segment, index []int64, from, last uint64, lim int64, maxEntries int, maxBytes int64,
	fn func(Entry),
) error {
	k := int((from - s.first) / sparseEvery)
	if k >= len(index) {
		k = len(index) - 1
	}
	it := newScanner(s.f, index[k], lim)
	seq := s.first + uint64(k)*sparseEvery
	var n int
	var bytes int64
	for seq <= last && n < maxEntries && (n == 0 || bytes < maxBytes || maxBytes <= 0) {
		e, err := it.next(seq >= from)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return &GapError{Requested: from, Next: last + 1, Reason: GapRetention}
			}
			return fmt.Errorf("read replication log segment %d at %d: %w", s.first, seq, err)
		}
		if e.Seq != seq {
			return fmt.Errorf("replication log segment %d: found sequence %d, want %d", s.first, e.Seq, seq)
		}
		if seq >= from {
			fn(e)
			n++
			bytes += e.Size()
		}
		seq++
	}
	return nil
}

type header struct {
	seq  uint64
	tgID int64
	wall int64
	n    uint32
	crc  uint32
}

func parseHeader(b []byte) header {
	return header{
		seq:  binary.LittleEndian.Uint64(b[0:]),
		tgID: int64(binary.LittleEndian.Uint64(b[8:])),
		wall: int64(binary.LittleEndian.Uint64(b[16:])),
		n:    binary.LittleEndian.Uint32(b[24:]),
		crc:  binary.LittleEndian.Uint32(b[28:]),
	}
}

func readHeader(f *os.File, off int64) (header, error) {
	var b [headerLen]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		return header{}, err
	}
	return parseHeader(b[:]), nil
}

// scanner reads consecutive entries from a segment file region.
type scanner struct {
	f     *os.File
	off   int64
	lim   int64
	count int
	hdr   [headerLen]byte
}

func newScanner(f *os.File, off, lim int64) *scanner {
	return &scanner{f: f, off: off, lim: lim}
}

// next reads the entry at the scanner's offset. With withData it returns the
// data and verifies the CRC; without, it only steps over the entry.
func (sc *scanner) next(withData bool) (Entry, error) {
	if sc.off+headerLen > sc.lim {
		return Entry{}, goio.EOF
	}
	if _, err := sc.f.ReadAt(sc.hdr[:], sc.off); err != nil {
		return Entry{}, err
	}
	h := parseHeader(sc.hdr[:])
	end := sc.off + headerLen + int64(h.n)
	if int64(h.n) >= maxEntryLen || end > sc.lim {
		return Entry{}, goio.ErrUnexpectedEOF
	}
	e := Entry{Seq: h.seq, TGID: h.tgID, Wallclock: h.wall}
	if withData {
		e.Data = make([]byte, h.n)
		if _, err := sc.f.ReadAt(e.Data, sc.off+headerLen); err != nil {
			return Entry{}, err
		}
		if crc32.Update(crc32.Checksum(sc.hdr[:28], crcTable), crcTable, e.Data) != h.crc {
			return Entry{}, errors.New("replication log entry checksum mismatch")
		}
	}
	sc.off = end
	return e, nil
}

// Notify returns a channel that is closed by the next Append. Take it before
// reading, and wait on it only if the read found nothing new.
func (l *Log) Notify() <-chan struct{} { return l.tail.wait() }

// DiskReads counts reads that were served from segment files rather than the
// in-memory tail.
func (l *Log) DiskReads() int64 { return l.diskReads.Load() }

// HotTailFirstSeq is the oldest sequence held in memory, or 0.
func (l *Log) HotTailFirstSeq() uint64 { return l.tail.first() }

// Dir is the directory the log lives in.
func (l *Log) Dir() string { return l.dir }
