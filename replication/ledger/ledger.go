// Package ledger is the master's change ledger: for every bucket, which epoch
// ranges were written and in which sequence range of the replication log. A
// replica that falls behind the log's retention asks it what changed since its
// cursor, and re-pulls exactly those ranges for fixed buckets.
//
// The ledger coalesces: its size depends on the number of buckets and a
// bounded number of ranges per bucket, not on the number of writes. Ranges
// over-approximate (a merged range may cover epochs written before a given
// sequence), which is safe because fixed re-pulls are idempotent.
package ledger

import (
	"sort"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// DefaultMaxRanges bounds the ranges kept per bucket.
const DefaultMaxRanges = 64

// Range is one coalesced run of writes to a bucket.
type Range struct {
	FirstSeq, LastSeq  uint64
	EpochMin, EpochMax int64 // inclusive, unix seconds
	LastWall           int64 // unix ns of the newest write
}

// Bucket is one bucket's changes.
type Bucket struct {
	Key      string // "Symbol/Timeframe/AttributeGroup"
	Variable bool
	Interval int64 // timeframe in seconds
	Ranges   []Range
}

// Ledger records changed ranges per bucket. It is safe for concurrent use.
type Ledger struct {
	mu        sync.Mutex
	buckets   map[string]*Bucket
	maxRanges int
	// historyStart: the ledger holds every change with a sequence greater
	// than historyStart. Expiry and log holes raise it.
	historyStart uint64
	lastSeq      uint64
	keyCache     map[string]keyInfo
}

type keyInfo struct {
	bucket   string
	interval int64
	year     int16
	tf       time.Duration
	ok       bool
}

func New(maxRanges int) *Ledger {
	if maxRanges <= 0 {
		maxRanges = DefaultMaxRanges
	}
	return &Ledger{buckets: map[string]*Bucket{}, maxRanges: maxRanges, keyCache: map[string]keyInfo{}}
}

// Record adds one logged transaction group with sequence seq, logged at wall
// (unix ns). Sequences must be recorded in increasing order.
func (l *Ledger) Record(seq uint64, wall int64, tgSerialized []byte) {
	type span struct {
		min, max int64
		variable bool
		interval int64
	}
	spans := map[string]*span{}
	l.mu.Lock()
	defer l.mu.Unlock()
	scanTG(tgSerialized, func(recordType io.EnumRecordType, keyPath []byte, index int64) {
		ki := l.keyInfo(keyPath)
		if !ki.ok {
			return
		}
		start := io.IndexToTime(index, ki.tf, ki.year).Unix()
		end := start + ki.interval - 1
		s := spans[ki.bucket]
		if s == nil {
			spans[ki.bucket] = &span{min: start, max: end, variable: recordType == io.VARIABLE, interval: ki.interval}
			return
		}
		if start < s.min {
			s.min = start
		}
		if end > s.max {
			s.max = end
		}
	})
	for key, s := range spans {
		b := l.buckets[key]
		if b == nil {
			b = &Bucket{Key: key, Variable: s.variable, Interval: s.interval}
			l.buckets[key] = b
		}
		b.add(Range{FirstSeq: seq, LastSeq: seq, EpochMin: s.min, EpochMax: s.max, LastWall: wall}, l.maxRanges)
	}
	if seq > l.lastSeq {
		l.lastSeq = seq
	}
}

func (l *Ledger) keyInfo(keyPath []byte) keyInfo {
	if ki, ok := l.keyCache[string(keyPath)]; ok {
		return ki
	}
	k := string(keyPath)
	tbk, year, err := io.NewTimeBucketKeyFromWalKeyPath(k)
	var ki keyInfo
	if err == nil {
		if tf, err := tbk.GetTimeFrame(); err == nil && tf.Duration > 0 {
			ki = keyInfo{bucket: tbk.GetItemKey(), tf: tf.Duration, interval: int64(tf.Duration.Seconds()),
				year: int16(year), ok: true}
			if ki.interval < 1 {
				ki.interval = 1
			}
		}
	}
	l.keyCache[k] = ki
	return ki
}

// add merges r into the bucket's ranges: with every range it overlaps or
// touches (within one interval), then down to maxRanges by merging the two
// ranges with the smallest epoch gap.
func (b *Bucket) add(r Range, maxRanges int) {
	kept := b.Ranges[:0]
	for _, x := range b.Ranges {
		if x.EpochMin <= r.EpochMax+b.Interval && r.EpochMin <= x.EpochMax+b.Interval {
			r = merge(r, x)
			continue
		}
		kept = append(kept, x)
	}
	b.Ranges = append(kept, r)
	sort.Slice(b.Ranges, func(i, j int) bool { return b.Ranges[i].EpochMin < b.Ranges[j].EpochMin })
	for len(b.Ranges) > maxRanges {
		best, gap := 0, int64(-1)
		for i := 0; i+1 < len(b.Ranges); i++ {
			if g := b.Ranges[i+1].EpochMin - b.Ranges[i].EpochMax; gap < 0 || g < gap {
				best, gap = i, g
			}
		}
		b.Ranges[best] = merge(b.Ranges[best], b.Ranges[best+1])
		b.Ranges = append(b.Ranges[:best+1], b.Ranges[best+2:]...)
	}
}

func merge(a, b Range) Range {
	if b.FirstSeq < a.FirstSeq {
		a.FirstSeq = b.FirstSeq
	}
	if b.LastSeq > a.LastSeq {
		a.LastSeq = b.LastSeq
	}
	if b.EpochMin < a.EpochMin {
		a.EpochMin = b.EpochMin
	}
	if b.EpochMax > a.EpochMax {
		a.EpochMax = b.EpochMax
	}
	if b.LastWall > a.LastWall {
		a.LastWall = b.LastWall
	}
	return a
}

// Changes is the answer to "what changed after sequence since".
type Changes struct {
	// Incomplete: the ledger does not reach back to since; changes after it
	// may be missing. A full resync is required.
	Incomplete   bool
	HistoryStart uint64
	AsOf         uint64
	Buckets      []Bucket // only ranges with LastSeq > since
}

// ChangesSince returns, per bucket, the ranges written after since.
func (l *Ledger) ChangesSince(since uint64) Changes {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := Changes{HistoryStart: l.historyStart, AsOf: l.lastSeq, Incomplete: since < l.historyStart}
	keys := make([]string, 0, len(l.buckets))
	for k := range l.buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b := l.buckets[k]
		var rs []Range
		for _, r := range b.Ranges {
			if r.LastSeq > since {
				rs = append(rs, r)
			}
		}
		if len(rs) > 0 {
			c.Buckets = append(c.Buckets, Bucket{Key: b.Key, Variable: b.Variable, Interval: b.Interval, Ranges: rs})
		}
	}
	return c
}

// Expire drops ranges whose newest write is older than cutoff (unix ns). The
// ledger can then no longer answer for sequences those ranges covered.
func (l *Ledger) Expire(cutoff int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, b := range l.buckets {
		kept := b.Ranges[:0]
		for _, r := range b.Ranges {
			if r.LastWall < cutoff {
				if r.LastSeq > l.historyStart {
					l.historyStart = r.LastSeq
				}
				continue
			}
			kept = append(kept, r)
		}
		b.Ranges = kept
		if len(kept) == 0 {
			delete(l.buckets, k)
		}
	}
}

// LastSeq is the newest sequence recorded.
func (l *Ledger) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSeq
}

// HistoryStart: the ledger holds every change with a sequence above it.
func (l *Ledger) HistoryStart() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.historyStart
}

// Forget marks every change up to seq as unknown: used when the record of
// them was lost (for example a hole in the log while rebuilding).
func (l *Ledger) Forget(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq > l.historyStart {
		l.historyStart = seq
	}
	if seq > l.lastSeq {
		l.lastSeq = seq
	}
}

// scanTG calls fn for every write command of a serialized transaction group,
// without copying its data. See executor.ParseTGData for the format.
func scanTG(tg []byte, fn func(recordType io.EnumRecordType, keyPath []byte, index int64)) {
	const header = 16
	if len(tg) < header {
		return
	}
	n := io.ToInt64(tg[8:16])
	c := header
	for i := int64(0); i < n; i++ {
		if c+1+2 > len(tg) {
			return
		}
		rt := io.EnumRecordType(io.ToInt8(tg[c : c+1]))
		c++
		kl := int(io.ToInt16(tg[c : c+2]))
		c += 2
		if c+kl+4+4+16 > len(tg) {
			return
		}
		key := tg[c : c+kl]
		c += kl
		dataLen := int(io.ToInt32(tg[c : c+4]))
		c += 4 + 4 // data length, variable record length
		if c+16+dataLen > len(tg) {
			return
		}
		index := io.ToInt64(tg[c+8 : c+16]) // offset, then index
		c += 16 + dataLen
		fn(rt, key, index)
		if c >= len(tg) {
			return // DSVFromBytes cannot take an empty slice
		}
		_, dsLen := io.DSVFromBytes(tg[c:])
		c += dsLen
	}
}
