package wal

import "github.com/alpacahq/marketstore/v4/utils/io"

type OffsetIndexBuffer []byte

// Offset is the byte offset from the head of the file to write the record.
// offset = (index-1)*int64(recordSize) + FileHeadersSize.
// used to seek the point to write the record to the file.
func (b OffsetIndexBuffer) Offset() int64 {
	return io.ToInt64(b[:8])
}

// Index indicates the number of timeframes before this data from Jan 1st, 00:00:00 of the year.
// Note that Index starts from 1 (unless the timeframe is 1D).
// e.g. if timeframe=1Min and the time of the record = Jan 2nd, 03:04:05,
// then its index is 1625 because it's 1day 3hour 4min (=1624min) from Jan 1st, 00:00:00.
func (b OffsetIndexBuffer) Index() int64 {
	return io.ToInt64(b[8:16])
}

func (b OffsetIndexBuffer) IndexAndPayload() []byte {
	return b[8:]
}

// Payload can be multiple rows data that have the same index, in case of VariableLength record type.
func (b OffsetIndexBuffer) Payload() []byte {
	return b[16:]
}

// Span is a run of record writes that land on contiguous file offsets and can
// therefore be committed with a single WriteAt instead of one per record.
type Span struct {
	Offset int64
	Data   []byte
}

// CoalesceAdjacent merges runs of writes whose file offsets are strictly
// contiguous into single spans, preserving the input order.
//
// Fixed-length records are index-addressed (offset = (index-1)*recordSize +
// headersSize), so consecutive bars for one bucket are adjacent on disk.
// Emitting them as one write per record turns a run of N bars into N syscalls,
// each smaller than a page. That is invisible in the byte count -- the logical
// volume is identical -- but it is not invisible to the disk: on a
// copy-on-write or compressing filesystem every one of those sub-page writes
// forces a read-modify-write of the extent containing it, so the cost scales
// with the number of writes rather than the number of bytes.
//
// This deliberately does NOT sort. Two commands in one transaction group may
// target the same offset, and the contract there is last-write-wins; reordering
// would silently change which one survives. Preserving order keeps the merge
// semantically identical to writing each buffer in turn, which is the only
// reason it is safe to do at all. Unsorted input simply coalesces less.
func CoalesceAdjacent(writes []OffsetIndexBuffer) []Span {
	if len(writes) == 0 {
		return nil
	}
	spans := make([]Span, 0, len(writes))
	start := 0
	for i := 1; i <= len(writes); i++ {
		// Extend the current run while the next write begins exactly where the
		// previous one ends. A gap would need a seek and an overlap would need
		// the later write to win, so neither can be folded into one WriteAt.
		if i < len(writes) && writes[i].Offset() == endOffset(writes[i-1]) {
			continue
		}
		spans = append(spans, newSpan(writes[start:i]))
		start = i
	}
	return spans
}

// endOffset is the offset one byte past the record b writes.
func endOffset(b OffsetIndexBuffer) int64 {
	return b.Offset() + int64(len(b.IndexAndPayload()))
}

// newSpan flattens one contiguous run. A run of one is returned by reference so
// the common non-adjacent case stays allocation-free.
func newSpan(run []OffsetIndexBuffer) Span {
	if len(run) == 1 {
		return Span{Offset: run[0].Offset(), Data: run[0].IndexAndPayload()}
	}
	n := 0
	for _, b := range run {
		n += len(b.IndexAndPayload())
	}
	data := make([]byte, 0, n)
	for _, b := range run {
		data = append(data, b.IndexAndPayload()...)
	}
	return Span{Offset: run[0].Offset(), Data: data}
}
