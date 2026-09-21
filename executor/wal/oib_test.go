package wal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// recordPayloadLen makes an on-disk record 32 bytes (8-byte index + payload),
// matching a fixed OHLCV row: epoch + 4 float32 + int64.
const recordPayloadLen = 24

const recordLen = 8 + recordPayloadLen

// buf builds an OffsetIndexBuffer the same way serializeTG does:
// [offset int64][index int64][payload].
func buf(t *testing.T, offset, index int64, fill byte) wal.OffsetIndexBuffer {
	t.Helper()

	payload := make([]byte, recordPayloadLen)
	for i := range payload {
		payload[i] = fill
	}

	b, err := io.Serialize(nil, offset)
	assert.Nil(t, err)
	b, err = io.Serialize(b, index)
	assert.Nil(t, err)
	b, err = io.Serialize(b, payload)
	assert.Nil(t, err)

	return wal.OffsetIndexBuffer(b)
}

// bars builds n records starting at index 1 (offset 0), i.e. consecutive bars
// in one bucket, which is what a backfill run or a multi-bar batch produces.
func bars(t *testing.T, n int) []wal.OffsetIndexBuffer {
	t.Helper()

	writes := make([]wal.OffsetIndexBuffer, 0, n)
	for i := 0; i < n; i++ {
		writes = append(writes, buf(t, int64(i*recordLen), int64(i+1), byte('a'+i%26)))
	}
	return writes
}

// applySpans writes the coalesced spans into a scratch file image.
func applySpans(size int, spans []wal.Span) []byte {
	image := make([]byte, size)
	for _, s := range spans {
		copy(image[s.Offset:], s.Data)
	}
	return image
}

// applyEach writes every buffer individually, i.e. the pre-coalescing
// behaviour, into a scratch file image.
func applyEach(size int, writes []wal.OffsetIndexBuffer) []byte {
	image := make([]byte, size)
	for _, w := range writes {
		copy(image[w.Offset():], w.IndexAndPayload())
	}
	return image
}

// A run of consecutive bars is the case this exists for: it must collapse to a
// single write regardless of how long the run is. len(spans) is literally the
// number of WriteAt calls writeFixedBuffer will issue.
func TestCoalesceAdjacentCollapsesConsecutiveBarsToOneWrite(t *testing.T) {
	writes := bars(t, 500)

	spans := wal.CoalesceAdjacent(writes)

	assert.Len(t, spans, 1)
	assert.Equal(t, int64(0), spans[0].Offset)
	assert.Len(t, spans[0].Data, 500*recordLen)
}

func TestCoalesceAdjacentSplitsOnGap(t *testing.T) {
	// Two runs of three, separated by one missing bar.
	writes := []wal.OffsetIndexBuffer{
		buf(t, 0*recordLen, 1, 'a'),
		buf(t, 1*recordLen, 2, 'b'),
		buf(t, 2*recordLen, 3, 'c'),
		// index 4 missing
		buf(t, 4*recordLen, 5, 'd'),
		buf(t, 5*recordLen, 6, 'e'),
		buf(t, 6*recordLen, 7, 'f'),
	}

	spans := wal.CoalesceAdjacent(writes)

	assert.Len(t, spans, 2)
	assert.Equal(t, int64(0), spans[0].Offset)
	assert.Len(t, spans[0].Data, 3*recordLen)
	assert.Equal(t, int64(4*recordLen), spans[1].Offset)
	assert.Len(t, spans[1].Data, 3*recordLen)
}

// Unsorted input must still be correct -- it simply coalesces less. Nothing may
// be reordered, because that is what keeps last-write-wins intact.
func TestCoalesceAdjacentDoesNotReorderDescendingOffsets(t *testing.T) {
	writes := []wal.OffsetIndexBuffer{
		buf(t, 2*recordLen, 3, 'c'),
		buf(t, 1*recordLen, 2, 'b'),
		buf(t, 0*recordLen, 1, 'a'),
	}

	spans := wal.CoalesceAdjacent(writes)

	assert.Len(t, spans, 3)
	assert.Equal(t, int64(2*recordLen), spans[0].Offset)
	assert.Equal(t, int64(1*recordLen), spans[1].Offset)
	assert.Equal(t, int64(0*recordLen), spans[2].Offset)
}

// Two commands in one transaction group may target the same offset. The later
// one has to win, so they must not be folded together and their order must
// survive.
func TestCoalesceAdjacentKeepsLastWriteWinsOnDuplicateOffset(t *testing.T) {
	first := buf(t, 0, 1, 'a')
	second := buf(t, 0, 1, 'z')
	writes := []wal.OffsetIndexBuffer{first, second}

	spans := wal.CoalesceAdjacent(writes)

	assert.Len(t, spans, 2)
	assert.Equal(t, applyEach(recordLen, writes), applySpans(recordLen, spans))
	// the survivor is the second write
	assert.Equal(t, byte('z'), applySpans(recordLen, spans)[recordLen-1])
}

// The property that makes this safe: for any input, committing the spans must
// leave the file byte-for-byte identical to committing each record on its own.
func TestCoalesceAdjacentIsEquivalentToPerRecordWrites(t *testing.T) {
	const image = 64 * recordLen

	cases := map[string][]wal.OffsetIndexBuffer{
		"consecutive run": bars(t, 16),
		"single record":   {buf(t, 5*recordLen, 6, 'x')},
		"reverse order": {
			buf(t, 3*recordLen, 4, 'd'),
			buf(t, 2*recordLen, 3, 'c'),
			buf(t, 1*recordLen, 2, 'b'),
		},
		"runs split by gaps": {
			buf(t, 0*recordLen, 1, 'a'),
			buf(t, 1*recordLen, 2, 'b'),
			buf(t, 7*recordLen, 8, 'h'),
			buf(t, 8*recordLen, 9, 'i'),
			buf(t, 20*recordLen, 21, 'u'),
		},
		"duplicate offsets interleaved": {
			buf(t, 0*recordLen, 1, 'a'),
			buf(t, 1*recordLen, 2, 'b'),
			buf(t, 0*recordLen, 1, 'z'),
			buf(t, 2*recordLen, 3, 'c'),
		},
	}

	for name, writes := range cases {
		t.Run(name, func(t *testing.T) {
			spans := wal.CoalesceAdjacent(writes)
			assert.Equal(t, applyEach(image, writes), applySpans(image, spans))
		})
	}
}

func TestCoalesceAdjacentEmptyInput(t *testing.T) {
	assert.Nil(t, wal.CoalesceAdjacent(nil))
	assert.Nil(t, wal.CoalesceAdjacent([]wal.OffsetIndexBuffer{}))
}

// A run of one must not be copied: the span should alias the original buffer.
func TestCoalesceAdjacentSingleRunAliasesInput(t *testing.T) {
	b := buf(t, 0, 1, 'a')

	spans := wal.CoalesceAdjacent([]wal.OffsetIndexBuffer{b})

	assert.Len(t, spans, 1)
	assert.Equal(t, &b.IndexAndPayload()[0], &spans[0].Data[0])
}
