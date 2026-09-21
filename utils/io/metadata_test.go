package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/utils"
)

// quoteShapes mirrors the real QUOTE attrgroup that triggered the production
// incident: a variable-length bucket whose configured schema also listed a
// "Nanoseconds" column.
func quoteShapes(withNanoseconds bool) []DataShape {
	names := []string{"Epoch", "AskPrice", "BidPrice", "AskSize", "BidSize"}
	types := []EnumElementType{INT64, FLOAT64, FLOAT64, UINT64, UINT64}
	if withNanoseconds {
		names = append(names, "Nanoseconds")
		types = append(types, INT32)
	}
	return NewDataShapeVector(names, types)
}

func elementNamesOf(tbi *TimeBucketInfo) []string {
	return tbi.GetElementNames()
}

// TestNewTimeBucketInfo_VariableDropsNanoseconds is the regression test for the
// incident where a VARIABLE bucket header carried a "Nanoseconds" element.
// Writers strip that column from every incoming ColumnSeries, so such a header
// describes a bucket no write can ever satisfy: every WriteCSM fails the column
// count check, and on a replica that error is non-retryable and permanently
// kills the replication stream.
func TestNewTimeBucketInfo_VariableDropsNanoseconds(t *testing.T) {
	t.Parallel()

	tbi := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
		"variable with Nanoseconds", 2026, quoteShapes(true), VARIABLE)

	assert.NotContains(t, elementNamesOf(tbi), "Nanoseconds",
		"a VARIABLE bucket header must never carry a Nanoseconds element")
	assert.Equal(t, []string{"AskPrice", "BidPrice", "AskSize", "BidSize"}, elementNamesOf(tbi))
	assert.Equal(t, int32(4), tbi.GetNelements())
}

// TestNewTimeBucketInfo_FixedKeepsNanoseconds guards the opposite direction.
// The invariant is conditional on record type: a FIXED bucket may legitimately
// store a sub-second offset as a real column (ColumnSeries.GetTime reads it),
// and stripping it there would silently corrupt tick data.
func TestNewTimeBucketInfo_FixedKeepsNanoseconds(t *testing.T) {
	t.Parallel()

	tbi := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
		"fixed with Nanoseconds", 2026, quoteShapes(true), FIXED)

	assert.Contains(t, elementNamesOf(tbi), "Nanoseconds",
		"a FIXED bucket may legitimately carry a Nanoseconds column")
	assert.Equal(t, int32(5), tbi.GetNelements())
}

// TestNewTimeBucketInfo_VariableDropsNanosecondsAnyCase covers a differently
// cased column name. Writers remove the canonically spelled "Nanoseconds", so a
// header element spelled "nanoseconds" is equally unsatisfiable.
func TestNewTimeBucketInfo_VariableDropsNanosecondsAnyCase(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{"nanoseconds", "NANOSECONDS", "NanoSeconds"} {
		spelling := spelling
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()
			dsv := NewDataShapeVector(
				[]string{"Epoch", "Price", spelling},
				[]EnumElementType{INT64, FLOAT64, INT32},
			)
			tbi := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
				"variable", 2026, dsv, VARIABLE)

			assert.Equal(t, []string{"Price"}, elementNamesOf(tbi))
		})
	}
}

// TestNewTimeBucketInfo_VariableWithoutNanosecondsUnchanged makes sure the
// strip is inert on well-formed input and does not disturb column ordering.
func TestNewTimeBucketInfo_VariableWithoutNanosecondsUnchanged(t *testing.T) {
	t.Parallel()

	dsv := quoteShapes(false)
	tbi := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
		"variable clean", 2026, dsv, VARIABLE)

	assert.Equal(t, []string{"AskPrice", "BidPrice", "AskSize", "BidSize"}, elementNamesOf(tbi))
	assert.Len(t, dsv, 5, "the caller's slice must not be mutated")
}

// TestNewTimeBucketInfo_NanosecondsNotPersistedToHeader proves the column is
// gone from the bytes on disk, not merely from the in-memory struct. The
// production failure read the header back from a file, so an in-memory-only
// fix would not have prevented it.
func TestNewTimeBucketInfo_NanosecondsNotPersistedToHeader(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tbi := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), dir,
		"variable with Nanoseconds", 2026, quoteShapes(true), VARIABLE)

	fp, err := os.Create(tbi.Path)
	require.Nil(t, err)
	require.Nil(t, WriteHeader(fp, tbi))
	require.Nil(t, fp.Close())

	// Re-read the header straight off disk, the way the catalog scan does.
	reloaded := TimeBucketInfo{Year: 2026, Path: filepath.Join(dir, "2026.bin")}
	assert.NotContains(t, reloaded.GetElementNames(), "Nanoseconds")
	assert.Equal(t, int32(4), reloaded.GetNelements())
	assert.Equal(t, VARIABLE, reloaded.GetRecordType())
}

// TestNewTimeBucketInfo_VariableRecordLengthExcludesNanoseconds checks the
// derived record length agrees with the stripped schema. A stale length would
// mis-slice every variable record read back from the file.
func TestNewTimeBucketInfo_VariableRecordLengthExcludesNanoseconds(t *testing.T) {
	t.Parallel()

	withNs := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
		"with", 2026, quoteShapes(true), VARIABLE)
	withoutNs := NewTimeBucketInfo(*utils.NewTimeframe("1Sec"), t.TempDir(),
		"without", 2026, quoteShapes(false), VARIABLE)

	assert.Equal(t, withoutNs.GetVariableRecordLength(), withNs.GetVariableRecordLength(),
		"a config that lists Nanoseconds must produce the same record length as one that does not")
}
