package replication_test

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	offset = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	// index starts from 1.
	index = []byte{1, 0, 0, 0, 0, 0, 0, 0}
	// tbk=AMMZN:1Min:OHLC, year=2020, epoch=2020-01-01 00:00:00
	// Open: 1, High: 2, Low: 3, Close: 4.
	buffer32 = []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
	}
	recordSize         = int32(32)
	variableRecordDate = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
)

func makeMockOffsetIndexBuffer(offset, index, buffer []byte) wal.OffsetIndexBuffer {
	var oib []byte
	oib = append(oib, offset...)
	oib = append(oib, index...)
	oib = append(oib, buffer...)
	return oib
}

func makeMockOffsetIndexBufferVariable(t time.Time, tf *utils.Timeframe, buffer []byte) wal.OffsetIndexBuffer {
	// Variable Length Record has IntervalTicks(4bytes) at the end of the buffer
	var oib []byte
	index := io.TimeToIndex(t, tf.Duration)
	offset := io.IndexToOffset(index, recordSize)

	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))
	oib = append(oib, offsetBytes...)

	indexBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(indexBytes, uint64(index))
	oib = append(oib, indexBytes...)

	oib = append(oib, buffer...)

	intervalsPerDay := 24 * time.Hour.Nanoseconds() / tf.Duration.Nanoseconds()
	intervalTicks := io.GetIntervalTicks32Bit(t, index, intervalsPerDay)
	intervalTicksBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(intervalTicksBytes, intervalTicks)
	oib = append(oib, intervalTicksBytes...)

	return oib
}

func makeMockOHLCColumnSeries(epoch time.Time, open, high, low, clos int64) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch.Unix()})
	cs.AddColumn("Open", []int64{open})
	cs.AddColumn("High", []int64{high})
	cs.AddColumn("Low", []int64{low})
	cs.AddColumn("Close", []int64{clos})

	if epoch.Nanosecond() != 0 {
		cs.AddColumn("Nanoseconds", []int32{int32(epoch.Nanosecond())})
	}
	return cs
}

// ohlcShapes is the shared column layout for the mixed-record-type fixtures.
func ohlcShapes() []io.DataShape {
	return []io.DataShape{
		{Name: "Epoch", Type: io.INT64},
		{Name: "Open", Type: io.INT64},
		{Name: "High", Type: io.INT64},
		{Name: "Low", Type: io.INT64},
		{Name: "Close", Type: io.INT64},
	}
}

func fixedWTSet(filePath string) wal.WTSet {
	return wal.WTSet{
		RecordType: io.FIXED,
		FilePath:   filePath,
		DataLen:    32,
		Buffer:     makeMockOffsetIndexBuffer(offset, index, buffer32),
		DataShapes: ohlcShapes(),
	}
}

func variableWTSet(filePath string) wal.WTSet {
	return wal.WTSet{
		RecordType: io.VARIABLE,
		FilePath:   filePath,
		DataLen:    24,
		VarRecLen:  32 + 4, // Open,High,Low,Close + IntervalTicks(4bytes)
		Buffer: makeMockOffsetIndexBufferVariable(
			variableRecordDate, utils.TimeframeFromDuration(1*time.Second), buffer32,
		),
		DataShapes: ohlcShapes(),
	}
}

// TestReplayerImpl_Replay_MixedRecordTypes is the regression test for a replay
// that labelled every write transaction set with wtsets[0]'s record type.
//
// A transaction group is a flush of the write channel, so it can carry sets for
// several buckets at once and freely mix FIXED and VARIABLE records.
// ParseTGData decodes a record type per set, and local WAL replay switches on
// it per set. Reading index 0 for all of them mislabels every set after the
// first whenever a group is mixed, so WriteCSM strips (or fails to strip) the
// Nanoseconds column against the wrong schema. The resulting column mismatch is
// non-retryable and permanently kills the replication stream.
func TestReplayerImpl_Replay_MixedRecordTypes(t *testing.T) {
	t.Parallel()

	const (
		variableBucket = "AMZN/1Sec/OHLC"
		fixedBucket    = "MSFT/1Min/OHLC"
	)

	tests := []struct {
		name   string
		wtSets []wal.WTSet
	}{
		{
			// VARIABLE first: the bug marked the trailing FIXED set as variable.
			name: "variable first then fixed",
			wtSets: []wal.WTSet{
				variableWTSet("/data/AMZN/1Sec/OHLC/2020.bin"),
				fixedWTSet("/data/MSFT/1Min/OHLC/2020.bin"),
			},
		},
		{
			// FIXED first: the bug marked the trailing VARIABLE set as fixed.
			name: "fixed first then variable",
			wtSets: []wal.WTSet{
				fixedWTSet("/data/MSFT/1Min/OHLC/2020.bin"),
				variableWTSet("/data/AMZN/1Sec/OHLC/2020.bin"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			// Record the isVariableLength flag the replayer chose per bucket.
			got := map[string]bool{}
			writeFunc := func(csm io.ColumnSeriesMap, isVariableLength bool) error {
				for tbk := range csm {
					got[tbk.GetItemKey()] = isVariableLength
				}
				return nil
			}
			parseTGFunc := func(_ []byte, _ string) (int64, []wal.WTSet) {
				return 1, tt.wtSets
			}

			r := replication.NewReplayer(parseTGFunc, writeFunc, "/file/path")

			// --- when ---
			if err := r.Replay(nil); err != nil {
				t.Fatalf("Replay() unexpected error: %v", err)
			}

			// --- then ---
			// Each set must be written with ITS OWN record type, regardless of
			// where it sits in the group.
			if len(got) != 2 {
				t.Fatalf("expected both buckets to be written, got %v", got)
			}
			if !got[variableBucket] {
				t.Errorf("bucket %s is a VARIABLE set but was written with isVariableLength=false",
					variableBucket)
			}
			if got[fixedBucket] {
				t.Errorf("bucket %s is a FIXED set but was written with isVariableLength=true",
					fixedBucket)
			}
		})
	}
}

func TestReplayerImpl_Replay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		wtSets               []wal.WTSet
		writeErr             bool
		wantCSM              io.ColumnSeriesMap
		wantIsVariableLength bool
		wantErr              bool
	}{
		{
			name: "success/Fixed Length record",
			wtSets: []wal.WTSet{
				{
					RecordType: io.FIXED,
					FilePath:   "/data/AMZN/1Min/OHLC/2020.bin",
					DataLen:    32,
					Buffer:     makeMockOffsetIndexBuffer(offset, index, buffer32),
					DataShapes: []io.DataShape{
						{Name: "Epoch", Type: io.INT64},
						{Name: "Open", Type: io.INT64},
						{Name: "High", Type: io.INT64},
						{Name: "Low", Type: io.INT64},
						{Name: "Close", Type: io.INT64},
					},
				},
			},
			writeErr: false,
			wantCSM: io.ColumnSeriesMap(
				map[io.TimeBucketKey]*io.ColumnSeries{
					*io.NewTimeBucketKey("AMZN/1Min/OHLC"): makeMockOHLCColumnSeries(
						time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						1, 2, 3, 4,
					),
				},
			),
			wantIsVariableLength: false,
			wantErr:              false,
		},
		{
			name: "success/Variable Length record",
			wtSets: []wal.WTSet{
				{
					RecordType: io.VARIABLE,
					FilePath:   "/data/AMZN/1Sec/OHLC/2020.bin",
					DataLen:    24,
					VarRecLen:  32 + 4, // Open,High,Low,Close + IntervalTicks(4bytes)
					Buffer: makeMockOffsetIndexBufferVariable(
						variableRecordDate, utils.TimeframeFromDuration(1*time.Second), buffer32,
					),
					DataShapes: []io.DataShape{
						{Name: "Epoch", Type: io.INT64},
						{Name: "Open", Type: io.INT64},
						{Name: "High", Type: io.INT64},
						{Name: "Low", Type: io.INT64},
						{Name: "Close", Type: io.INT64},
					},
				},
			},
			writeErr: false,
			wantCSM: io.ColumnSeriesMap(
				map[io.TimeBucketKey]*io.ColumnSeries{
					*io.NewTimeBucketKey("AMZN/1Sec/OHLC"): makeMockOHLCColumnSeries(variableRecordDate, 1, 2, 3, 4),
				},
			),
			wantIsVariableLength: true,
			wantErr:              false,
		},
		{
			name:                 "empty WTset parsed/no error is returned",
			wtSets:               []wal.WTSet{},
			writeErr:             false,
			wantCSM:              nil,
			wantIsVariableLength: false,
			wantErr:              false,
		},
		{
			name: "invalid filepath in WTset/error is returned",
			wtSets: []wal.WTSet{
				{
					RecordType: io.FIXED,
					FilePath:   "/invalidFilePath/2020.bin",
					DataLen:    32,
					Buffer:     makeMockOffsetIndexBuffer(offset, index, buffer32),
					DataShapes: []io.DataShape{
						{Name: "Epoch", Type: io.INT64},
						{Name: "Open", Type: io.INT64},
						{Name: "High", Type: io.INT64},
						{Name: "Low", Type: io.INT64},
						{Name: "Close", Type: io.INT64},
					},
				},
			},
			writeErr:             false,
			wantCSM:              nil,
			wantIsVariableLength: false,
			wantErr:              true,
		},
		{
			name: "invalid timeframe in WTset/error is returned",
			wtSets: []wal.WTSet{
				{
					RecordType: io.FIXED,
					FilePath:   "/data/AMZN/1InvalidTimeframe/OHLC/2020.bin",
					DataLen:    32,
					Buffer:     makeMockOffsetIndexBuffer(offset, index, buffer32),
					DataShapes: []io.DataShape{
						{Name: "Epoch", Type: io.INT64},
						{Name: "Open", Type: io.INT64},
						{Name: "High", Type: io.INT64},
						{Name: "Low", Type: io.INT64},
						{Name: "Close", Type: io.INT64},
					},
				},
			},
			writeErr:             false,
			wantCSM:              nil,
			wantIsVariableLength: false,
			wantErr:              true,
		},
		{
			name: "error/Variable Length record length is 0 (=bug)",
			wtSets: []wal.WTSet{
				{
					RecordType: io.VARIABLE,
					FilePath:   "/data/AMZN/1Sec/OHLC/2020.bin",
					DataLen:    24,
					VarRecLen:  0,
					Buffer: makeMockOffsetIndexBufferVariable(
						variableRecordDate, utils.TimeframeFromDuration(1*time.Second), buffer32,
					),
					DataShapes: []io.DataShape{},
				},
			},
			writeErr:             false,
			wantCSM:              nil,
			wantIsVariableLength: true,
			wantErr:              true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			// mock function to assert if expected parameters are passed to the writeCSM function
			writeFunc := func(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
				if tt.writeErr {
					return errors.New("some error")
				}

				opt := cmp.AllowUnexported(io.ColumnSeries{})
				if !cmp.Equal(csm, tt.wantCSM, opt) {
					t.Errorf("Replayed CSM: diff:%v", cmp.Diff(tt.wantCSM, csm, opt))
				}

				if isVariableLength != tt.wantIsVariableLength {
					t.Errorf("Replayed CSM: want= %v, got=%v", tt.wantCSM, csm)
				}

				return nil
			}

			// mock function to control the parsed WTSets
			parseTGFunc := func(TG_Serialized []byte, rootPath string) (TGID int64, wtSets []wal.WTSet) {
				return 1, tt.wtSets
			}

			r := replication.NewReplayer(parseTGFunc, writeFunc, "/file/path")

			// --- when ---
			err := r.Replay(nil)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("Replay() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
