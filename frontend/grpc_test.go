package frontend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// A query that matches no data leaves GRPCService.Query's accumulator nil, which used to
// segfault the server (SIGSEGV in ToProtoNumpyMultiDataSet). An empty result must convert
// to a well-formed empty message instead.
func TestToProtoNumpyMultiDataSetNilIsEmptyNotPanic(t *testing.T) {
	t.Parallel()

	got := frontend.ToProtoNumpyMultiDataSet(nil)

	assert.NotNil(t, got)
	assert.NotNil(t, got.Data)
	assert.Equal(t, int32(0), got.Data.Length)
	assert.Empty(t, got.Data.ColumnTypes)
	assert.Empty(t, got.Data.ColumnNames)
	assert.Empty(t, got.Data.ColumnData)
	assert.Empty(t, got.StartIndex)
	assert.Empty(t, got.Lengths)
}

func TestToProtoNumpyMultiDataSetPreservesData(t *testing.T) {
	t.Parallel()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{1_600_000_000, 1_600_000_060})
	cs.AddColumn("Ask", []float32{1.5, 2.5})

	nds, err := io.NewNumpyDataset(cs)
	assert.Nil(t, err)

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLCV")
	nmds, err := io.NewNumpyMultiDataset(nds, *tbk)
	assert.Nil(t, err)

	got := frontend.ToProtoNumpyMultiDataSet(nmds)

	assert.NotNil(t, got)
	assert.Equal(t, int32(2), got.Data.Length)
	assert.Equal(t, nmds.ColumnNames, got.Data.ColumnNames)
	assert.Equal(t, nmds.ColumnTypes, got.Data.ColumnTypes)
	// The map is keyed by the TimeBucketKey's full string form
	// ("TEST/1Min/OHLCV:Symbol/Timeframe/AttributeGroup").
	assert.Len(t, got.Lengths, 1)
	assert.Equal(t, int32(2), got.Lengths[tbk.String()])
	assert.Equal(t, int32(0), got.StartIndex[tbk.String()])
}
