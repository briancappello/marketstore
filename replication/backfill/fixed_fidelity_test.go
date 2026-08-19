package backfill_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestFixedRoundTripIsIdempotent proves the premise the backfill relies on for
// the buckets it actually handles: for FIXED (OHLCV) records, re-writing data
// read from a Query overwrites by epoch — it does NOT duplicate rows. This is
// why the backfill can overlap the live stream and re-pull a lookback window
// freely. (Variable-length buckets, which append instead, are excluded from
// backfill — see TestDriverReconcileSkipsVariableBuckets.)
func TestFixedRoundTripIsIdempotent(t *testing.T) {
	rootDir := t.TempDir()
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	catDir := c.GetCatalogDir()
	metadata := executor.NewInstanceSetup(catDir, c.GetInitWALFile())

	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	base := time.Date(2026, 6, 22, 14, 30, 0, 0, time.UTC)

	write := func(csm io.ColumnSeriesMap) {
		w, werr := executor.NewWriter(catDir, metadata.WALFile)
		require.Nil(t, werr)
		require.Nil(t, w.WriteCSM(csm, false))
		require.Nil(t, metadata.WALFile.FlushToWAL())
		require.Nil(t, metadata.WALFile.CreateCheckpoint())
	}
	readRange := func() io.ColumnSeriesMap {
		q := planner.NewQuery(catDir)
		q.AddTargetKey(tbk)
		q.SetRange(base.Add(-time.Hour), base.Add(time.Hour))
		parsed, perr := q.Parse()
		require.Nil(t, perr)
		r, rerr := executor.NewReader(parsed)
		require.Nil(t, rerr)
		csm, rerr := r.Read()
		require.Nil(t, rerr)
		return csm
	}

	const n = 5
	epochs := make([]int64, n)
	opens := make([]float32, n)
	for i := 0; i < n; i++ {
		epochs[i] = base.Add(time.Duration(i) * time.Minute).Unix()
		opens[i] = 100.0 + float32(i)
	}
	seed := io.NewColumnSeries()
	seed.AddColumn("Epoch", epochs)
	seed.AddColumn("Open", opens)
	seed.AddColumn("High", opens)
	seed.AddColumn("Low", opens)
	seed.AddColumn("Close", opens)
	seed.AddColumn("Volume", make([]int64, n))
	seedCSM := io.NewColumnSeriesMap()
	seedCSM.AddColumnSeries(*tbk, seed)
	write(seedCSM)

	csmA := readRange()
	require.NotEmpty(t, csmA)
	require.Len(t, csmA[*tbk].GetEpoch(), n)

	// Re-write what we read (as the backfill would on an overlapping re-pull),
	// then read again. FIXED records overwrite by epoch: no duplication.
	write(csmA)
	csmB := readRange()

	require.Len(t, csmB[*tbk].GetEpoch(), n, "re-writing FIXED data must not duplicate rows")
	opt := cmp.AllowUnexported(io.ColumnSeries{})
	if !cmp.Equal(csmA, csmB, opt) {
		t.Fatalf("fixed round-trip not idempotent: diff:\n%s", cmp.Diff(csmA, csmB, opt))
	}
}
