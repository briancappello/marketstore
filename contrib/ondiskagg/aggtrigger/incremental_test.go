package aggtrigger

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/internal/di"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// recordsFor converts a ColumnSeries into trigger records the way the
// executor's trigger dispatcher does.
func recordsFor(t *testing.T, cs *io.ColumnSeries, tbk *io.TimeBucketKey) []trigger.Record {
	t.Helper()
	rs, err := cs.ToRowSeries(*tbk, true)
	require.Nil(t, err)
	rowData := rs.GetData()
	times, err := rs.GetTime()
	require.Nil(t, err)
	numRows := len(times)
	rowLen := len(rowData) / numRows

	records := make([]trigger.Record, numRows)
	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := rowData[pos : pos+rowLen]
		index := io.TimeToIndex(times[i], time.Minute)
		buf, _ := io.Serialize(nil, index)
		buf = append(buf, record[8:]...)
		records[i] = trigger.Record(buf)
	}
	return records
}

// TestFireIncrementalDailyAggregate reproduces the live-streaming pattern:
// 1Min bars arrive one at a time and the trigger fires per write, rather
// than once with the whole session. The resulting 1D bar must still
// aggregate the ENTIRE day, not just the most recent batch.
//
// Observed in production: AAPL's intraday 1D volume was 109,704 against a
// true 1,347,413 (8%) because the cached path unioned only the current
// batch onto a cache that had itself been sliced down.
func TestFireIncrementalDailyAggregate(t *testing.T) {
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")
	tz := utils.InstanceConfig.Timezone

	rootDir := filepath.Join(t.TempDir(), "mktsdb")
	_ = os.MkdirAll(rootDir, 0o777)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	// Mirrors mkts.yml Stage 3: */1Min/OHLCV -> 1D, no filter.
	trig, err := NewTrigger(map[string]interface{}{
		"destinations": []string{"1D"},
	})
	require.Nil(t, err)

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLCV")
	const numBars = 60
	base := time.Date(2024, 3, 5, 9, 30, 0, 0, tz)

	var wantVolume float32
	for i := 0; i < numBars; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		vol := float32(100 + i)
		wantVolume += vol

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{ts.Unix()})
		cs.AddColumn("Open", []float32{10})
		cs.AddColumn("High", []float32{11})
		cs.AddColumn("Low", []float32{9})
		cs.AddColumn("Close", []float32{10.5})
		cs.AddColumn("Volume", []float32{vol})

		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)
		require.Nil(t, executor.WriteCSM(csm, false))

		// Fire with ONLY this bar, as the live dispatcher does.
		trig.Fire("TEST/1Min/OHLCV/2024.bin", recordsFor(t, cs, tbk))
	}

	// Read back the daily bar.
	catalogDir := executor.ThisInstance.CatalogDir
	q := planner.NewQuery(catalogDir)
	tbk1D := io.NewTimeBucketKey("TEST/1D/OHLCV")
	q.AddTargetKey(tbk1D)
	q.SetRange(base.Add(-24*time.Hour), base.Add(24*time.Hour))
	parsed, err := q.Parse()
	require.Nil(t, err)
	scanner, err := executor.NewReader(parsed)
	require.Nil(t, err)
	csm, err := scanner.Read()
	require.Nil(t, err)

	got := csm[*tbk1D]
	require.NotNil(t, got, "no 1D bar was written")
	vols := got.GetColumn("Volume").([]float32)
	require.Len(t, vols, 1, "expected exactly one daily bar")

	assert.InDelta(t, wantVolume, vols[0], 0.001,
		"daily bar must aggregate the whole session, not just the last batch")
}
