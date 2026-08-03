package aggtrigger

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/internal/di"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestFireConcurrentDailyAggregate reproduces the production dispatcher,
// which fires triggers in a goroutine PER WRITE BATCH (executor/written.go:45).
//
// Fire() read-modify-writes aggCache: Load, union with the batch, then Store
// via defer. sync.Map makes each op safe but not the sequence, so two
// concurrent fires both load cache C and each stores "C + own batch" --
// last writer wins and the other batch's bars are lost.
func TestFireConcurrentDailyAggregate(t *testing.T) {
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")
	tz := utils.InstanceConfig.Timezone

	rootDir := filepath.Join(t.TempDir(), "mktsdb")
	_ = os.MkdirAll(rootDir, 0o777)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	trig, err := NewTrigger(map[string]interface{}{"destinations": []string{"1D"}})
	require.Nil(t, err)

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLCV")
	const numBars = 120
	base := time.Date(2024, 3, 5, 9, 30, 0, 0, tz)

	var wantVolume float32
	batches := make([]*io.ColumnSeries, numBars)
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
		batches[i] = cs

		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)
		require.Nil(t, executor.WriteCSM(csm, false))
	}

	// Fire in overlapping pairs, as the dispatcher does: a goroutine per
	// write batch, a few in flight at once. Two concurrent fires are all it
	// takes to lose an update.
	const inFlight = 16
	for i := 0; i < numBars; i += inFlight {
		var wg sync.WaitGroup
		for j := i; j < i+inFlight && j < numBars; j++ {
			wg.Add(1)
			go func(cs *io.ColumnSeries) {
				defer wg.Done()
				trig.Fire("TEST/1Min/OHLCV/2024.bin", recordsFor(t, cs, tbk))
			}(batches[j])
		}
		wg.Wait()
	}

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

	t.Logf("daily volume: got %.0f want %.0f (%.1f%%)", vols[0], wantVolume, 100*vols[0]/wantVolume)
	assert.InDelta(t, wantVolume, vols[0], 0.001,
		"concurrent fires lost bars: daily aggregate must cover the whole session")
}
