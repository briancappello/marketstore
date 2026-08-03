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
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestFireRewrittenBarSupersedesCache covers the live cascade's central
// behaviour: a source bar is REWRITTEN as more data lands inside it.
//
// 1Sec/OHLCV -> 1Min re-emits the current minute on every 1Sec batch, each
// time with a larger cumulative volume. Stage 3 (1Min -> 1D) must aggregate
// the newest version of that minute, not the first one it happened to see.
//
// The cached path did `ColumnSeriesUnion(batch, cache)`, and the union lets
// the RIGHT side win on duplicate epochs -- so the stale cached minute
// overwrote the fresh rewrite and the daily bar kept each minute's first,
// nearly-empty value.
func TestFireRewrittenBarSupersedesCache(t *testing.T) {
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
	minute := time.Date(2024, 3, 5, 9, 30, 0, 0, tz)

	writeBar := func(ts time.Time, vol float32) {
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
		trig.Fire("TEST/1Min/OHLCV/2024.bin", recordsFor(t, cs, tbk))
	}

	// A second minute first, so the cache is populated and the rewrite below
	// takes the cached path rather than the full-day query path.
	writeBar(minute, 10)
	writeBar(minute.Add(time.Minute), 20)

	// Now the first minute is rewritten as more 1Sec data lands in it.
	writeBar(minute, 1000)

	catalogDir := executor.ThisInstance.CatalogDir
	q := planner.NewQuery(catalogDir)
	tbk1D := io.NewTimeBucketKey("TEST/1D/OHLCV")
	q.AddTargetKey(tbk1D)
	q.SetRange(minute.Add(-24*time.Hour), minute.Add(24*time.Hour))
	parsed, err := q.Parse()
	require.Nil(t, err)
	scanner, err := executor.NewReader(parsed)
	require.Nil(t, err)
	csm, err := scanner.Read()
	require.Nil(t, err)

	got := csm[*tbk1D]
	require.NotNil(t, got, "no 1D bar was written")
	vols := got.GetColumn("Volume").([]float32)
	require.Len(t, vols, 1)

	// 1000 (rewritten first minute) + 20 (second minute)
	t.Logf("daily volume: got %.0f want 1020", vols[0])
	assert.InDelta(t, float32(1020), vols[0], 0.001,
		"daily aggregate kept the stale cached bar instead of the rewritten one")
}
