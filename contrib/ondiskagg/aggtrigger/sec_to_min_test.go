package aggtrigger

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// recordsForTF is recordsFor with a configurable timeframe.
func recordsForTF(t testing.TB, cs *io.ColumnSeries, tbk *io.TimeBucketKey, tf time.Duration) []trigger.Record {
	t.Helper()
	rs, err := cs.ToRowSeries(*tbk, true)
	require.Nil(t, err)
	rowData := rs.GetData()
	times, err := rs.GetTime()
	require.Nil(t, err)
	rowLen := len(rowData) / len(times)
	records := make([]trigger.Record, len(times))
	for i := range times {
		buf, _ := io.Serialize(nil, io.TimeToIndex(times[i], tf))
		buf = append(buf, rowData[i*rowLen+8:(i+1)*rowLen]...)
		records[i] = trigger.Record(buf)
	}
	return records
}

func readBucket(t testing.TB, key string, from, to time.Time) *io.ColumnSeries {
	t.Helper()
	q := planner.NewQuery(executor.ThisInstance.CatalogDir)
	tbk := io.NewTimeBucketKey(key)
	q.AddTargetKey(tbk)
	q.SetRange(from, to)
	parsed, err := q.Parse()
	require.Nil(t, err)
	r, err := executor.NewReader(parsed)
	require.Nil(t, err)
	csm, err := r.Read()
	require.Nil(t, err)
	return csm[*tbk]
}

// TestSecToMinMatchesSum: the live 1Sec -> 1Min cascade, one bar per write
// and per fire (as the websocket handler writes them). Every 1Min bar must
// carry exactly the volume of the 1Sec bars inside it.
func TestSecToMinMatchesSum(t *testing.T) {
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")
	tz := utils.InstanceConfig.Timezone
	rootDir := filepath.Join(t.TempDir(), "mktsdb")
	_ = os.MkdirAll(rootDir, 0o777)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	trig, err := NewTrigger(map[string]interface{}{"destinations": []string{"1Min"}})
	require.Nil(t, err)

	tbk := io.NewTimeBucketKey("TEST/1Sec/OHLCV")
	start := time.Date(2024, 3, 5, 10, 0, 0, 0, tz)
	rng := rand.New(rand.NewSource(1))
	want := map[int64]int64{}
	for sec := 0; sec < 180; sec++ {
		if rng.Intn(3) == 0 {
			continue // no trade this second
		}
		ts := start.Add(time.Duration(sec) * time.Second)
		vol := int64(1 + rng.Intn(1000))
		want[ts.Unix()-ts.Unix()%60] += vol

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{ts.Unix()})
		cs.AddColumn("Open", []float32{10})
		cs.AddColumn("High", []float32{11})
		cs.AddColumn("Low", []float32{9})
		cs.AddColumn("Close", []float32{10.5})
		cs.AddColumn("Volume", []int64{vol})
		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)
		require.Nil(t, executor.WriteCSM(csm, false))
		trig.Fire("TEST/1Sec/OHLCV/2024.bin", recordsForTF(t, cs, tbk, time.Second))
	}

	got := readBucket(t, "TEST/1Min/OHLCV", start, start.Add(time.Hour))
	require.NotNil(t, got)
	gv := got.GetColumn("Volume").([]int64)
	for i, e := range got.GetEpoch() {
		require.Equal(t, want[e], gv[i], "minute %s", time.Unix(e, 0).In(tz).Format("15:04"))
	}
	require.Len(t, gv, len(want))
}
