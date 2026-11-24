package corpaction_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
)

var ny, _ = time.LoadLocation("America/New_York")

// startInstance starts an instance in the given timezone over a temp dir.
func startInstance(t *testing.T, tz *time.Location) *catalog.Directory {
	t.Helper()
	prev := utils.InstanceConfig
	t.Cleanup(func() { utils.InstanceConfig = prev })

	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	cfg.Timezone = tz
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcherWithWorkers(nil, 1), executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
	return c.GetCatalogDir()
}

func split(t *testing.T, sym string, y int, m time.Month, d int, from, to float64, st corpaction.Subtype) corpaction.Event {
	t.Helper()
	e, err := corpaction.NewSplit(sym, corpaction.EffectiveDate(y, m, d), from, to, st)
	require.NoError(t, err)
	return e
}

func write(t *testing.T, events ...corpaction.Event) {
	t.Helper()
	csm, err := corpaction.EncodeCSM(events)
	require.NoError(t, err)
	require.NoError(t, executor.WriteCSM(csm, false))
}

func TestNewSplitFactors(t *testing.T) {
	cases := []struct {
		name     string
		from, to float64
		subtype  corpaction.Subtype
		factor   float64
		wantErr  bool
	}{
		{"forward 1:4", 1, 4, corpaction.ForwardSplit, 4, false},
		{"reverse 10:1", 10, 1, corpaction.ReverseSplit, 0.1, false},
		{"stock dividend 1:1.05", 1, 1.05, corpaction.StockDividend, 1.05, false},
		{"zero from", 0, 2, corpaction.ForwardSplit, 0, true},
		{"negative to", 1, -2, corpaction.ForwardSplit, 0, true},
		{"unknown subtype", 1, 2, corpaction.SubtypeUnknown, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := corpaction.NewSplit("AAPL", corpaction.EffectiveDate(2020, 8, 31), tc.from, tc.to, tc.subtype)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tc.factor, e.PriceFactor, 1e-12)
			assert.InDelta(t, tc.factor, e.VolumeFactor, 1e-12)
			assert.Equal(t, tc.subtype, e.Subtype)
			assert.Equal(t, "SPLITS", e.Kind.AttributeGroup())
		})
	}
}

func TestEncodeMergesSameDay(t *testing.T) {
	a := split(t, "XYZ", 2023, 3, 1, 1, 2, corpaction.ForwardSplit)
	b := split(t, "XYZ", 2023, 3, 1, 1, 1.5, corpaction.StockDividend)
	c := split(t, "XYZ", 2022, 1, 3, 1, 3, corpaction.ForwardSplit)
	csm, err := corpaction.EncodeCSM([]corpaction.Event{a, b, c})
	require.NoError(t, err)
	cs := csm[*corpaction.TimeBucketKey("XYZ", corpaction.Splits)]
	require.NotNil(t, cs)
	assert.Equal(t, []int64{c.Effective.Unix(), a.Effective.Unix()}, cs.GetEpoch())
	assert.InDeltaSlice(t, []float64{3, 3}, cs.GetColumn(corpaction.ColPriceFactor), 1e-12)
}

func TestRoundTripKeepsNewYorkMidnight(t *testing.T) {
	for _, tz := range []*time.Location{ny, time.UTC} {
		t.Run(tz.String(), func(t *testing.T) {
			dir := startInstance(t, tz)
			want := split(t, "NVDA", 2024, 6, 10, 1, 10, corpaction.ForwardSplit)
			write(t, want, split(t, "NVDA", 2021, 7, 20, 1, 4, corpaction.ForwardSplit))

			got, err := corpaction.Load(dir, "NVDA", corpaction.Splits)
			require.NoError(t, err)
			require.Len(t, got, 2)
			assert.Equal(t, time.Date(2021, 7, 20, 0, 0, 0, 0, ny).Unix(), got[0].Effective.Unix())
			assert.Equal(t, time.Date(2024, 6, 10, 0, 0, 0, 0, ny).Unix(), got[1].Effective.Unix())
			assert.Equal(t, want.PriceFactor, got[1].PriceFactor)
			assert.Equal(t, corpaction.ForwardSplit, got[1].Subtype)
		})
	}
}

func TestRewriteIsIdempotentAndCorrects(t *testing.T) {
	dir := startInstance(t, ny)
	e := split(t, "NVDA", 2024, 6, 10, 1, 4, corpaction.ForwardSplit)
	write(t, e)
	write(t, e)
	got, err := corpaction.Load(dir, "NVDA")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 4.0, got[0].PriceFactor)

	write(t, split(t, "NVDA", 2024, 6, 10, 1, 10, corpaction.ForwardSplit))
	got, err = corpaction.Load(dir, "NVDA")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 10.0, got[0].PriceFactor)
}

func TestNeutralizeWithdrawnEvent(t *testing.T) {
	dir := startInstance(t, ny)
	keep := split(t, "AAPL", 2020, 8, 31, 1, 4, corpaction.ForwardSplit)
	gone := split(t, "AAPL", 2021, 1, 4, 1, 2, corpaction.ForwardSplit)
	write(t, keep, gone)

	stored, err := corpaction.LoadAll(dir, "AAPL", corpaction.Splits)
	require.NoError(t, err)
	fresh := []corpaction.Event{keep}
	neutral := corpaction.Neutralize(stored, fresh)
	require.Len(t, neutral, 1)
	write(t, append(fresh, neutral...)...)

	got, err := corpaction.Load(dir, "AAPL", corpaction.Splits)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, keep.Effective.Unix(), got[0].Effective.Unix())

	all, err := corpaction.LoadAll(dir, "AAPL", corpaction.Splits)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.True(t, all[1].IsNeutral())

	// Neutralizing again is a no-op.
	assert.Empty(t, corpaction.Neutralize(all, fresh))
}

func TestLoadMissingBucket(t *testing.T) {
	dir := startInstance(t, ny)
	got, err := corpaction.Load(dir, "MSFT", corpaction.Splits)
	assert.NoError(t, err)
	assert.Empty(t, got)
}
