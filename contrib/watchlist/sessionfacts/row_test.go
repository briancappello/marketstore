package sessionfacts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
)

func TestCompute(t *testing.T) {
	t.Parallel()
	ds, err := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	require.NoError(t, err)

	bar := func(hh, mm int, c float64, v int64) sessionfacts.Bar {
		return sessionfacts.Bar{Epoch: et(2026, 9, 22, hh, mm).Unix(), Close: c, Volume: v}
	}
	bars := []sessionfacts.Bar{
		bar(3, 59, 1, 999), // before premarket: ignored
		bar(9, 0, 10.5, 200),
		bar(4, 0, 10, 100), // out of order
		bar(9, 30, 11, 1_000),
		bar(15, 59, 12, 3_000),
		bar(9, 30, 11.1, 2_000), // rewrite of 09:30: replaces it
		bar(16, 0, 12.5, 50),
		bar(20, 0, 99, 999), // after afterhours: ignored
	}
	r := sessionfacts.Compute(ds, bars)

	assert.True(t, r.Date.Equal(et(2026, 9, 22, 0, 0)))
	assert.Equal(t, sessionfacts.Version, r.Version)
	assert.Equal(t, int64(300), r.PreVolume)
	assert.Equal(t, int64(5_000), r.RegVolume, "the rewritten 09:30 bar counts once, at its latest volume")
	assert.Equal(t, int64(50), r.PostVolume)
	assert.Equal(t, 10.5, r.PreClose, "the latest premarket bar, not the last one in the slice")
	assert.Equal(t, 12.0, r.RegClose)
	assert.Equal(t, 12.5, r.PostClose)
	assert.Equal(t, [3]int32{2, 2, 1}, [3]int32{r.PreBars, r.RegBars, r.PostBars})

	c, ok := r.Close(calendar.Afterhours)
	assert.True(t, ok)
	assert.Equal(t, 12.5, c)
}

func TestComputeEmptySessionIsRecorded(t *testing.T) {
	t.Parallel()
	ds, err := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	require.NoError(t, err)
	r := sessionfacts.Compute(ds, []sessionfacts.Bar{{Epoch: et(2026, 9, 22, 10, 0).Unix(), Close: 5, Volume: 1}})
	_, ok := r.Close(calendar.Afterhours)
	assert.False(t, ok, "no afterhours bars")
	assert.Equal(t, int32(0), r.PostBars)
	assert.True(t, r.Current(), "the row itself exists and is current")
}

func TestWriteReadRoundTrip(t *testing.T) {
	startInstance(t)
	catDir := executor.ThisInstance.CatalogDir

	d1, err := calendar.Nasdaq.SessionBounds(2026, 9, 21)
	require.NoError(t, err)
	d2, err := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	require.NoError(t, err)
	rows := []sessionfacts.Row{
		sessionfacts.Compute(d2, []sessionfacts.Bar{{Epoch: et(2026, 9, 22, 17, 0).Unix(), Close: 3.25, Volume: 7}}),
		sessionfacts.Compute(d1, []sessionfacts.Bar{
			{Epoch: et(2026, 9, 21, 8, 0).Unix(), Close: 1.5, Volume: 10},
			{Epoch: et(2026, 9, 21, 10, 0).Unix(), Close: 2.5, Volume: 20},
		}),
	}
	require.NoError(t, sessionfacts.Write(executor.WriteCSM, map[string][]sessionfacts.Row{"RT": rows}))

	got, err := sessionfacts.Read(catDir, "RT", et(2026, 9, 1, 0, 0), et(2026, 9, 30, 0, 0))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, rows[1], got[0], "rows come back in date order and unchanged")
	assert.Equal(t, rows[0], got[1])

	none, err := sessionfacts.Read(catDir, "NOPE", et(2026, 9, 1, 0, 0), et(2026, 9, 30, 0, 0))
	require.NoError(t, err)
	assert.Empty(t, none, "a symbol without a SESSIONS bucket has no rows")
}

func TestComputeRangeFromDisk(t *testing.T) {
	startInstance(t)
	catDir := executor.ThisInstance.CatalogDir
	writeMinutes(t, "CR",
		minuteBar{et(2026, 9, 21, 9, 0), 1, 10},
		minuteBar{et(2026, 9, 21, 19, 59), 2, 20},
		minuteBar{et(2026, 9, 22, 4, 0), 3, 30},
		minuteBar{et(2026, 9, 22, 12, 0), 4, 40},
	)
	d1, _ := calendar.Nasdaq.SessionBounds(2026, 9, 21)
	d2, _ := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	d3, _ := calendar.Nasdaq.SessionBounds(2026, 9, 23)

	rows, err := sessionfacts.ComputeRange(catDir, "CR", []calendar.DaySessions{d3, d1, d2})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, [3]int64{10, 0, 20}, [3]int64{rows[0].PreVolume, rows[0].RegVolume, rows[0].PostVolume})
	assert.Equal(t, 2.0, rows[0].PostClose)
	assert.Equal(t, [3]int64{30, 40, 0}, [3]int64{rows[1].PreVolume, rows[1].RegVolume, rows[1].PostVolume})
	assert.Equal(t, int32(0), rows[2].PreBars+rows[2].RegBars+rows[2].PostBars, "a day with no bars")
	assert.True(t, rows[2].Current())
}
